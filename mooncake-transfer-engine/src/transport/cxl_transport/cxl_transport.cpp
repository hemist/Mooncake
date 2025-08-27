// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/cxl_transport/cxl_transport.h"

#include <bits/stdint-uintn.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <memory>

#include "common.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/transport.h"
#include <cstring>
#include <fcntl.h>    // For O_RDWR, O_CREAT, etc.
#include <unistd.h>   // For open(), close(), read(), write()
#include <sys/mman.h> // For mmap, munmap

#ifdef USE_CXL_DSA
#include <dml/dml.hpp>
#endif

#ifdef USE_CXL_CUDA
#include <cuda_runtime.h>
#define CUDA_CHECK(call) \
    do { \
        cudaError_t err = call; \
        if (err != cudaSuccess) { \
            fprintf(stderr, "CUDA error at %s:%d - %s\n", __FILE__, __LINE__, cudaGetErrorString(err)); \
            exit(1); \
        } \
    } while(0)

bool isCudaAddress(void* ptr) {
    if (ptr == nullptr) {
        return false;
    }

    cudaPointerAttributes attributes;
    cudaError_t err = cudaPointerGetAttributes(&attributes, ptr);
    if (err != cudaSuccess) {
        return false;
    }
    
    // 根据CUDA版本使用适当的判断方法
    #if CUDA_VERSION >= 11000
    // CUDA 11.0及以上版本
    // 检查是否为设备指针且不是统一内存(managed memory)
    return (attributes.devicePointer != nullptr) && !attributes.isManaged;
    #else
    // 旧版CUDA (10.x及以下)
    // 检查内存类型是否为设备内存（排除统一内存）
    return (attributes.type == cudaMemoryTypeDevice);
    #endif
}

#endif

namespace mooncake {

CxlTransport::CxlTransport() {
    // cxl_dev_path = "/dev/dax0.0";
    // cxl_dev_size = 1024 * 1024 * 1024;
    // get from env
    const char* env_cxl_dev_path = std::getenv("MC_CXL_DEV_PATH");

    LOG(INFO) << "MC_CXL_DEV_PATH: " << env_cxl_dev_path;

    if (env_cxl_dev_path) {
        cxl_dev_path = (char *) env_cxl_dev_path;
        cxl_dev_size = cxlGetDeviceSize();
    }
}

CxlTransport::~CxlTransport() {
#ifdef USE_CXL_CUDA
    CUDA_CHECK(cudaHostUnregister(cxl_base_addr));
#endif
    munmap(cxl_base_addr, cxl_dev_size);
    metadata_->removeSegmentDesc(local_server_name_);
}

size_t CxlTransport::cxlGetDeviceSize() {
    // for now, get cxl_shm size from env
    const char* env_cxl_dev_size = std::getenv("MC_CXL_DEV_SIZE");

    LOG(INFO) << "MC_CXL_DEV_SIZE: " << env_cxl_dev_size;

    if (env_cxl_dev_size) {
        char* end = nullptr;
        unsigned long long val = strtoull(env_cxl_dev_size, &end, 10);
        if (end != env_cxl_dev_size && *end == '\0')
            return static_cast<size_t>(val);
    }
    return 0;
}

#ifdef USE_CXL_DSA
int CxlTransport::execute_copy_crc(void *dest, void *src, size_t size) {
    auto crc_seed = std::uint32_t(0u);
    char* c_src = static_cast<char*>(src);
    char* c_dst = static_cast<char*>(dest);
    if (size < 1) {
        return -2;
    }
    // Run operation
    auto handler = dml::submit<dml::software>(dml::copy_crc,
                                                dml::make_view(c_src, size),
                                                dml::make_view(c_dst, size),
                                                crc_seed);
    // Wait for the result
    int i = 0;
    do {
        auto result = handler.get();
        if (result.status == dml::status_code::ok)
        {
            //LOG(ERROR) << "DSA copy ok:"<< ++i << std::endl;
            return 0;
        } else if (result.status == dml::status_code::partial_completion) {
            LOG(ERROR) << "DSA copy partial_completion:"<< ++i << std::endl;
            continue;
        } else {
            LOG(ERROR) << "DSA copy failed：" << static_cast<int>(result.status) << std::endl;
            return -1;
        }
    } while(true);
    return -1;
}
#endif

int CxlTransport::cxlMemcpy(void *dest, void *src, size_t size) {
    // Input validation
    if (!src || !dest) {
        LOG(ERROR) << "CxlTransport::cxlMemcpy invalid arguments: null pointer provided.";
        return -1; // null pointer
    }
    
    // Validate memory bounds using the helper function
    if (!validateMemoryBounds(dest, src, size)) {
        return -1; // validation failed
    }

#ifdef USE_CXL_CUDA
    if (isAddressInCxlRange(dest) && isCudaAddress(src)) {
        // dest is CXL, src is VRAM, Opcode is TransferRequest::READ, VRAM->CXL
        // cudaHostRegister is finished in cxlDevInit()
        // LOG(INFO) << "CxlTransport::cxlMemcpy TransferRequest::READ, VRAM->CXL.";
        // LOG(INFO) << "CXL:dest: " << dest << " CUDA:src: " << src  << " size: " << size << " CXL:base " << cxl_base_addr;
        CUDA_CHECK(cudaMemcpy(dest, src, size, cudaMemcpyDeviceToHost));
        CUDA_CHECK(cudaDeviceSynchronize());
        return 0;
    } else if (isAddressInCxlRange(src) && isCudaAddress(dest)) {
        // dest is VRAM, src is CXL, Opcode is TransferRequest::WRITE, CXL->VRAM
        // cudaHostRegister is finished in cxlDevInit()
        // LOG(INFO) << "CxlTransport::cxlMemcpy TransferRequest::WRITE, CXL->VRAM.";
        // LOG(INFO) << "CUDA:dest: " << dest << " CXL:src: " << src  << " size: " << size << " CXL:base " << cxl_base_addr;
        CUDA_CHECK(cudaMemcpy(dest, src, size, cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaDeviceSynchronize());
        return 0;
    } else {
        LOG(INFO) << "CxlTransport::cxlMemcpy, you are using CXL_CUDA, but not using VRAM.";
    }
#endif

    
#ifdef USE_CXL_DSA
    execute_copy_crc(dest, src, size);
#else
    // Perform the memory copy
    std::memcpy(dest, src, size);
    // Memory barriers and cache operations
    if (isAddressInCxlRange(dest) || isAddressInCxlRange(src)) {
        // Ensure memory ordering for CXL operations
        __sync_synchronize();
    }
#endif
    return 0; // success
}

bool CxlTransport::validateMemoryBounds(void *dest, void *src, size_t size) {
    uintptr_t base = reinterpret_cast<uintptr_t>(cxl_base_addr);
    uintptr_t end = base + cxl_dev_size;
    uintptr_t dest_ptr = reinterpret_cast<uintptr_t>(dest);
    uintptr_t src_ptr = reinterpret_cast<uintptr_t>(src);
    
    if (isAddressInCxlRange(dest)) {
        uintptr_t dest_end = dest_ptr + size;
        if (dest_end > end || dest_end < dest_ptr) {
            LOG(ERROR) << "CxlTransport::cxlMemcpy destination out of bounds.";
            return false;
        }
    }
    
    if (isAddressInCxlRange(src)) {
        uintptr_t src_end = src_ptr + size;
        if (src_end > end || src_end < src_ptr) {
            LOG(ERROR) << "CxlTransport::cxlMemcpy source out of bounds.";
            return false;
        }
    }
    
    return true;
}

bool CxlTransport::isAddressInCxlRange(void *addr) {
    if (!addr || !cxl_base_addr) return false;
    
    uintptr_t base = reinterpret_cast<uintptr_t>(cxl_base_addr);
    uintptr_t end = base + cxl_dev_size;
    uintptr_t ptr = reinterpret_cast<uintptr_t>(addr);
    
    return (ptr >= base && ptr < end);
}

#define ALIGNMENT (2 * 1024 * 1024) // devdax 2MB alignment
void *do_mmap_by_anonymous_fixed(size_t obj_size, int dax_fd, size_t num_dimm, size_t len_dimm) {
    void *addr_raw;
    size_t i,j;
    unsigned long offset, len;

    /* check alignment ??? */

    addr_raw = mmap(NULL, obj_size + ALIGNMENT, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0);
    if (addr_raw == MAP_FAILED) {
        printf("Mmap anonymous addr failed.");
        return NULL;
    }
    //memset(addr_raw, 0, obj_size + ALIGNMENT);

    uintptr_t addr_int = (uintptr_t) addr_raw;
    uintptr_t aligned_addr_int = (addr_int + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    void * aligned_begin = (void *) aligned_addr_int;

    void *cur_p = aligned_begin;

    printf("Anonymous mmap addr: %p\n", aligned_begin);

    size_t ext_num = (obj_size + ALIGNMENT)/(num_dimm*ALIGNMENT);
    for (i = 0; i < ext_num; i++) {
        offset = ALIGNMENT*i;
        len = ALIGNMENT;

        for(j = 0; j < num_dimm; j++) {
            void *addr_tmp = mmap(cur_p, len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, dax_fd, len_dimm*j + offset);
            if (addr_tmp == MAP_FAILED) {
                printf("Mmap1 extents[%ld] { offset(%lu), len(%lu) } failed.%s\n", i, offset, len, strerror(errno));
                return NULL;
            }
            cur_p = (void*)((char*)cur_p + len);
        }
    }

    printf("Anonymous mmap addr: %p\n", aligned_begin);
    return aligned_begin;
}

int CxlTransport::cxlDevInit()
{
    int fd = open(cxl_dev_path, O_RDWR);
    if (fd == -1) {
        LOG(ERROR) << "CxlTransport: Cannot open cxl device." << strerror(errno);
        return -1;
    }

    // void* ptr = mmap(NULL, cxl_dev_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    // LOG(INFO) << "CxlTransport: use normal mmap.";
    void* ptr = do_mmap_by_anonymous_fixed(cxl_dev_size, fd, 2, 137438953472);
    LOG(INFO) << "CxlTransport: use fixed mmap.";
    if (ptr == MAP_FAILED) {
        close(fd);
        return ERR_MEMORY;
    }
    // DSA copy requires that memory has already undergone page faults
    memset((char*)ptr, 0, cxl_dev_size);
    // LOG(INFO) << "Memset dsa base addrs";
    cxl_base_addr = ptr;
    close(fd);

#ifdef USE_CXL_CUDA
    cudaError_t reg_err = cudaHostRegister(cxl_base_addr, cxl_dev_size, cudaHostRegisterDefault);
    if (reg_err != cudaSuccess) {
        LOG(ERROR) << "CxlTransport: Cannot register CXL memory to CUDA." << strerror(errno);
        return -1;
    }
#endif

    return 0;
}

int CxlTransport::install(std::string &local_server_name,
                          std::shared_ptr<TransferMetadata> meta,
                          std::shared_ptr<Topology> topo) {
    metadata_ = meta;
    local_server_name_ = local_server_name;

    int ret = cxlDevInit();
    if (ret) {
        LOG(ERROR) << "CxlTransport: Mmap cxl device failed.";
        return -1;
    }

    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "CxlTransport: cannot allocate local segment";
        return -1;
    }

    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "CxlTransport: cannot publish segments, "
                      "check the availability of metadata storage";
        return -1;
    }

    return 0;
}

int CxlTransport::allocateLocalSegmentID() {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
    desc->protocol.push_back("cxl");
    desc->cxl_base_addr = (uint64_t)cxl_base_addr;
    desc->cxl_name = cxl_dev_path;
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int CxlTransport::registerLocalMemory(void *addr, size_t length,
                                      const std::string &location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    // used by store: 
    // for now, skip the local buffer registering case... 
    if (!remote_accessible) {
        return 0;
    }
    
    BufferDesc cxl_buffer_desc;
    cxl_buffer_desc.name = local_server_name_;

    uintptr_t base = reinterpret_cast<uintptr_t>(cxl_base_addr);
    uintptr_t end = base + cxl_dev_size;
    uintptr_t ptr = reinterpret_cast<uintptr_t>(addr);
    uintptr_t ptr_end = ptr + length;
    // check addr legal
    if (ptr < base || ptr >= end) {
        errno = EFAULT;
        return -1;
    }
    // check overflow
    if (ptr_end > end || ptr_end < ptr) {
        errno = EOVERFLOW;
        return -1;
    }

    cxl_buffer_desc.offset = ptr - base;
    cxl_buffer_desc.length = length;
    cxl_buffer_desc.protocol = "cxl";

    return metadata_->addLocalMemoryBuffer(cxl_buffer_desc, update_metadata);
}

int CxlTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int CxlTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list)
        registerLocalMemory(buffer.addr, buffer.length, location, true, false);
    return metadata_->updateLocalSegmentDesc();
}

int CxlTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto &addr : addr_list) unregisterLocalMemory(addr, false);
    return metadata_->updateLocalSegmentDesc();
}

Status CxlTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                       TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) {
        return Status::InvalidArgument(
            "CxlTransport::getTransportStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        if (failed_slice_count) {
            status.s = TransferStatusEnum::FAILED;
        } else {
            status.s = TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status CxlTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "CxlTransport: Exceed the limitation of current batch's "
                      "capacity";
        return Status::InvalidArgument(
            "CxlTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }

    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());

    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        uint64_t dest_cxl_offset = request.target_offset;
        task.total_bytes = request.length;
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->cxl.dest_addr = (char *)cxl_base_addr + dest_cxl_offset;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        __sync_fetch_and_add(&task.slice_count, 1);
        int err;
        if (slice->opcode == TransferRequest::READ)
            //READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void *)slice->cxl.dest_addr,
                             slice->length);
        else
            //WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy((void *)slice->cxl.dest_addr, slice->source_addr,
                             slice->length);
        if (err != 0)
            slice->markFailed();
        else
            slice->markSuccess();
    }

    return Status::OK();
}

Status CxlTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        uint64_t dest_cxl_offset = request.target_offset;
        task.total_bytes = request.length;
        
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = (char *)request.source;
        slice->cxl.dest_addr = (char *)cxl_base_addr + dest_cxl_offset;
        slice->length = request.length;
        slice->opcode = request.opcode;
        slice->task = &task;
        slice->target_id = request.target_id;
        slice->status = Slice::PENDING;
        task.slice_list.push_back(slice);
        __sync_fetch_and_add(&task.slice_count, 1);
        int err;
        if (slice->opcode == TransferRequest::READ)
            //READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void *)slice->cxl.dest_addr,
                             slice->length);
        else
            //WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy((void *)slice->cxl.dest_addr, slice->source_addr,
                             slice->length);
        if (err != 0)
            slice->markFailed();
        else
            slice->markSuccess();
    }
    return Status::OK();
}

}  // namespace mooncake
