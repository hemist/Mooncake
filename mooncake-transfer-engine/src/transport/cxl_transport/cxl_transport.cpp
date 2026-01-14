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

#include <string_view>

#ifdef USE_CXL_DSA
#include <dml/dml.hpp>
#endif

#ifdef USE_CXL_CUDA
#include <cuda_runtime.h>
#include "transport/cxl_transport/cxl_copy.h"
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
        std::cout << "ERROR: check_cuda_memory: null pointer provided" << std::endl;
        return false;
    }

    // 确保CUDA运行时已经初始化
    static bool cuda_initialized = false;
    if (!cuda_initialized) {
        cudaError_t init_error = cudaFree(0);
        if (init_error != cudaSuccess) {
            std::cout << "WARNING: cudaFree(0) failed: " << cudaGetErrorString(init_error) << std::endl;
        }
        cuda_initialized = true;
    }

    cudaPointerAttributes attributes;
    cudaError_t error = cudaPointerGetAttributes(&attributes, ptr);

    // std::cout << "=== CUDA Memory Check Results ===" << std::endl;
    // std::cout << "Pointer: " << ptr << " (0x" << std::hex << ptr << std::dec << ")" << std::endl;

    // if (error == cudaSuccess) {
    //     switch (attributes.type) {
    //         case cudaMemoryTypeDevice:
    //             std::cout << "Memory type: Device memory" << std::endl;
    //             std::cout << "Device: " << attributes.device << std::endl;
    //             std::cout << "Device pointer: " << attributes.devicePointer << std::endl;
    //             break;
    //         case cudaMemoryTypeHost:
    //             std::cout << "Memory type: Host memory" << std::endl;
    //             std::cout << "Host pointer: " << attributes.hostPointer << std::endl;
    //             break;
    //         case cudaMemoryTypeUnregistered:
    //             std::cout << "Memory type: Unregistered memory (likely unified memory)" << std::endl;
    //             // 检查是否支持统一内存
    //             cudaDeviceProp prop;
    //             cudaGetDeviceProperties(&prop, 0);
    //             if (prop.unifiedAddressing) {
    //                 std::cout << "Unified addressing is supported" << std::endl;
    //             }
    //             break;
    //         case cudaMemoryTypeManaged:
    //             std::cout << "Memory type: Managed memory (UVA)" << std::endl;
    //             break;
    //         default:
    //             std::cout << "Memory type: Unknown" << std::endl;
    //             break;
    //     }
        
    //     std::cout << "Detailed attributes:" << std::endl;
    //     std::cout << "  type: " << attributes.type << std::endl;
    //     std::cout << "  device: " << attributes.device << std::endl;
    //     std::cout << "  devicePointer: " << attributes.devicePointer << std::endl;
    //     std::cout << "  hostPointer: " << attributes.hostPointer << std::endl;
        
    // } else {
    //     std::cout << "ERROR: cudaPointerGetAttributes failed: " << cudaGetErrorString(error) << std::endl;
    //     std::cout << "Error code: " << error << std::endl;
    // }
    // std::cout << "=================================" << std::endl;
    
    // 根据CUDA版本使用适当的判断方法
    #if CUDA_VERSION >= 11000
    // CUDA 11.0及以上版本
    // 检查内存类型是否为设备内存
    LOG(INFO) << "attributes.type = " << attributes.type << " attributes.isManaged = " << attributes.isManaged;
    bool result = (attributes.type == cudaMemoryTypeDevice) && !attributes.isManaged;
    // LOG(ERROR) << "isCudaAddress: Result=" << result 
    //         << " (type == cudaMemoryTypeDevice)=" << (attributes.type == cudaMemoryTypeDevice)
    //         << " (isManaged)=" << attributes.isManaged;
    return result;
    #else
    // 旧版CUDA (10.x及以下)
    // 检查内存类型是否为设备内存
    bool result = (attributes.type == cudaMemoryTypeDevice);
    LOG(ERROR) << "isCudaAddress: Result=" << result 
            << " (type == cudaMemoryTypeDevice)=" << (attributes.type == cudaMemoryTypeDevice);
    return result;
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
    // auto handler = dml::submit<dml::software>(dml::copy_crc,
    //                                             dml::make_view(c_src, size),
    //                                             dml::make_view(c_dst, size),
    //                                             crc_seed);
    // // Wait for the result
    // int i = 0;
    // do {
    //     auto result = handler.get();
    //     if (result.status == dml::status_code::ok)
    //     {
    //         //LOG(ERROR) << "DSA copy ok:"<< ++i << std::endl;
    //         return 0;
    //     } else if (result.status == dml::status_code::partial_completion) {
    //         LOG(ERROR) << "DSA copy partial_completion:"<< ++i << std::endl;
    //         continue;
    //     } else {
    //         LOG(ERROR) << "DSA copy failed：" << static_cast<int>(result.status) << std::endl;
    //         return -1;
    //     }
    // } while(true);
    // return -1;

    auto result = dml::execute<dml::hardware>(dml::copy_crc, dml::make_view(c_src, size),
                    dml::make_view(c_dst, size), crc_seed);

    // Check results
    if (result.status == dml::status_code::ok) {
        //LOG(INFO) << "DSA copy successed. from " << src << " to " << dest << " len:" << size;
        return 0;
    }
    else {
        LOG(ERROR) << "DSA copy failed: " << static_cast<int>(result.status) << std::endl;
        //LOG(INFO) << "DSA copy failed. from " << src << " to " << dest << " len:" << size;
        return -1;
    }
    return 0;
}
#endif

#define CL_SIZE 64
void cache_invalidate_range(const void *addr, size_t len) {
    uintptr_t p = (uintptr_t)addr; 
    uintptr_t end = p + len; 
    if (p % CL_SIZE) {
        p &= ~(uintptr_t)(CL_SIZE - 1);
    }
    for (; p < end; p += CL_SIZE) {
        // __asm__ __volatile__("clflush (%0)" :: "r"((void*)p)); 
        __asm__ __volatile__("clflushopt (%0)" :: "r"(p) : "memory");
    }
}

void do_clflush(const void* addr, size_t len) {
    cache_invalidate_range(addr, len);
    _mm_sfence();
}

int CxlTransport::cxlMemcpy(void *dest, void *src, size_t size, bool is_read) {
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
        LOG(INFO) << "CxlTransport::cxlMemcpy TransferRequest::READ, VRAM->CXL.";
        LOG(INFO) << "CXL:dest: " << dest << " CUDA:src: " << src  << " size: " << size << " CXL:base " << cxl_base_addr;
        CUDA_CHECK(cudaMemcpy(dest, src, size, cudaMemcpyDeviceToHost));
        CUDA_CHECK(cudaDeviceSynchronize());
        return 0;
    } else if (isAddressInCxlRange(src) && isCudaAddress(dest)) {
        // dest is VRAM, src is CXL, Opcode is TransferRequest::WRITE, CXL->VRAM
        // cudaHostRegister is finished in cxlDevInit()
        LOG(INFO) << "CxlTransport::cxlMemcpy TransferRequest::WRITE, CXL->VRAM.";
        LOG(INFO) << "CUDA:dest: " << dest << " CXL:src: " << src  << " size: " << size << " CXL:base " << cxl_base_addr;
        CUDA_CHECK(cudaMemcpy(dest, src, size, cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaDeviceSynchronize());
        return 0;
    } else {
        LOG(INFO) << "CxlTransport::cxlMemcpy, you are using CXL_CUDA, but not using VRAM. src=" << src << ", dest=" << dest << ", size=" << size 
                  << ", isAddressInCxlRange(dest)=" << isAddressInCxlRange(dest) << ", isCudaAddress(src)=" << isCudaAddress(src) 
                  << ", isAddressInCxlRange(src)=" << isAddressInCxlRange(src) << ", isCudaAddress(dest)=" << isCudaAddress(dest);
    }
#endif
    
#ifdef USE_CXL_DSA
    execute_copy_crc(dest, src, size);
#else
    // if (is_read) {
    //     LOG(INFO) << "CxlTransport::cxlMemcpy TransferRequest::READ, CXL->DRAM.";
    //     do_clflush(src, size);
    // }
    // Perform the memory copy
    std::memcpy(dest, src, size);
    if (!is_read) {
        LOG(INFO) << "CxlTransport::cxlMemcpy TransferRequest::WRITE, DRAM->CXL.";
        // Memory barriers and cache operations
        do_clflush(dest, size);
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

int CxlTransport::cxlDevInit()
{
    int fd = open(cxl_dev_path, O_RDWR);
    if (fd == -1) {
        LOG(ERROR) << "CxlTransport: Cannot open cxl device." << strerror(errno);
        return -1;
    }

    void* ptr = mmap(NULL, cxl_dev_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    // LOG(INFO) << "CxlTransport: use normal mmap.";
    LOG(INFO) << "CxlTransport: use mmap, size:" << cxl_dev_size/1024/1024/1024 << "GB";
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
        LOG(INFO) << "[CXL] opcode=" << (slice->opcode == TransferRequest::READ ? "READ" : "WRITE")
          << " cxl_base=0x" << std::hex << cxl_base_addr
          << " offset=0x" << dest_cxl_offset
          << " dest=0x" << slice->cxl.dest_addr
          << " length=" << std::dec << slice->length;
        int err;
        if (slice->opcode == TransferRequest::READ) {
            //READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void *)slice->cxl.dest_addr,
                             slice->length);
            std::ostringstream oss;
            oss << "READ slice source_addr dump (first 256 bytes max):\n";
            const unsigned char *p =
                reinterpret_cast<const unsigned char *>(slice->source_addr);
            size_t dump_len = std::min<size_t>(slice->length, 256);
            for (size_t i = 0; i < dump_len; ++i) {
                if (i % 16 == 0) oss << std::hex << std::setw(4)
                                       << std::setfill('0') << i << ":";
                oss << " " << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<unsigned>(p[i]);
                if ((i + 1) % 16 == 0 || i + 1 == dump_len) oss << "\n";
            }
            LOG(INFO) << oss.str();
        }
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
#ifdef USE_CXL_CUDA
    return submitTransferTaskKernel(task_list);
#else
    return submitTransferTaskNormal(task_list);
#endif
}

#ifdef USE_CXL_CUDA
Status CxlTransport::submitTransferTaskKernel(
    const std::vector<TransferTask *> &task_list) {
    size_t total_slices = 0;
    for (size_t index = 0; index < task_list.size(); ++index) {
        auto &task = *task_list[index];
        total_slices += task.total_bytes;
    }

    LOG(INFO) << "CxlTransport::submitTransferTaskKernel total_slices=" << total_slices << ", task_list.size()=" << task_list.size();
    // auto deleter = [](void *p){ ::operator delete(p); };
    // std::unique_ptr<void* [], decltype(deleter)>
    //     h_src(static_cast<void**>(::operator new(total_slices * sizeof(void*))), deleter);
    // std::unique_ptr<void* [], decltype(deleter)>
    //     h_dst(static_cast<void**>(::operator new(total_slices * sizeof(void*))), deleter);
    // std::unique_ptr<uint64_t[], decltype(deleter)>
    //     h_len(static_cast<uint64_t*>(::operator new(total_slices * sizeof(uint64_t))), deleter);


    size_t idx = 0;
    bool fail = false;
    for (size_t index = 0; index < task_list.size(); ++index) {
        assert(task_list[index]);
        auto &task = *task_list[index];
        assert(task.request);
        auto &request = *task.request;
        uint64_t dest_cxl_offset = request.target_offset;
        task.total_bytes = request.length;
        
        // Slice *slice = getSliceCache().allocate();
        // slice->source_addr = (char *)request.source;
        // slice->cxl.dest_addr = (char *)cxl_base_addr + dest_cxl_offset;
        // slice->length = request.length;
        // slice->opcode = request.opcode;
        // slice->task = &task;
        // slice->target_id = request.target_id;
        // slice->status = Slice::PENDING;
        // task.slice_list.push_back(slice);
        // __sync_fetch_and_add(&task.slice_count, 1);
        int err;
        if (request.opcode == TransferRequest::READ)
            //READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(request.source, cxl_base_addr + dest_cxl_offset,
                             request.length);
        else
            //WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(cxl_base_addr + dest_cxl_offset, request.source,
                             request.length);
        if (err != 0)
            fail = true;
            // __sync_fetch_and_add(task.failed_slice_count, 1);
            // return Status::CxlKernelFail("CXL_KERNEL_FAIL");
        // else
            // __sync_fetch_and_add(task.transferred_bytes, request.length);
            // __sync_fetch_and_add(task.success_slice_count, 1);


        // for (Slice *s : task.slice_list) {
        //     h_src[idx] = s->source_addr;
        //     h_dst[idx] = s->cxl.dest_addr;
        //     h_len[idx] = static_cast<int64_t>(s->length);
        //     ++idx;
        // }
    }

    if (fail)
        return Status::CxlKernelFail("CXL_KERNEL_FAIL");
    else
        return Status::OK();

    // const int n = total_slices;
    // if (n == 0) {
    //     return Status::OK();
    // }

    // void** h_src_ptr = h_src.get();
    // void** h_dst_ptr = h_dst.get();
    // uint64_t* h_len_ptr = h_len.get();

    // launch_batch_memcpy(
    //     reinterpret_cast<const void* const*>(h_src_ptr),
    //     reinterpret_cast<void* const*>(h_dst_ptr),
    //     reinterpret_cast<const int64_t*>(h_len_ptr),
    //     static_cast<int>(idx),
    //     0, 
    //     true
    // );

    // cudaStreamSynchronize(0);
    // for (auto *task_ptr : task_list) {
    //     for (Slice *slice : task_ptr->slice_list) {
    //         slice->status = Slice::SUCCESS;
    //         slice->markSuccess(); 
    //     }
    // }

    // return Status::OK();
}

Status CxlTransport::submitTransferTaskKernelBK(
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

#endif

Status CxlTransport::submitTransferTaskNormal(
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

        LOG(INFO) << "Data on cxl offset:" << dest_cxl_offset << ", length:" << slice->length << "\n";
        int err;
        if (slice->opcode == TransferRequest::READ) {
            //READ: Source is in local memory, Destination is on CXL
            err = cxlMemcpy(slice->source_addr, (void *)slice->cxl.dest_addr,
                             slice->length, true);

            LOG(INFO) << "source_addr dump (" << slice->length << " bytes):";
            fwrite(slice->source_addr, 1, slice->length, stdout);   // 原样打印
            fflush(stdout);        // 确保立即刷到屏幕
        } else {
            //WRITE: Source is in local memory, Destination is on CXL
            err = cxlMemcpy((void *)slice->cxl.dest_addr, slice->source_addr,
                             slice->length, false);
        }
        if (err != 0)
            slice->markFailed();
        else
            slice->markSuccess();
    }
    return Status::OK();
}

}  // namespace mooncake
