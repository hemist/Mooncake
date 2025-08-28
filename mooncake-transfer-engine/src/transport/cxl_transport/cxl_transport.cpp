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
#include <dml/dml.hpp>

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

int CxlTransport::execute_copy_crc(void *dest, void *src, size_t size) {
    auto crc_seed = std::uint32_t(0u);
    char* c_src = static_cast<char*>(src);
    char* c_dst = static_cast<char*>(dest);
    if (size < 1) {
        return -2;
    }
    // Run operation
    auto result = dml::execute<dml::hardware>(dml::copy_crc, dml::make_view(c_src, size),
                    dml::make_view(c_dst, size), crc_seed);

    // Check result
    if (result.status == dml::status_code::ok) {
        //LOG(INFO) << "DSA copy successed. from " << src << " to " << dest << " len:" << size;
        return 0;
    }
    else {
        LOG(ERROR) << "DSA copy failed：" << static_cast<int>(result.status) << std::endl;
        //LOG(INFO) << "DSA copy failed. from " << src << " to " << dest << " len:" << size;
        return -1;
    }

    return 0;
}

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
    
    // Perform the memory copy
    /*
    if (size < 32768) {
        std::memcpy(dest, src, size);
    } else {
        execute_copy_crc(dest, src, size);
    }
        */
    //LOG(INFO) << "DSA copy 1" << std::endl;
    execute_copy_crc(dest, src, size);
    //LOG(INFO) << "DSA copy 2" << std::endl;

    // Memory barriers and cache operations
    if (isAddressInCxlRange(dest) || isAddressInCxlRange(src)) {
        // Ensure memory ordering for CXL operations
        __sync_synchronize();
    }
    
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

#include <numa.h>
#include <numaif.h>
//#include <cstdint>
#include <stdint.h>
#include <errno.h>

#define SIZE1 (16UL * 1024 * 1024 * 1024) // 32GB
#define SIZE2 (16UL * 1024 * 1024 * 1024) // 32GB
#define SHM_NAME1 "/my_numa_shm1"
#define SHM_NAME2 "/my_numa_shm2"
#define ALIGNMENT (2 * 1024 * 1024) // devdax 2MB alignment
void *do_dram_mmap_by_anonymous_fixed() {
    // 1. 创建共享内存对象
    int fd1 = shm_open(SHM_NAME1, O_CREAT | O_RDWR, 0666);
    if (fd1 == -1) {
        perror("shm_open");
        exit(EXIT_FAILURE);
    }
    if (ftruncate(fd1, SIZE1) == -1) {
        perror("ftruncate");
        close(fd1);
        exit(EXIT_FAILURE);
    }
    int fd2 = shm_open(SHM_NAME2, O_CREAT | O_RDWR, 0666);
    if (fd2 == -1) {
        perror("shm_open");
        exit(EXIT_FAILURE);
    }
    if (ftruncate(fd2, SIZE2) == -1) {
        perror("ftruncate");
        close(fd2);
        exit(EXIT_FAILURE);
    }

    // 2. 映射内存
    void *addr1 = mmap(NULL, SIZE1, PROT_READ | PROT_WRITE, MAP_SHARED, fd1, 0);
    if (addr1 == MAP_FAILED) {
        perror("mmap");
        close(fd1);
        exit(EXIT_FAILURE);
    }
    void *addr2 = mmap(NULL, SIZE2, PROT_READ | PROT_WRITE, MAP_SHARED, fd2, 0);
    if (addr2 == MAP_FAILED) {
        perror("mmap");
        close(fd2);
        exit(EXIT_FAILURE);
    }

    // 3. 绑定到指定NUMA节点（例如node0）
    unsigned long nodemask1 = 0x1; // node0的掩码（1 << 0）
    if (mbind(addr1, SIZE1, MPOL_BIND, &nodemask1, sizeof(nodemask1)*8, MPOL_MF_MOVE | MPOL_MF_STRICT) == -1) {
        perror("mbind");
        // 继续执行，但需记录NUMA绑定失败
    } else {
        printf("内存已绑定至node0\n");
    }
    unsigned long nodemask2 = 0x2; // node0的掩码（1 << 0）
    if (mbind(addr2, SIZE2, MPOL_BIND, &nodemask2, sizeof(nodemask2)*8, MPOL_MF_MOVE | MPOL_MF_STRICT) == -1) {
        perror("mbind");
        // 继续执行，但需记录NUMA绑定失败
    } else {
        printf("内存已绑定至node1\n");
    }

    // 4. 使用内存（示例：填充数据）
    memset(addr1, 0xAA, SIZE1); // 实际应用中可分块操作
    memset(addr2, 0xAA, SIZE1); // 实际应用中可分块操作

    /////////////////////////////////////////////////////////////////////////
    void *addr_raw;
    size_t i;
    unsigned long offset, len;
    size_t obj_size = SIZE1 + SIZE2;

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

    size_t ext_num = (obj_size + ALIGNMENT)/(2*ALIGNMENT);
    for (i = 0; i < ext_num; i++) {
        offset = ALIGNMENT*i;
        len = ALIGNMENT;

        void *addr_tmp = mmap(cur_p, len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd1, offset);
        if (addr_tmp == MAP_FAILED) {
            printf("Mmap1 extents[%ld] { offset(%lu), len(%lu) } failed.%s\n", i, offset, len, strerror(errno));
            return NULL;
        }
        cur_p = (void*)((char*)cur_p + len);

        void *addr_tmp2 = mmap(cur_p, len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd2, offset);
        if (addr_tmp2 == MAP_FAILED) {
            printf("Mmap2 extents[%ld] { offset(%lu), len(%lu) } failed.%s\n", i, offset, len, strerror(errno));
            return NULL;
        }
        cur_p = (void*)((char*)cur_p + len);
    }

    printf("Anonymous mmap addr: %p\n", aligned_begin);
    return aligned_begin;
    /////////////////////////////////////////////////////////////////////////
    printf("getchar:\n");
    getchar();

    // 5. 释放资源
    munmap(addr1, SIZE1);
    close(fd1);
    shm_unlink(SHM_NAME1);
    munmap(addr2, SIZE2);
    close(fd2);
    shm_unlink(SHM_NAME2);
    return 0;
}

#define SIZE (32UL * 1024 * 1024 * 1024) // 32GB
#define SHM_NAME "/my_numa_shm11"
void* do_dram_mmap1() 
{
    // 1. 创建共享内存对象
    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd == -1) {
        perror("shm_open");
        exit(EXIT_FAILURE);
    }
    if (ftruncate(fd, SIZE) == -1) {
        perror("ftruncate");
        close(fd);
        exit(EXIT_FAILURE);
    }

    // 2. 映射内存
    void *addr = mmap(NULL, SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        perror("mmap");
        close(fd);
        exit(EXIT_FAILURE);
    }

    // 3. 绑定到NUMA节点1（关键修改）
    unsigned long nodemask = 0x1; // node1的掩码（1 << 1）
    if (mbind(addr, SIZE, MPOL_BIND, &nodemask, sizeof(nodemask)*8, MPOL_MF_MOVE) == -1) {
        perror("mbind");
        // 继续执行，但需记录NUMA绑定失败
    } else {
        printf("内存已成功绑定至node1\n");
    }

    // 4. 使用内存（示例：填充数据）
    memset(addr, 0xAA, SIZE); // 实际应用中可分块操作

    return addr;
}

int CxlTransport::cxlDevInit()
{
    int fd = open(cxl_dev_path, O_RDWR);
    if (fd == -1) {
        LOG(ERROR) << "CxlTransport: Cannot open cxl device." << strerror(errno);
        return -1;
    }

    //void* ptr = mmap(NULL, cxl_dev_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0*128*1073741824LL);
    void* ptr = do_mmap_by_anonymous_fixed(cxl_dev_size, fd, 6, 137438953472);
    //printf("%ld ptr:%p\n", cxl_dev_size, ptr);
    //void* ptr = do_dram_mmap_by_anonymous_fixed();
    //void* ptr = do_dram_mmap1();
    //void* ptr = numa_alloc_onnode(34359738368, 1);
    if (ptr == MAP_FAILED) {
        close(fd);
        return ERR_MEMORY;
    }
    // DSA copy requires that memory has already undergone page faults
    memset((char*)ptr, 0, cxl_dev_size);
    cxl_base_addr = ptr;
    close(fd);
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
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "cxl";
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
    (void)remote_accessible;
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

    cxl_buffer_desc.offset = (uint64_t)addr - (uint64_t)cxl_base_addr;
    cxl_buffer_desc.length = length;
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
