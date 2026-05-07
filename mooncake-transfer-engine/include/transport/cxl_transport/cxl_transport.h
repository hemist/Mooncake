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

#ifndef CXL_TRANSPORT_H_
#define CXL_TRANSPORT_H_


#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
class TransferMetadata;

class CxlTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;

   public:
    CxlTransport();

    ~CxlTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status submitTransferTaskNormal(
        const std::vector<TransferTask *> &task_list);
    
    Status submitTransferTaskKernel(
        const std::vector<TransferTask *> &task_list);

    // `stream` is a cudaStream_t cast to uintptr_t (0 == default stream).
    // The kernel launch and the trailing sync both run on this stream so the
    // call composes with caller-side CUDA work.
    Status submitTransferDirectKernel(const std::vector<TransferRequest> &entries,
                                      uintptr_t stream = 0);

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    void cudaDefaultStreamSynchronize();

    // Synchronize a specific CUDA stream. `stream` is a cudaStream_t cast to
    // uintptr_t (so this header stays free of <cuda_runtime.h>); 0 == default
    // stream, equivalent to cudaDefaultStreamSynchronize().
    void cudaStreamSynchronize(uintptr_t stream);

    void* getCxlBaseAddr() { return cxl_base_addr; }

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo);

    int allocateLocalSegmentID();

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata);

    int unregisterLocalMemory(void *addr, bool update_metadata = false);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location);

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    const char *getName() const override { return "cxl"; }

    int cxlDevInit();

    size_t cxlGetDeviceSize();
#ifdef USE_CXL_DSA
    int execute_copy_crc(void *dest, void *src, size_t size);
    int execute_copy_crc_slice(void *dest, void *src, size_t size);
#endif
    // `stream` is a cudaStream_t cast to uintptr_t (0 == default stream); only
    // the USE_CXL_CUDA branch (cudaMemcpyAsync) consults it. Other branches
    // ignore it. `is_read` controls whether the plain-memcpy fallback issues a
    // post-write clflush (write side only).
    int cxlMemcpy(void *dest_addr, void *source_addr, size_t size,
                  bool is_read = false, uintptr_t stream = 0);

    bool isAddressInCxlRange(void *addr);

    bool validateMemoryBounds(void *dest, void *src, size_t size);

   private:
    void* cxl_base_addr;
    size_t cxl_dev_size;
    char* cxl_dev_path;
};
}  // namespace mooncake

#endif