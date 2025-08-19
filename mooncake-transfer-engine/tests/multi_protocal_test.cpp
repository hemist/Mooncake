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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "common.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(local_server_name, getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "10.129.131.15:2379", "etcd server host address");
DEFINE_string(protocols, "tcp,cxl", "Transfer protocols: rdma|tcp|cxl");
DEFINE_string(cxl_device_name, "/dev/dax0.0", "Device name for cxl");
DEFINE_string(tcp_device_name, "enp1s0", "Device name for tcp");
DEFINE_int64(cxl_device_size, 1073741824, "Device Size for cxl");
DEFINE_int64(dram_size, 1073741824, "Dram Size for tcp/rdma");

static void *allocateMemoryPool(size_t size, int socket_id, bool from_vram = false) {
    return numa_alloc_onnode(size, socket_id);
}

static void freeMemoryPool(void *addr, size_t size) { 
    numa_free(addr, size); 
}

class MultiProtocalTest : public ::testing::Test {
   public:
    const size_t offset_1 = 2 * 1024 * 1024;
    const size_t offset_2 = 6 * 1024 * 1024;
    const size_t len = 2 * 1024 * 1024;
    const size_t kDataLength = 4 * 1024;
    int tmp_fd = -1;
    std::vector<std::string> protocals;
    uint8_t *addr = nullptr;
    uint8_t *base_addr;
    uint8_t *buffer_addr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    mooncake::Transport::SegmentID segment_id;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    std::unordered_map<std::string, std::vector<mooncake::TransferEngine::RegisteredBuffer>> buffer_map;
    

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("MultiProtocalTransportTest");
        FLAGS_logtostderr = 1;

        // Set device name from gflags parameter
        setenv("MC_CXL_DEV_PATH", FLAGS_cxl_device_name.c_str(), 1);
        setenv("MC_CXL_DEV_SIZE", std::to_string(FLAGS_cxl_device_size).c_str(), 1);

        // Parse protocol
        std::stringstream ss(FLAGS_protocols);
        std::string item;
        while (std::getline(ss, item, ',')) {
            if (!item.empty()) {
                protocals.push_back(item);
            }
        }

        // transfer engine setup
        std::vector<std::string> devices = {FLAGS_tcp_device_name};
        engine = std::make_unique<TransferEngine>(true, devices);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(),
                     hostname_port.second + offset++);

        // register local memory for source data
        addr = (uint8_t*) allocateMemoryPool(kDataLength, 0, false);

        // DSA copy requires that memory has already undergone page faults
        memset(addr, 0, kDataLength);

        // register local memory for target data
        for (const auto &protocol : protocals) {
            if (protocol == "cxl") {
                base_addr = (uint8_t*)engine->getBaseAddr();
                buffer_map[protocol].emplace_back(base_addr + offset_1, len);
            } else if (protocol == "tcp") {
                buffer_addr = (uint8_t*)allocateMemoryPool(FLAGS_dram_size, 0);
                buffer_map[protocol].emplace_back(buffer_addr, FLAGS_dram_size);
            }
        }
        int rc = engine->registerLocalMemory(buffer_map);
        ASSERT_EQ(rc, 0);

        segment_id = engine->openSegment(FLAGS_local_server_name.c_str());
        // bindToSocket(0);
        segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    }

    void TearDown() override {
        if (tmp_fd >= 0) { 
            close(tmp_fd); 
            unlink(FLAGS_cxl_device_name.c_str()); 
        }
        google::ShutdownGoogleLogging();
        freeMemoryPool(addr, kDataLength);
        engine->unregisterLocalMemory(buffer_map);
    }
};

TEST_F(MultiProtocalTest, MultiWrite) {
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char *)(addr) + offset) = 'a' + lrand48() % 26;
        
        auto batch_id = engine->allocateBatchID(1);
        auto proto = protocals[times % protocals.size()];

        Status s;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(addr);
        entry.target_id = segment_id;
        entry.target_offset = proto == "cxl" ? offset_1 : (uint64_t) buffer_addr + offset_1;

        s = engine->submitTransfer(batch_id, {entry}, proto);
        LOG_ASSERT(s.ok());
        // LOG(INFO) << "Transfer " << 10 - times << ", " << proto << ", completed";
        
        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = engine->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "FAILED";
                completed = true;
            }
        }

        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
    }
}

TEST_F(MultiProtocalTest, MultipleRead) {
    int times = 10;

    while (times--) {
        auto batch_id = engine->allocateBatchID(1);
        char *data = (char *) allocateMemoryPool(kDataLength, 0, false);
        memset(data, 0, kDataLength);

        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((data) + offset) = 'a' + lrand48() % 26;
        
        auto proto = protocals[times % protocals.size()];

        Status s;
        TransferStatus status;
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(data);
        entry.target_id = segment_id;
        entry.target_offset = proto == "cxl" ? offset_2 : (uint64_t) buffer_addr + offset_2;
        
        s = engine->submitTransfer(batch_id, {entry}, proto);
        LOG_ASSERT(s.ok());
        
        bool completed = false;
        while (!completed) {
            Status s = engine->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "FAILED";
                completed = true;
            }
        }
        
        ASSERT_EQ(completed, true);
        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());

        // sleep(3);

        batch_id = engine->allocateBatchID(1);
        void *src = allocateMemoryPool(kDataLength, 0, false);
        memset(src, 0, kDataLength);

        entry.opcode = TransferRequest::READ;
        entry.length = kDataLength;
        entry.source = (uint8_t *)(src);
        entry.target_id = segment_id;
        entry.target_offset = proto == "cxl" ? offset_2 : (uint64_t) buffer_addr + offset_2;
       
        s = engine->submitTransfer(batch_id, {entry}, proto);
        ASSERT_EQ(s, Status::OK());
        LOG(INFO) << "Transfer write-read " << 10 - times << ", " << proto << ", completed";
        

        completed = false;
        while (!completed) {
            Status s = engine->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED)
                completed = true;
            else if (status.s == TransferStatusEnum::FAILED) {
                completed = true;
            }
        }
        s = engine->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());

        int ret = memcmp((uint8_t *)(src), (uint8_t *)(data), kDataLength);
        ASSERT_EQ(ret, 0);

        freeMemoryPool(src, kDataLength);
        freeMemoryPool(data, kDataLength);
    }

}

}  // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
