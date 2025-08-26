#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "allocator.h"
#include "client.h"
#include "types.h"
#include "utils.h"

DEFINE_string(protocol, "cxl,tcp", "Transfer protocol: rdma|tcp|cxl");
DEFINE_string(device_name, "enp1s0", "Device name to use, valid if protocol=rdma|tcp");
DEFINE_string(transfer_engine_metadata_url, "etcd://10.129.131.15:2379", "Metadata connection string for transfer engine");
DEFINE_uint64(default_kv_lease_ttl, mooncake::DEFAULT_DEFAULT_KV_LEASE_TTL,
              "Default lease time for kv objects, must be set to the same as the master's default_kv_lease_ttl");
DEFINE_string(cxl_device_name, "/dev/dax0.0", "Device name for cxl");
DEFINE_uint64(cxl_device_size, 4294967296, "Device Size for cxl");
DEFINE_bool(auto_disc, true, "Auto discover tcp devices");
DEFINE_uint64(segment_size, 512UL * 1024 * 1024, "Segment size");

namespace mooncake { 
namespace testing {

class LevelStorageTest : public ::testing::Test {
   public:
    static std::shared_ptr<Client> CreateClient(const std::string& host_name) {
        bool rdma_enabled = false;
        std::stringstream ss(FLAGS_protocol);
        std::string item;
        while (std::getline(ss, item, ',')) {
            if (!item.empty()) {
                if (item == "rdma") rdma_enabled = true;
                protocols.push_back(item);
            }
        }

        void** args = rdma_enabled ? rdma_args(FLAGS_device_name) : nullptr;
        auto client_opt = Client::Create(
            host_name,                           // Local hostname
            FLAGS_transfer_engine_metadata_url,  // Metadata connection string
            protocols, args,
            "localhost:50051"  // Master server address
        );

        EXPECT_TRUE(client_opt.has_value())
            << "Failed to create client with host_name: " << host_name;
        if (!client_opt.has_value()) {
            return nullptr;
        }
        return client_opt.value();
    }

    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");

        // Override flags from environment variables if present
        setenv("MC_CXL_DEV_PATH", FLAGS_cxl_device_name.c_str(), 1);
        setenv("MC_CXL_DEV_SIZE", std::to_string(FLAGS_cxl_device_size).c_str(), 1);
        setenv("MC_MS_AUTO_DISC", std::to_string(FLAGS_auto_disc).c_str(), 1);

        InitializeClients();
        InitializeSegment();
    }

    static void TearDownTestSuite() {
        CleanupSegment();
        CleanupClients();
        google::ShutdownGoogleLogging();
    }

    static void ClientMountSegment(Client* client) {
        for (auto &protocol : client->GetLevelProtocols()) {
            if (protocol.first == StorageLevel::RAM) {
                void *ptr = allocate_buffer_allocator_memory(FLAGS_segment_size);
                LOG_ASSERT(ptr);
                auto mount_result = client->MountSegment(ptr, FLAGS_segment_size, protocol.first);
                if (!mount_result.has_value()) {
                    LOG(ERROR) << "Failed to mount segment: " << toString(mount_result.error());
                }
                client_segment_ptrs_[client]
                    .emplace_back(reinterpret_cast<uintptr_t>(ptr), reinterpret_cast<size_t>(FLAGS_segment_size));
            } else if (protocol.first == StorageLevel::CXL) { 
                void *ptr = client->GetBaseAddr();
                LOG_ASSERT(ptr);
                LOG(INFO) << "Mounting CXL segment: " << FLAGS_cxl_device_size << " bytes, " << ptr;
                auto mount_result = client->MountSegment(ptr, FLAGS_cxl_device_size, protocol.first);
                if (!mount_result.has_value()) {
                    LOG(ERROR) << "Failed to mount segment: " << toString(mount_result.error());
                }
                client_segment_ptrs_[client]
                    .emplace_back(reinterpret_cast<uintptr_t>(ptr), reinterpret_cast<size_t>(FLAGS_cxl_device_size));
            }
        }
    }

    static void InitializeSegment() {
        // init local buffer allocator
        client_buffer_allocator_ = std::make_unique<SimpleAllocator>(128 * 1024 * 1024);
        auto register_result = test_client_->RegisterLocalMemory(
            client_buffer_allocator_->getBase(), 128 * 1024 * 1024, "cpu:0", false, false);
        if (!register_result.has_value()) {
            LOG(ERROR) << "Failed to register local memory: " << toString(register_result.error());
        }

        // Mount segments
        ClientMountSegment(test_client_.get());
        LOG(INFO) << "Test client segment mounted successfully";
        ClientMountSegment(segment_provider_client_.get());
        LOG(INFO) << "Segment mounted successfully";
    }

    static void InitializeClients() {
        // This client is used for testing purposes.
        test_client_ = CreateClient("localhost:17183");
        ASSERT_TRUE(test_client_ != nullptr);

        // This client is used to provide segments.
        segment_provider_client_ = CreateClient("localhost:17182");
        ASSERT_TRUE(segment_provider_client_ != nullptr);
    }

    static void CleanupClients() {
        if (test_client_) {
            test_client_.reset();
        }
        if (segment_provider_client_) {
            segment_provider_client_.reset();
        }
    }

    static void CleanupSegment() {
        if (!client_segment_ptrs_.empty()) {
            for (auto& kv : client_segment_ptrs_) {
                Client* client = kv.first;
                auto& segment_ptrs = kv.second;
                for (auto [ptr, size] : segment_ptrs) {
                    if (!client->UnmountSegment(reinterpret_cast<void*>(ptr), size).has_value()) {
                        LOG(ERROR) << "Failed to unmount segment";
                    }
                }
            }
        }
    }

    static std::shared_ptr<Client> test_client_;
    static std::shared_ptr<Client> segment_provider_client_;
    // Here we use a simple allocator for the client buffer. In a real
    // application, user should manage the memory allocation and deallocation
    // themselves.
    static std::unique_ptr<SimpleAllocator> client_buffer_allocator_;
    static std::vector<std::string> protocols;
    static std::unordered_map<Client*, std::vector<std::pair<uintptr_t, size_t>>> client_segment_ptrs_;
    static size_t ram_buffer_size_;
    static size_t test_client_ram_buffer_size_;
    static uint64_t default_kv_lease_ttl_;
};

// Static members initialization
std::shared_ptr<Client> LevelStorageTest::test_client_ = nullptr;
std::shared_ptr<Client> LevelStorageTest::segment_provider_client_ = nullptr;
std::unique_ptr<SimpleAllocator> LevelStorageTest::client_buffer_allocator_ = nullptr;
size_t LevelStorageTest::ram_buffer_size_ = 0;
size_t LevelStorageTest::test_client_ram_buffer_size_ = 0;
uint64_t LevelStorageTest::default_kv_lease_ttl_ = FLAGS_default_kv_lease_ttl;
std::vector<std::string> LevelStorageTest::protocols = {};
std::unordered_map<Client*, std::vector<std::pair<uintptr_t, size_t>>>
    LevelStorageTest::client_segment_ptrs_ = {};

// Test basic Put/Get operations through the client
TEST_F(LevelStorageTest, BasicPutGetOperations) {
    const std::string test_data = "Hello, World!";
    const std::string key = "test_key";
    void* buffer = client_buffer_allocator_->allocate(test_data.size());

    // write
    memcpy(buffer, test_data.data(), test_data.size());
    std::vector<Slice> slices;
    slices.emplace_back(Slice{buffer, test_data.size()});

    // Test Put operation
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_result = test_client_->Put(key, slices, config);
    ASSERT_TRUE(put_result.has_value())
        << "Put operation failed: " << toString(put_result.error());
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    buffer = client_buffer_allocator_->allocate(1 * 1024 * 1024);
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});

    // Verify data through Get operation
    auto get_result = test_client_->Get(key, slices);
    ASSERT_TRUE(get_result.has_value())
        << "Get operation failed: " << toString(get_result.error());
    ASSERT_EQ(slices.size(), 1);
    ASSERT_EQ(slices[0].size, test_data.size());
    ASSERT_EQ(slices[0].ptr, buffer);
    ASSERT_EQ(memcmp(slices[0].ptr, test_data.data(), test_data.size()), 0);
    client_buffer_allocator_->deallocate(buffer, test_data.size());

    // Put again with the same key, should succeed
    buffer = client_buffer_allocator_->allocate(test_data.size());
    memcpy(buffer, test_data.data(), test_data.size());
    slices.clear();
    slices.emplace_back(Slice{buffer, test_data.size()});
    auto put_result2 = test_client_->Put(key, slices, config);
    ASSERT_TRUE(put_result2.has_value())
        << "Second Put operation failed: " << toString(put_result2.error());
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl_));
    auto remove_result = test_client_->Remove(key);
    ASSERT_TRUE(remove_result.has_value())
        << "Remove operation failed: " << toString(remove_result.error());
    client_buffer_allocator_->deallocate(buffer, test_data.size());
}


} // namespace testing
} // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}