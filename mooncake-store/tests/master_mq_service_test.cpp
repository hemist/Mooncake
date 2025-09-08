#include "master_mq_service.h"
#include "master_service.h"
#include "types.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>

DEFINE_int64(batch_num, 10, "client batch number");

namespace mooncake::test {
    class MasterMqServiceTest : public ::testing::Test { 
       public:
        std::shared_ptr<MasterMQService> master_mq_service = std::make_shared<MasterMQService>();
        std::vector<UUID> clients;
       protected:
        void SetUp() override {
            google::InitGoogleLogging("MasterServiceTest");
            FLAGS_logtostderr = true;

            clients.resize(FLAGS_batch_num);
            for (int i = 0; i < FLAGS_batch_num; i++) {
                clients[i] = generate_uuid();
            }
        }
        void TearDown() override { google::ShutdownGoogleLogging(); }
    };



TEST_F(MasterMqServiceTest, Bind) {
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < FLAGS_batch_num; i++) {
        threads.push_back(std::thread(
            [this, client_id = clients[i], &success_count]() {
            auto ret = master_mq_service->bind(client_id);
            EXPECT_EQ(ret, 0);
            success_count.fetch_add(1, std::memory_order_relaxed);
        }));
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count, FLAGS_batch_num);
}

TEST_F(MasterMqServiceTest, Base) {
    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;
    std::unique_ptr<MasterService> service_(new MasterService());
    for (int i = 0; i < FLAGS_batch_num; i++) {
        threads.push_back(std::thread([this, i, &success_count, &service_]() {

            std::vector<Replica::Descriptor> replica_list;

            std::unique_ptr<MasterService> service_(new MasterService());
            constexpr size_t buffer = 0x300000000;
            constexpr size_t size = 1024 * 1024 * 16;
            std::string segment_name = "test_segment_" + std::to_string(i);

            UUID client_id = clients[i];
            Segment segment(client_id, segment_name, buffer, size);

            auto mount_result = service_->MountSegment(segment, client_id);
            ASSERT_TRUE(mount_result.has_value());

            std::string key = "key-" + std::to_string(i);
            uint64_t value_length = 1024;
            std::vector<uint64_t> slice_lengths = {value_length};
            ReplicateConfig config;
            config.client_id = client_id;
            config.replica_num = 1;

            auto put_start_result = service_->PutStart(key, slice_lengths, config);
            EXPECT_TRUE(put_start_result.has_value());
            replica_list = put_start_result.value();
            EXPECT_FALSE(replica_list.empty());
            EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);

            DegradeMsg msg = {key, replica_list[0]};

            master_mq_service->push(client_id, msg);

            std::optional<DegradeMsg> msg_peak = master_mq_service->peak(client_id);
            EXPECT_EQ(msg_peak.value().key_, key);

            EXPECT_FALSE(master_mq_service->empty(client_id));

            DegradeMsg msg_pop;
            master_mq_service->pop(client_id, msg_pop);
            EXPECT_EQ(msg_pop.key_, key);

            if (!master_mq_service->empty(client_id)) {
                auto res = master_mq_service->clear(client_id);
                EXPECT_EQ(res, 0);
            }

            success_count.fetch_add(1, std::memory_order_relaxed);
        }));
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(success_count, FLAGS_batch_num);
}

TEST_F(MasterMqServiceTest, PushPop) { 
    const UUID client_id = generate_uuid();
    constexpr int kTotal = 10;

    int ret = master_mq_service->bind(client_id);
    EXPECT_EQ(ret, 0);

    std::atomic<int> pop_sum{0};
    std::atomic<bool> done{false};

    // Consumer
    std::thread consumer([&]() {
        DegradeMsg out;
        while (!done || !master_mq_service->empty(client_id)) {
            if (master_mq_service->pop(client_id, out) == 0) {
                pop_sum.fetch_add(1, std::memory_order_relaxed);
            } else {
                std::this_thread::yield();
            }
        }
    });

    // Producer
    std::thread producer([&]() { 
        for (int i = 0; i < kTotal; ++i) {
            std::vector<Replica::Descriptor> replica_list;

            std::unique_ptr<MasterService> service_(new MasterService());
            constexpr size_t buffer = 0x300000000;
            constexpr size_t size = 1024 * 1024 * 16;
            std::string segment_name = "test_segment_" + std::to_string(i);

            Segment segment(client_id, segment_name, buffer, size);

            auto mount_result = service_->MountSegment(segment, client_id);
            ASSERT_TRUE(mount_result.has_value());

            std::string key = "key-" + std::to_string(i);
            uint64_t value_length = 1024;
            std::vector<uint64_t> slice_lengths = {value_length};
            ReplicateConfig config;
            config.client_id = client_id;
            config.replica_num = 1;

            auto put_start_result = service_->PutStart(key, slice_lengths, config);
            EXPECT_TRUE(put_start_result.has_value());
            replica_list = put_start_result.value();
            EXPECT_FALSE(replica_list.empty());
            EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);

            DegradeMsg msg = {key, replica_list[0]};
            
            master_mq_service->push(client_id, msg);
            service_->UnmountSegment(segment.id, client_id);
        }
        done.store(true, std::memory_order_release);
    });

    producer.join();
    consumer.join();

    EXPECT_EQ(pop_sum.load(), kTotal);
    EXPECT_TRUE(master_mq_service->empty(client_id));
}


} // namespace mooncake

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}