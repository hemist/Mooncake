#include "master_mq_service.h"
#include "types.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>

DEFINE_int64(batch_num, 10, "client batch number");

namespace mooncake::test {
    class MasterMqServiceTest : public ::testing::Test { 
       public:
        std::shared_ptr<MasterMQService> master_mq_service = std::make_shared<MasterMQService>();
        UUID clients[];
       protected:
        void SetUp() override {
            google::InitGoogleLogging("MasterServiceTest");
            FLAGS_logtostderr = true;

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
    for (int i = 0; i < FLAGS_batch_num; i++) {
        threads.push_back(std::thread([this, i, &success_count]() {
            auto client_id = mooncake::generate_uuid(); 
            std::string key = "key-" + std::to_string(i);
            master_mq_service->push(client_id, key);

            std::optional<std::string> key_peak = master_mq_service->peak(client_id);
            EXPECT_EQ(key_peak.value(), key);

            EXPECT_FALSE(master_mq_service->empty(client_id));

            std::string key_pop;
            master_mq_service->pop(client_id, key_pop);
            EXPECT_EQ(key_pop, key);

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
    constexpr int kTotal = 1000;

    int ret = master_mq_service->bind(client_id);
    EXPECT_EQ(ret, 0);

    std::atomic<int> pop_sum{0};
    std::atomic<bool> done{false};

    // Consumer
    std::thread consumer([&]() {
        std::string out;
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
            master_mq_service->push(client_id, "key-" + std::to_string(i));
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