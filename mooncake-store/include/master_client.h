#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "rpc_service.h"
#include "types.h"

#if defined(STORE_USE_CXL_CHANNEL)
#include "cxl_shm/message_queue.h"
#endif

using namespace async_simple;
using namespace coro_rpc;

namespace mooncake {

static const std::string kDefaultMasterAddress = "localhost:50051";

/**
 * @brief Client for interacting with the mooncake master service
 */
class MasterClient {
   public:
    MasterClient();
    ~MasterClient();

    MasterClient(const MasterClient&) = delete;
    MasterClient& operator=(const MasterClient&) = delete;

    /**
     * @brief Connects to the master service
     * @param master_addr Master service address (IP:Port)
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = kDefaultMasterAddress);

        /**
     * @brief Create and bind a CXL Channel RPC client.
     * @param channel_name The channel name to use (from handshake)
     * @param master_server_addr The master server address
     * @return tl::expected<bool, ErrorCode>
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> CreateCxlChannelRpcClient();

    /**
     * @brief Reset (unbind and clear) the CXL Channel RPC client.
     * @return tl::expected<bool, ErrorCode>
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> ResetCxlChannelRpcClient();

    /**
     * @brief Checks if an object exists
     * @param object_key Key to query
     * @return tl::expected<bool, ErrorCode> indicating exist or not
     */
    [[nodiscard]] tl::expected<bool, ErrorCode> ExistKey(
        const std::string& object_key);

    /**
     * @brief CXL channel handshake, get a UUID string
     * @return tl::expected<std::string, ErrorCode> UUID string on success
     */
    [[nodiscard]] tl::expected<std::string, ErrorCode> cxlChannelHandshake();

    /**
     * @brief Checks if multiple objects exist
     * @param object_keys Vector of keys to query
     * @return Vector containing existence status for each key
     */
    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& object_keys);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    GetReplicaList(const std::string& object_key);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]]
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& object_keys);

    /**
     * @brief Starts a put operation
     * @param key Object key
     * @param slice_lengths Vector of slice lengths
     * @param value_length Total value length
     * @param config Replication configuration
     * @return tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
     * indicating success/failure
     */
    [[nodiscard]] tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    PutStart(const std::string& key, const std::vector<size_t>& slice_lengths,
             const ReplicateConfig& config);

    /**
     * @brief Starts a batch of put operations for N objects
     * @param keys Vector of object key
     * @param value_lengths Vector of total value lengths
     * @param slice_lengths Vector of vectors of slice lengths
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config);

    /**
     * @brief Ends a put operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutEnd(const std::string& key);

    /**
     * @brief Ends a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<std::string>& keys);

    /**
     * @brief Revokes a put operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutRevoke(
        const std::string& key);

    /**
     * @brief Revokes a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> Remove(const std::string& key);

    /**
     * @brief Removes all objects and all its replicas
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    [[nodiscard]] tl::expected<long, ErrorCode> RemoveAll();

    /**
     * @brief Registers a segment to master for allocation
     * @param segment Segment to register
     * @param client_id The uuid of the client
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment, const UUID& client_id);

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @param segments Segments to remount
     * @param client_id The uuid of the client
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id);

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_id ID of the segment to unmount
     * @param client_id The uuid of the client
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id, const UUID& client_id);

    /**
     * @brief Gets the cluster ID for the current client to use as subdirectory
     * name
     * @return GetClusterIdResponse containing the cluster ID
     */
    [[nodiscard]] tl::expected<std::string, ErrorCode> GetFsdir();

    /**
     * @brief Pings master to check its availability
     * @param client_id The uuid of the client
     * @return tl::expected<PingResponse, ErrorCode>
     * containing view version and client status
     */
    [[nodiscard]] tl::expected<PingResponse, ErrorCode>
    Ping(const UUID& client_id);

    /**
     * @brief Start a migrate operation for an object, allocate buffers for the
     * slices on the lower level storage
     * @param key Object key
     * @param slice_lengths Vector of slice lengths
     * @param value_length Total value length
     * @param config Replication configuration
     * @return tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
     * indicating success/failure
     */
    [[nodiscard]] tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    MigrateStart(const std::string& key, const std::vector<size_t>& slice_lengths,
             const ReplicateConfig& config);

    /**
     * @brief Revokes a migrate operation
     * @param key Object key
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MigrateRevoke(
        const std::string& key, const ReplicateConfig& config);

    /**
     * @brief Complete a migrate operation
     * @param key Object key
     * @param config Replication configuration
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MigrateEnd(const std::string& key,
                                                       const ReplicateConfig& config);

    /**
     * @brief Peak the master message queue by client id
     * @param client_id The uuid of the client
     * @return tl::expected<DegradeMsg, ErrorCode>
     */
    [[nodiscard]] tl::expected<DegradeMsg, ErrorCode> PopMasterMQ(const UUID& client_id);

    /**
     * @brief [Just For Test] Push the master message queue by client id
     * @param client_id The uuid of the client
     * @param msg The message to push
     * @return tl::expected<void, ErrorCode>
     */
 
   private:
    /**
     * @brief Accessor for the coro_rpc_client. Since coro_rpc_client cannot
     * reconnect to a different address, a new coro_rpc_client is created if
     * the address is different from the current one.
     */
    class RpcClientAccessor {
       public:
        void SetClient(std::shared_ptr<coro_rpc_client> client) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_ = client;
        }

        std::shared_ptr<coro_rpc_client> GetClient() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_rpc_client> client_;
    };
    RpcClientAccessor client_accessor_;

    #if defined(STORE_USE_CXL_CHANNEL)
    /**
     * @brief Send heartbeat message through CXL channel
     * @param channel The CXL channel to send heartbeat
     */
    void sendHeartbeat(std::shared_ptr<cxl_shm::Channel> channel);
    
    /**
     * @brief Accessor for the cxl_shm::Channel. Thread-safe.
     */
    class CxlChannelRpcClientAccessor {
       public:
        void SetChannel(std::shared_ptr<cxl_shm::Channel> channel, const std::string& channel_name) {
            std::lock_guard<std::shared_mutex> lock(channel_mutex_);
            channel_ = channel;
            channel_name_ = channel_name;
        }

        std::shared_ptr<cxl_shm::Channel> GetChannel() {
            std::shared_lock<std::shared_mutex> lock(channel_mutex_);
            return channel_;
        }

        std::string GetChannelName() {
            std::shared_lock<std::shared_mutex> lock(channel_mutex_);
            return channel_name_;
        }

       private:
        mutable std::shared_mutex channel_mutex_;
        std::shared_ptr<cxl_shm::Channel> channel_;
        std::string channel_name_;
    };
    CxlChannelRpcClientAccessor cxl_channel_accessor_;

    // 心跳线程相关
    // heartbeat_mtx_/heartbeat_cv_ 用于让 Reset 时能立即唤醒 sleep 中的心跳线程，
    // 避免使用 std::this_thread::sleep_for 这种不可打断的等待，使 join() 保持 μs 级返回。
    std::thread heartbeat_thread_;
    std::atomic<bool> heartbeat_running_{false};
    std::mutex heartbeat_mtx_;
    std::condition_variable heartbeat_cv_;
    #endif

    // Mutex to insure the Connect function is atomic.
    mutable Mutex connect_mutex_;
    // The address which is passed to the coro_rpc_client
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);
};

}  // namespace mooncake
