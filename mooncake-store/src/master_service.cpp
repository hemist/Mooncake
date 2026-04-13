#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <queue>
#include <shared_mutex>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "segment.h"
#include "types.h"

#if defined(STORE_USE_CXL_CHANNEL)
#include "cxl_shm/message_queue.h"
#include "cxl_rpc_protocol.h"
#endif
#include <iostream>
namespace mooncake {

MasterService::MasterService(bool enable_gc, uint64_t default_kv_lease_ttl,
                             uint64_t default_kv_soft_pin_ttl,
                             bool allow_evict_soft_pinned_objects,
                             double eviction_ratio,
                             double eviction_high_watermark_ratio,
                             ViewVersionId view_version,
                             int64_t client_live_ttl_sec, bool enable_ha,
                             const std::string& cluster_id,
                             BufferAllocatorType memory_allocator,
                             bool enable_cxl)
    : enable_gc_(enable_gc),
      default_kv_lease_ttl_(default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(default_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(allow_evict_soft_pinned_objects),
      eviction_ratio_(eviction_ratio),
      eviction_high_watermark_ratio_(eviction_high_watermark_ratio),
      client_live_ttl_sec_(client_live_ttl_sec),
      enable_ha_(enable_ha),
      cluster_id_(cluster_id),
      segment_manager_(memory_allocator, enable_cxl),
      enable_cxl_(enable_cxl),
      master_mq_service_(std::make_shared<MasterMQService>()) {
    if (eviction_ratio_ < 0.0 || eviction_ratio_ > 1.0) {
        LOG(ERROR) << "Eviction ratio must be between 0.0 and 1.0, "
                   << "current value: " << eviction_ratio_;
        throw std::invalid_argument("Invalid eviction ratio");
    }
    if (eviction_high_watermark_ratio_ < 0.0 ||
        eviction_high_watermark_ratio_ > 1.0) {
        LOG(ERROR)
            << "Eviction high watermark ratio must be between 0.0 and 1.0, "
            << "current value: " << eviction_high_watermark_ratio_;
        throw std::invalid_argument("Invalid eviction high watermark ratio");
    }
    gc_running_ = true;
    gc_thread_ = std::thread(&MasterService::GCThreadFunc, this);
    VLOG(1) << "action=start_gc_thread";

    if (enable_ha) {
        client_monitor_running_ = true;
        client_monitor_thread_ =
            std::thread(&MasterService::ClientMonitorFunc, this);
        VLOG(1) << "action=start_client_monitor_thread";
    }

    if (enable_cxl) {
        allocation_strategy_ = std::make_shared<LevelAllocationStrategy>();
        segment_manager_.initializeCxlAllocator();
        VLOG(1) << "action=start_cxl_global_allocator";
    } else {
        allocation_strategy_ = std::make_shared<RandomAllocationStrategy>();
    }
}

MasterService::~MasterService() {
    // Stop and join the threads
    gc_running_ = false;
    client_monitor_running_ = false;
    if (gc_thread_.joinable()) {
        gc_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }

    // Clean up any remaining GC tasks
    GCTask* task = nullptr;
    while (gc_queue_.pop(task)) {
        if (task) {
            delete task;
        }
    }
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    if (enable_ha_) {
        // Tell the client monitor thread to start timing for this client. To
        // avoid the following undesired situations, this message must be sent
        // after locking the segment mutex and before the mounting operation
        // completes:
        // 1. Sending the message before the lock: the client expires and
        // unmouting invokes before this mounting are completed, which prevents
        // this segment being able to be unmounted forever;
        // 2. Sending the message after mounting the segment: After mounting
        // this segment, when trying to push id to the queue, the queue is
        // already full. However, at this point, the message must be sent,
        // otherwise this client cannot be monitored and expired.
        PodUUID pod_client_id;
        pod_client_id.first = client_id.first;
        pod_client_id.second = client_id.second;
        if (!client_ping_queue_.push(pod_client_id)) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=client_ping_queue_full";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    auto err = segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    // Bind client to master message queue
    master_mq_service_->bind(client_id);
    return {};
}

auto MasterService::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    if (!enable_ha_) {
        LOG(ERROR) << "ReMountSegment is only available in HA mode";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    std::unique_lock<std::shared_mutex> lock(client_mutex_);
    if (ok_client_.contains(client_id)) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=client_already_remounted";
        // Return OK because this is an idempotent operation
        return {};
    }

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Tell the client monitor thread to start timing for this client. To
    // avoid the following undesired situations, this message must be sent
    // after locking the segment mutex or client mutex and before the remounting
    // operation completes:
    // 1. Sending the message before the lock: the client expires and
    // unmouting invokes before this remounting are completed, which prevents
    // this segment being able to be unmounted forever;
    // 2. Sending the message after remounting the segments: After remounting
    // these segments, when trying to push id to the queue, the queue is
    // already full. However, at this point, the message must be sent,
    // otherwise this client cannot be monitored and expired.
    PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    ErrorCode err = segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    master_mq_service_->bind(client_id);
    // Change the client status to OK
    ok_client_.insert(client_id);
    MasterMetricManager::instance().inc_active_clients();

    return {};
}

void MasterService::ClearInvalidHandles() {
    for (auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            // Check if the object has any invalid replicas
            bool has_invalid = false;
            for (auto& replica : it->second.replicas) {
                if (replica.has_invalid_handle()) {
                    has_invalid = true;
                    break;
                }
            }
            // Remove the object if it has no valid replicas
            if (has_invalid || CleanupStaleHandles(it->second)) {
                it = shard.metadata.erase(it);
            } else {
                ++it;
            }
        }
    }
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    ClearInvalidHandles();

    // 3. Commit the unmount operation
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    master_mq_service_->clear(client_id);
    return {};
}

auto MasterService::ExistKey(const std::string& key)
    -> tl::expected<bool, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    auto& metadata = accessor.Get();
    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
        LOG(WARNING) << "key=" << key << ", status=" << *status
                     << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Grant a lease to the object as it may be further used by the client.
    metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);

    return true;
}

#define STORE_USE_CXL_CHANNEL
#ifdef STORE_USE_CXL_CHANNEL
// CXL channel message deal function
bool MasterService::CxlChannelMsgDeal(const CxlChannelRpcRequest& request, CxlChannelRpcResponse& response) {
    try {
        // Deal with the CXL channel message based on the operation type
        switch (request.op) {
            case CxlChannelRpcOp::PutStart: {
                LOG(INFO) << "Processing PutStart request for key: " << request.key;
                // TODO: Implement PutStart logic
                break;
            }
            case CxlChannelRpcOp::PutEnd: {
                LOG(INFO) << "Processing PutEnd request for key: " << request.key;
                // TODO: Implement PutEnd logic
                break;
            }
            case CxlChannelRpcOp::PutRevoke: {
                LOG(INFO) << "Processing PutRevoke request for key: " << request.key;
                // TODO: Implement PutRevoke logic
                break;
            }
            case CxlChannelRpcOp::ExistKey: {
                // LOG(INFO) << "Processing ExistKey request for key: " << request.key;
                auto exist_result = ExistKey(request.key);
                if (exist_result.has_value()) {
                    response.status = CxlChannelRpcStatus::Success;
                    response.exist = exist_result.value();
                } else {
                    response.status = CxlChannelRpcStatus::Fail;
                    response.error_code = static_cast<int>(exist_result.error());
                    response.exist = false;
                }
                break;
            }
            case CxlChannelRpcOp::GetReplicaList: {
                // LOG(INFO) << "Processing GetReplicaList request for key: " << request.key;
                auto replica_list_result = GetReplicaList(request.key);
                if (replica_list_result.has_value()) {
                    response.status = CxlChannelRpcStatus::Success;
                    response.replica_list = replica_list_result.value();
                } else {
                    response.status = CxlChannelRpcStatus::Fail;
                    response.error_code = static_cast<int>(replica_list_result.error());
                }
                break;
            }
            case CxlChannelRpcOp::Heartbeat: {
                // 心跳包处理：只需要返回成功状态
                VLOG(1) << "Processing Heartbeat request";
                response.status = CxlChannelRpcStatus::Success;
                response.error_code = 0;
                break;
            }
            default:
                LOG(WARNING) << "Unknown operation type: " << static_cast<int>(request.op);
                return false;
        }
        return true;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in CxlChannelMsgDeal: " << e.what();
        return false;
    }
}

auto MasterService::cxlChannelHandshake()
    -> tl::expected<std::string, ErrorCode> {

    // Generate cxl_channel_name
    boost::uuids::random_generator gen;
    boost::uuids::uuid uuid = gen();
    std::string cxl_channel_name = boost::uuids::to_string(uuid);

    // Create channel
    std::string cxl_client_name = "master_service_" + cxl_channel_name;
    auto channel_opt = cxl_shm::Channel::Create(
        cxl_channel_name,
        cxl_client_name,  // client_name
        "/dev/dax0.0",     // dev_dax_path
        1024ULL * 1024ULL * 1024ULL, // dev_dax_size
        "127.0.0.1:50052", // master_server_addr ??? wcg 待完善
        cxl_shm::ChannelMode::CXL_SHM_REQ,
        false);
    if (!channel_opt.has_value()) {
        LOG(ERROR) << "Failed to create CXL channel: " << cxl_channel_name;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto channel = channel_opt.value();

    // Register channel
    auto register_result = channel->Register(1UL, STORE_CXL_CHANNEL_MSG_SIZE);
    if (!register_result.has_value()) {
        LOG(ERROR) << "Failed to register CXL channel: " << cxl_channel_name;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Define message structure
    struct Message {
        char data[4096];
    };

    // Create detached thread for message handling
    std::thread message_thread([this, channel]() {
        auto cleanup = [&]() {
            if (channel) {
                auto unregister_result = channel->Unregister();
                if (!unregister_result.has_value()) {
                    LOG(ERROR) << "Failed to unregister CXL channel";
                } else {
                    VLOG(1) << "Successfully unregistered CXL channel";
                }
            }
        };

        try {
            char data[STORE_CXL_CHANNEL_MSG_SIZE];
            #if 0
            while (true) {
                auto recv_result = channel->Recv(data, 128);
                // std::cout << "====recv_result: " << (recv_result.has_value() ? "success" : "failure") << std::endl;
                auto send_result = channel->Send(data, 128);
                // std::cout << "====send_result: " << (send_result.has_value() ? "success" : "failure") << std::endl;

                // CxlChannelRpcResponse response;
                // try {
                //     std::string request_data(data, 45);
                //     CxlChannelRpcRequest request = CxlChannelRpcRequest::deserialize(request_data);
                //     LOG(INFO) << "Deserialized CxlChannelRpcRequest - op: " 
                //               << static_cast<int>(request.op) 
                //               << ", key: " << request.key;
                //     CxlChannelMsgDeal(request, response);
                // } catch (const std::exception& e) {
                //     LOG(WARNING) << "Failed to deserialize CxlChannelRpcRequest: " << e.what();
                // }

                // std::string response_data = response.serialize();
                // // Send back
                // LOG(INFO) << "Sending message2: " << response_data.size() << "[" << response_data << "]";
                // auto send_result = channel->Send(response_data.data(), response_data.size());
                // if (!send_result.has_value()) {
                //     LOG(ERROR) << "Failed to send message";
                //     break;
                // }
            }
            #endif

            while (true) {
                // LOG(INFO) << "Waiting for message...";
                auto recv_result = channel->Recv(data, STORE_CXL_CHANNEL_MSG_SIZE);
                if (!recv_result.has_value()) {
                    LOG(ERROR) << "Failed to receive message";
                    break;
                }
                // LOG(INFO) << "recved message";

                CxlChannelRpcResponse response;
                try {
                    std::string request_data(data, recv_result.value());
                    CxlChannelRpcRequest request = CxlChannelRpcRequest::deserialize(request_data);
                    CxlChannelMsgDeal(request, response);
                } catch (const std::exception& e) {
                    LOG(WARNING) << "Failed to deserialize CxlChannelRpcRequest: " << e.what();
                }

                // Send back
                std::string response_data = response.serialize();
                auto send_result = channel->Send(response_data.data(), response_data.size());
                if (!send_result.has_value()) {
                    LOG(ERROR) << "Failed to send message";
                    break;
                }
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "CXL message thread exception: " << e.what();
        } catch (...) {
            LOG(ERROR) << "CXL message thread unknown exception";
        }

        cleanup();
    });
    message_thread.detach();

    return cxl_channel_name;
}
#else
bool MasterService::CxlChannelMsgDeal(const CxlChannelRpcRequest& request, CxlChannelRpcResponse& response) {
    VLOG(1) << "CxlChannelMsgDeal: cxl_shm channel integration is disabled";
    return false;
}
auto MasterService::cxlChannelHandshake()
    -> tl::expected<std::string, ErrorCode> {
    VLOG(1) << "cxlChannelHandshake: cxl_shm channel integration is disabled";
    return tl::make_unexpected(ErrorCode::UNSUPPORTED);
}
#endif

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<bool, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(ExistKey(key));
    }
    return results;
}

auto MasterService::GetAllKeys()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    for (size_t i = 0; i < kNumShards; i++) {
        MutexLocker lock(&metadata_shards_[i].mutex);
        for (const auto& item : metadata_shards_[i].metadata) {
            all_keys.push_back(item.first);
        }
    }
    return all_keys;
}

auto MasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto MasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    size_t used, capacity;
    auto err = segment_access.QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto MasterService::GetReplicaList(std::string_view key)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    MetadataAccessor accessor(this, std::string(key));
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
        LOG(WARNING) << "key=" << key << ", status=" << *status
                     << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    StorageLevel level = StorageLevel::NUM_STORAGE_LEVELS;
    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        replica_list.emplace_back(replica.get_descriptor());
        if (level == StorageLevel::NUM_STORAGE_LEVELS) {
            level = replica.get_descriptor().get_storage_level();
        }
    }

    // Only mark for GC if enabled
    if (enable_gc_) {
        MarkForGC(std::string(key),
                  1000);  // After 1 second, the object will be removed
    } else {
        // Grant a lease to the object so it will not be removed
        // when the client is reading it.
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
    }

    if (level == StorageLevel::RAM) {
        MasterMetricManager::instance().inc_ram_key_count(1);
    } else if (level == StorageLevel::CXL) {
        MasterMetricManager::instance().inc_cxl_key_count(1);
    }

    return replica_list;
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterService::BatchGetReplicaList(const std::vector<std::string>& keys) {
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(GetReplicaList(key));
    }
    return results;
}

auto MasterService::PutStart(const std::string& key,
                             const std::vector<uint64_t>& slice_lengths,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    if (config.replica_num == 0 || key.empty() || slice_lengths.empty()) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", slice_count=" << slice_lengths.size()
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate slice lengths
    uint64_t total_length = 0;
    for (size_t i = 0; i < slice_lengths.size(); ++i) {
        // if (slice_lengths[i] > kMaxSliceSize) {
        //     LOG(ERROR) << "key=" << key << ", slice_index=" << i
        //                << ", slice_size=" << slice_lengths[i]
        //                << ", max_size=" << kMaxSliceSize
        //                << ", error=invalid_slice_size";
        //     return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        // }
        total_length += slice_lengths[i];
    }

    // LOG(INFO) << "key=" << key << ", value_length=" << total_length
    //         << ", slice_count=" << slice_lengths.size() << ", config=" << config
    //         << ", action=put_start_begin";

    // Lock the shard and check if object already exists
    size_t shard_idx = getShardIndex(key);
    MutexLocker lock(&metadata_shards_[shard_idx].mutex);

    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it != metadata_shards_[shard_idx].metadata.end() &&
        !CleanupStaleHandles(it->second)) {
        LOG(WARNING) << "key=" << key << ", info=object_already_exists";
        // return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);

        // recover the object that is already exists
        std::vector<Replica::Descriptor> replica_list;
        for (auto& replica : it->second.replicas) {
            replica.reset_status();
            replica_list.emplace_back(replica.get_descriptor());
        }
        return replica_list;
    }

    // Update replicate_config
    ReplicateConfig client_config = config;
    ScopedSegmentAllocatorAccess segment_allocator_access = segment_manager_.getAllocatorAccess();
    if (segment_allocator_access.updateReplicateConfigBySegment(client_config) != ErrorCode::OK) {
        LOG(ERROR) << "client_id=" << config.client_id
                   << ", error=UpdateReplicateConfigBySegment failed";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }


    // LOG(INFO) << "key=" << key << ", config=" << config << ", action=allocate_replica_begin";

    // Allocate replicas
    std::vector<Replica> replicas;
    replicas.reserve(client_config.replica_num);
    {
        auto& allocators = segment_allocator_access.getAllocators();
        auto& allocators_by_name = segment_allocator_access.getAllocatorsByName();
        for (size_t i = 0; i < client_config.replica_num; ++i) {
            std::vector<std::unique_ptr<AllocatedBuffer>> handles;
            handles.reserve(slice_lengths.size());
            bool allocated = true;

            // Allocate space for each slice
            for (size_t j = 0; j < slice_lengths.size(); ++j) {
                auto chunk_size = slice_lengths[j];

                // Use the unified allocation strategy with replica config
                auto handle = allocation_strategy_->Allocate(
                    allocators, allocators_by_name, chunk_size, client_config);

                if (!handle) {
                    // If the allocation failed, we need to evict some objects
                    // to free up space for future allocations.
                    allocated = false;
                    if (client_config.preferred_storage_level <= StorageLevel::CXL) {
                        LOG(INFO) << "Allocate failed on current storage level, step down to next storage level";
                        break;
                    }
                    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
                }

                // LOG(INFO) << "key=" << key << ", replica_id=" << i
                //         << ", slice_index=" << j << ", handle=" << *handle
                //         << ", action=slice_allocated";
                handles.emplace_back(std::move(handle));
            }

            if (!allocated) {
                -- i;
                int current_level = static_cast<int>(client_config.preferred_storage_level);
                client_config.preferred_storage_level = static_cast<StorageLevel>(current_level + 1);
                continue;
            }

            replicas.emplace_back(std::move(handles),
                                  client_config.preferred_storage_level,
                                  ReplicaStatus::PROCESSING);
        }
    }

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(replicas.size());
    for (const auto& replica : replicas) {
        replica_list.emplace_back(replica.get_descriptor());
    }

    // No need to set lease here. The object will not be evicted until
    // PutEnd is called.
    metadata_shards_[shard_idx].metadata.emplace(
        std::piecewise_construct, 
        std::forward_as_tuple(key),
        std::forward_as_tuple(total_length, std::move(replicas), client_config.with_soft_pin)
    );
    return replica_list;
}

auto MasterService::PutEnd(const std::string& key)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        replica.mark_complete();
    }
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);
    return {};
}

auto MasterService::PutRevoke(const std::string& key)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::PROCESSING)) {
        LOG(ERROR) << "key=" << key << ", status=" << *status
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    accessor.Erase();
    return {};
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutEnd(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutEnd(key));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutRevoke(key));
    }
    return results;
}

auto MasterService::Remove(const std::string& key)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    // if (!metadata.IsLeaseExpired()) {
    //     VLOG(1) << "key=" << key << ", error=object_has_lease";
    //     return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    // }

    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
        LOG(ERROR) << "key=" << key << ", status=" << *status
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    StorageLevel storage_level = metadata.get_storage_level();
    if (storage_level == StorageLevel::RAM) {
        MasterMetricManager::instance().dec_allocated_dram_size(metadata.size);
    } else if (storage_level == StorageLevel::CXL) {
        MasterMetricManager::instance().dec_allocated_cxl_size(metadata.size);
    }

    // Remove object metadata
    accessor.Erase();
    return {};
}

long MasterService::RemoveAll() {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    auto now = std::chrono::steady_clock::now();

    for (auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        if (shard.metadata.empty()) {
            continue;
        }

        // Only remove objects with expired leases
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (it->second.IsLeaseExpired(now)) {
                total_freed_size +=
                    it->second.size * it->second.replicas.size();
                it = shard.metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

auto MasterService::MarkForGC(const std::string& key, uint64_t delay_ms)
    -> tl::expected<void, ErrorCode> {
    // Create a new GC task and add it to the queue
    GCTask* task = new GCTask(key, std::chrono::milliseconds(delay_ms));
    if (!gc_queue_.push(task)) {
        // Queue is full, delete the task to avoid memory leak
        delete task;
        LOG(ERROR) << "key=" << key << ", error=gc_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

bool MasterService::CleanupStaleHandles(ObjectMetadata& metadata) {
    // Iterate through replicas and remove those with invalid allocators
    auto replica_it = metadata.replicas.begin();
    while (replica_it != metadata.replicas.end()) {
        // Use any_of algorithm to check if any handle has an invalid allocator
        bool has_invalid_handle = replica_it->has_invalid_handle();

        // Remove replicas with invalid handles using erase-remove idiom
        if (has_invalid_handle) {
            replica_it = metadata.replicas.erase(replica_it);
        } else {
            ++replica_it;
        }
    }

    // Return true if no valid replicas remain after cleanup
    return metadata.replicas.empty();
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (const auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        total += shard.metadata.size();
    }
    return total;
}

auto MasterService::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    if (!enable_ha_) {
        LOG(ERROR) << "Ping is only available in HA mode";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    ClientStatus client_status;
    auto it = ok_client_.find(client_id);
    if (it != ok_client_.end()) {
        client_status = ClientStatus::OK;
    } else {
        client_status = ClientStatus::NEED_REMOUNT;
    }
    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return PingResponse(view_version_, client_status);
}

tl::expected<std::string, ErrorCode> MasterService::GetFsdir() const {
    if (cluster_id_.empty()) {
        LOG(ERROR) << "Cluster ID is not initialized";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return cluster_id_;
}

auto MasterService::MigrateStart(const std::string& key,
                                 const std::vector<uint64_t>& slice_lengths,
                                 const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    if (config.replica_num == 0 || key.empty() || slice_lengths.empty()) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", slice_count=" << slice_lengths.size()
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate slice lengths
    uint64_t total_length = 0;
    for (size_t i = 0; i < slice_lengths.size(); ++i) {
        if (slice_lengths[i] > kMaxSliceSize) {
            LOG(ERROR) << "key=" << key << ", slice_index=" << i
                       << ", slice_size=" << slice_lengths[i]
                       << ", max_size=" << kMaxSliceSize
                       << ", error=invalid_slice_size";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_length += slice_lengths[i];
    }

    VLOG(1) << "key=" << key << ", value_length=" << total_length
            << ", slice_count=" << slice_lengths.size() << ", config=" << config
            << ", action=migrate_start_begin";

    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& existing_metadata = accessor.Get();

    ScopedSegmentAllocatorAccess segment_allocator_access = segment_manager_.getAllocatorAccess();
    // Allocate replicas
    std::vector<Replica> new_replicas;
    new_replicas.reserve(config.replica_num);
    {
        auto& allocators = segment_allocator_access.getAllocators();
        auto& allocators_by_name = segment_allocator_access.getAllocatorsByName();
        for (size_t i = 0; i < config.replica_num; ++i) {
            std::vector<std::unique_ptr<AllocatedBuffer>> handles;
            handles.reserve(slice_lengths.size());

            // Allocate space for each slice
            for (size_t j = 0; j < slice_lengths.size(); ++j) {
                auto chunk_size = slice_lengths[j];
                // Use the unified allocation strategy with replica config
                auto handle = allocation_strategy_->Allocate(
                    allocators, allocators_by_name, chunk_size, config);

                if (!handle) {
                    LOG(ERROR)
                        << "key=" << key << ", replica_id=" << i
                        << ", slice_index=" << j << ", error=allocation_failed, level=" << config.preferred_storage_level;
                    // If the allocation failed, we need to evict some objects
                    // to free up space for future allocations.

                    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
                }

                VLOG(1) << "key=" << key << ", replica_id=" << i
                        << ", slice_index=" << j << ", handle=" << *handle
                        << ", action=slice_allocated";
                handles.emplace_back(std::move(handle));
            }

            new_replicas.emplace_back(std::move(handles),
                                  config.preferred_storage_level,
                                  ReplicaStatus::PROCESSING);
        }
    }

    std::vector<Replica::Descriptor> new_replicas_list;
    new_replicas_list.reserve(new_replicas.size());
    for (auto& replica : new_replicas) {
        // Construct new_replicas_list for client transfer.
        new_replicas_list.emplace_back(replica.get_descriptor());
        // Add new replicas to existing metadata
        existing_metadata.replicas.push_back(std::move(replica));
    }
    return new_replicas_list;
}

auto MasterService::MigrateRevoke(const std::string& key, const ReplicateConfig& config)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    metadata.replicas.erase(
        std::remove_if(
            metadata.replicas.begin(), metadata.replicas.end(),
            [&](const Replica& replica) {
                return replica.get_descriptor().storage_level ==
                       config.preferred_storage_level;
            }),
        metadata.replicas.end());

    return {};
}

auto MasterService::MigrateEnd(const std::string& key, 
                               const ReplicateConfig& config)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    bool target_replica_found = false;
    uint64_t total_ram_size = 0;
    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        if (replica.get_descriptor().storage_level == config.preferred_storage_level) {
            replica.mark_complete();
            target_replica_found = true;

            if (replica.get_descriptor().storage_level == StorageLevel::CXL) {
                MasterMetricManager::instance().inc_allocated_cxl_size(replica.get_size());
            }
        } else {
            if (replica.get_descriptor().storage_level == StorageLevel::RAM) {
                total_ram_size += metadata.size * replica.get_size();
                //MasterMetricManager::instance().dec_allocated_dram_size(metadata.size * replica.get_size());
            }
        }
    }

    MasterMetricManager::instance().dec_degraded_queue_size(total_ram_size);

    // remove old replicas
    if (target_replica_found) {
        metadata.replicas.erase(
            std::remove_if(
                metadata.replicas.begin(), metadata.replicas.end(),
                [&](const Replica& replica) {
                    return replica.get_descriptor().storage_level !=
                           config.preferred_storage_level;
                }
            ), metadata.replicas.end()
        );
    }
    
    // for now, just keep it 

    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);
    metadata.ResetEvictState();
    return {};
}

void MasterService::PushMasterMQ(const std::unordered_map<std::string, ObjectMetadata>::iterator& it) {
    // replicas可能包含多副本，选择其中第一个副本，提交给该副本所在的client
    // LOG(WARNING) << "START PUSHING MASTER MQ: " << it->first;
    auto& replica = it->second.replicas[0];
    const auto& replica_descriptor = replica.get_descriptor();
    DegradeMsg msg = {it->first, replica_descriptor};

    // Get client_id from replica and segment descriptor.
    auto segment_access = segment_manager_.getSegmentAccess();
    UUID client_id;
    // 通过遍历client_segments_映射查找包含目标segment的client_id
    const auto& buffer_descriptor = replica_descriptor.get_buffer_descriptors();
    auto& segment_name = buffer_descriptor[0].segment_name_;
    auto err = segment_access.GetClientBySegmentName(segment_name, client_id);
    // LOG(WARNING) << "START PUSHING MASTER MQ, client ID: " << client_id;
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to get client ID by segment name: " << static_cast<int>(err);
    }

    master_mq_service_->push(client_id, msg);
}

void MasterService::GCThreadFunc() {
    VLOG(1) << "action=gc_thread_started";

    std::priority_queue<GCTask*, std::vector<GCTask*>, GCTaskComparator>
        local_pq;

    while (gc_running_) {
        GCTask* task = nullptr;
        while (gc_queue_.pop(task)) {
            if (task) {
                local_pq.push(task);
            }
        }

        while (!local_pq.empty()) {
            task = local_pq.top();
            if (!task->is_ready()) {
                break;
            }

            local_pq.pop();
            VLOG(1) << "key=" << task->key << ", action=gc_removing_key";
            auto result = Remove(task->key);
            if (!result && result.error() != ErrorCode::OBJECT_NOT_FOUND &&
                result.error() != ErrorCode::OBJECT_HAS_LEASE) {
                LOG(WARNING) << "key=" << task->key
                             << ", error=gc_remove_failed, error_code="
                             << result.error();
            }
            delete task;
        }
        std::vector<double> used_ratios =
            MasterMetricManager::instance().get_global_used_ratio();

        // CXL eviction path: 1= cxl ratio, 2= ram remain ratio
        if (enable_cxl_ && used_ratios[1] > eviction_high_watermark_ratio_) {
            double evict_ratio_target = std::max(
                eviction_ratio_,
                used_ratios[1] - eviction_high_watermark_ratio_ + eviction_ratio_);
            double evict_ratio_lowerbound =
                std::max(evict_ratio_target * 0.5,
                         used_ratios[1] - eviction_high_watermark_ratio_);
            LOG(WARNING) << "CXL ratio: " << used_ratios[1]
                         << ", high watermark: " << eviction_high_watermark_ratio_
                         << ", action=start_cxl_eviction";
            BatchEvict(evict_ratio_target, evict_ratio_lowerbound, StorageLevel::CXL);
        }

        if (used_ratios[2] > eviction_high_watermark_ratio_) {
            double evict_ratio_target = std::max(
                eviction_ratio_,
                used_ratios[2] - eviction_high_watermark_ratio_ + eviction_ratio_);
            double evict_ratio_lowerbound =
                std::max(evict_ratio_target * 0.5,
                         used_ratios[2] - eviction_high_watermark_ratio_);
            LOG(WARNING) << "RAM Ratio: " << used_ratios[0] << ", Actual ratio: " << used_ratios[2] << ". Start eviction ";
            BatchEvict(evict_ratio_target, evict_ratio_lowerbound, StorageLevel::RAM);
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kGCThreadSleepMs));
    }

    while (!local_pq.empty()) {
        delete local_pq.top();
        local_pq.pop();
    }

    VLOG(1) << "action=gc_thread_stopped";
}

void MasterService::BatchEvict(double evict_ratio_target,
                               double evict_ratio_lowerbound,
                               StorageLevel target_storage_level) {
    if (target_storage_level != StorageLevel::CXL && target_storage_level != StorageLevel::RAM) {
        LOG(ERROR) << "target_storage_level=" << target_storage_level
                   << ", error=invalid_params";
        return;
    }
    if (evict_ratio_target < evict_ratio_lowerbound) {
        LOG(ERROR) << "evict_ratio_target=" << evict_ratio_target
                   << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound
                   << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::steady_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;

    // Candidates for second pass eviction
    std::vector<std::chrono::steady_clock::time_point> no_pin_objects;
    std::vector<std::chrono::steady_clock::time_point> soft_pin_objects;

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards. No need to use expensive random_device here.
    size_t start_idx = rand() % metadata_shards_.size();

    // First pass: evict objects without soft pin and lease expired
    for (size_t i = 0; i < metadata_shards_.size(); i++) {
        auto& shard = metadata_shards_[(start_idx + i) % metadata_shards_.size()];
        MutexLocker lock(&shard.mutex);

        // object_count must be updated at beginning as it will be used later
        // to compute ideal_evict_num
        //object_count += shard.metadata.size();
        for (auto it = shard.metadata.begin(); it != shard.metadata.end(); it++) {
            if (it->second.get_storage_level() == target_storage_level) {
                object_count += 1;
            }
        }

        // To achieve evicted_count / object_count = evict_ratio_target,
        // ideally how many object should be evicted in this shard
        const long ideal_evict_num = std::ceil(object_count * evict_ratio_target) - evicted_count;

        std::vector<std::chrono::steady_clock::time_point> candidates;  // can be removed
        for (auto it = shard.metadata.begin(); it != shard.metadata.end(); it++) {
            if (it->second.get_storage_level() != target_storage_level) {
                continue;
            }
            if (!it->second.CanBeEvicted()) {
                continue;
            }
            // Skip objects that are not expired or have incomplete replicas
            if (!it->second.IsLeaseExpired(now) ||
                it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
                continue;
            }

            if (!it->second.IsSoftPinned(now)) {
                if (ideal_evict_num > 0) {
                    // first pass candidates
                    candidates.push_back(it->second.lease_timeout);
                } else {
                    // No need to evict any object in this shard, put to
                    // second pass candidates
                    no_pin_objects.push_back(it->second.lease_timeout);
                }
            } else if (allow_evict_soft_pinned_objects_) {
                // second pass candidates, only if
                // allow_evict_soft_pinned_objects_ is true
                soft_pin_objects.push_back(it->second.lease_timeout);
            }
        }

        if (ideal_evict_num > 0 && !candidates.empty()) {
            long evict_num = std::min(ideal_evict_num, (long)candidates.size());
            long shard_evicted_count = 0;  // number of objects evicted from this shard
            std::nth_element(candidates.begin(),
                             candidates.begin() + (evict_num - 1),
                             candidates.end());
            auto target_timeout = candidates[evict_num - 1];

            // Evict objects with lease timeout less than or equal to target.
            auto it = shard.metadata.begin();
            while (it != shard.metadata.end()) {
                // Skip objects that are not allowed to be evicted in the first pass
                if (!it->second.CanBeEvicted() ||
                    it->second.get_storage_level() != target_storage_level ||
                    !it->second.IsLeaseExpired(now) ||
                    it->second.IsSoftPinned(now) ||
                    it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
                    ++it;
                    continue;
                }
                if (it->second.lease_timeout <= target_timeout) {
                    // Evict this object
                    total_freed_size += it->second.size * it->second.replicas.size();
                    if (it->second.get_storage_level() == StorageLevel::CXL) {
                        it = shard.metadata.erase(it);
                    } else if (it->second.get_storage_level() == StorageLevel::RAM) {
                        PushMasterMQ(it);
                        it->second.SetEvicted();
                        ++it;
                    }
                    shard_evicted_count++;
                } else {
                    // second pass candidates
                    no_pin_objects.push_back(it->second.lease_timeout);
                    ++it;
                }
            }
            evicted_count += shard_evicted_count;
        }
    }

    // The ideal number of objects to evict in the second pass
    long target_evict_num =
        std::ceil(object_count * evict_ratio_lowerbound) - evicted_count;
    // The actual number of objects we can evict in the second pass
    target_evict_num =
        std::min(target_evict_num,
                 (long)no_pin_objects.size() + (long)soft_pin_objects.size());

    // Do second pass eviction only if 1). there are candidates that can be
    // evicted AND 2). The evicted number in the first pass is less than
    // evict_ratio_lowerbound.
    if (target_evict_num > 0) {
        // If 1). there are enough candidates without soft pin OR 2). soft pin
        // candidates are empty, then do second pass A. Otherwise, do second
        // pass B. Note that the second condition is ensured implicitly by the
        // calculation of target_evict_num.
        if (target_evict_num <= static_cast<long>(no_pin_objects.size())) {
            // Second pass A: only evict objects without soft pin. The following
            // code is error-prone if target_evict_num > no_pin_objects.size().

            std::nth_element(no_pin_objects.begin(),
                             no_pin_objects.begin() + (target_evict_num - 1),
                             no_pin_objects.end());
            auto target_timeout = no_pin_objects[target_evict_num - 1];

            // Evict objects with lease timeout less than or equal to target.
            // Stop when the target is reached.
            for (size_t i = 0; i < metadata_shards_.size() && target_evict_num > 0; i++) {
                auto& shard = metadata_shards_[(start_idx + i) % metadata_shards_.size()];
                MutexLocker lock(&shard.mutex);

                auto it = shard.metadata.begin();
                while (it != shard.metadata.end() && target_evict_num > 0) {
                    if (!it->second.CanBeEvicted() ||
                        it->second.get_storage_level() != target_storage_level) {
                        ++it;
                        continue;
                    }
                    if (it->second.lease_timeout <= target_timeout &&
                        !it->second.IsSoftPinned(now) &&
                        !it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
                        // Evict this object
                        total_freed_size += it->second.size * it->second.replicas.size();
                        if (it->second.get_storage_level() == StorageLevel::CXL) {
                            LOG(ERROR) << "CXL evict 2:" << it->first;
                            it = shard.metadata.erase(it);
                        } else if (it->second.get_storage_level() == StorageLevel::RAM) {
                            LOG(ERROR) << "PushMasterMQ(it) 2";
                            PushMasterMQ(it);
                            it->second.SetEvicted();
                            ++it;
                        }
                        evicted_count++;
                        target_evict_num--;
                    } else {
                        ++it;
                    }
                }
            }
        } else if (!soft_pin_objects.empty()) {
            // Second pass B: Prioritize evicting objects without soft pin, but
            // also allow to evict soft pinned objects. The following code is
            // error-prone if the soft pin objects are empty.

            const long soft_pin_evict_num =
                target_evict_num - static_cast<long>(no_pin_objects.size());
            // For soft pin objects, prioritize to evict the ones with smaller
            // lease timeout.
            std::nth_element(
                soft_pin_objects.begin(),
                soft_pin_objects.begin() + (soft_pin_evict_num - 1),
                soft_pin_objects.end());
            auto soft_target_timeout = soft_pin_objects[soft_pin_evict_num - 1];

            // Stop when the target is reached.
            for (size_t i = 0;
                 i < metadata_shards_.size() && target_evict_num > 0; i++) {
                auto& shard =
                    metadata_shards_[(start_idx + i) % metadata_shards_.size()];
                MutexLocker lock(&shard.mutex);

                auto it = shard.metadata.begin();
                while (it != shard.metadata.end() && target_evict_num > 0) {
                    // Skip objects that are not expired or have incomplete
                    // replicas
                    if (!it->second.CanBeEvicted() ||
                        it->second.get_storage_level() != target_storage_level ||
                        !it->second.IsLeaseExpired(now) ||
                        it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
                        ++it;
                        continue;
                    }
                    // Evict objects with 1). no soft pin OR 2). with soft pin
                    // and lease timeout less than or equal to target.
                    if (!it->second.IsSoftPinned(now) ||
                        it->second.lease_timeout <= soft_target_timeout) {
                        total_freed_size +=
                            it->second.size * it->second.replicas.size();
                        if (it->second.get_storage_level() == StorageLevel::CXL) {
                            LOG(ERROR) << "CXL evict 3:" << it->first;
                            it = shard.metadata.erase(it);
                        } else if (it->second.get_storage_level() == StorageLevel::RAM) {
                            LOG(ERROR) << "PushMasterMQ(it) 3";
                            PushMasterMQ(it);
                            it->second.SetEvicted();
                            ++it;
                        }
                        evicted_count++;
                        target_evict_num--;
                    } else {
                        ++it;
                    }
                }
            }
        } else {
            // This should not happen.
            LOG(ERROR) << "Error in second pass eviction: target_evict_num="
                       << target_evict_num
                       << ", no_pin_objects.size()=" << no_pin_objects.size()
                       << ", soft_pin_objects.size()="
                       << soft_pin_objects.size()
                       << ", target_storage_level=" << target_storage_level
                       << ", evicted_count=" << evicted_count
                       << ", object_count=" << object_count
                       << ", evict_ratio_target=" << evict_ratio_target
                       << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound;
        }
    }

    if (target_storage_level == StorageLevel::CXL) {
        MasterMetricManager::instance().dec_allocated_cxl_size(total_freed_size);
    } else if (target_storage_level == StorageLevel::RAM) {
        MasterMetricManager::instance().inc_degraded_queue_size(total_freed_size);
    }

    if (evicted_count > 0) {
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
    } else {
        if (object_count == 0) {
            // No objects to evict, no need to check again
        }
        MasterMetricManager::instance().inc_eviction_fail();
    }
    if (evicted_count > 0) {
        LOG(INFO) << "Evicted " << evicted_count << " objects from "
                  << (target_storage_level == StorageLevel::CXL ? "CXL" : "RAM")
                  << " to free up " << total_freed_size << " bytes";
    }
}

void MasterService::ClientMonitorFunc() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point,
                       boost::hash<UUID>>
        client_ttl;
    while (client_monitor_running_) {
        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        PodUUID pod_client_id;
        while (client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            client_ttl[client_id] =
                now + std::chrono::seconds(client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                LOG(INFO) << "client_id=" << it->first
                          << ", action=client_expired";
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        // Update the client status to NEED_REMOUNT
        if (!expired_clients.empty()) {
            // Record which segments are unmounted, will be used in the commit
            // phase.
            std::vector<UUID> unmount_segments;
            std::vector<size_t> dec_capacities;
            std::vector<UUID> client_ids;
            std::vector<std::string> segment_names;
            {
                // Lock client_mutex and segment_mutex
                std::unique_lock<std::shared_mutex> lock(client_mutex_);
                for (auto& client_id : expired_clients) {
                    auto it = ok_client_.find(client_id);
                    if (it != ok_client_.end()) {
                        ok_client_.erase(it);
                        MasterMetricManager::instance().dec_active_clients();
                    }
                }

                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (auto& client_id : expired_clients) {
                    std::vector<Segment> segments;
                    segment_access.GetClientSegments(client_id, segments);
                    for (auto& seg : segments) {
                        size_t metrics_dec_capacity = 0;
                        if (segment_access.PrepareUnmountSegment(
                                seg.id, metrics_dec_capacity) ==
                            ErrorCode::OK) {
                            unmount_segments.push_back(seg.id);
                            dec_capacities.push_back(metrics_dec_capacity);
                            client_ids.push_back(client_id);
                            segment_names.push_back(seg.name);
                        } else {
                            LOG(ERROR) << "client_id=" << client_id
                                       << ", segment_name=" << seg.name
                                       << ", "
                                          "error=prepare_unmount_expired_"
                                          "segment_failed";
                        }
                    }
                }
            }  // Release the mutex before long-running ClearInvalidHandles and
               // avoid deadlocks

            if (!unmount_segments.empty()) {
                ClearInvalidHandles();

                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (size_t i = 0; i < unmount_segments.size(); i++) {
                    segment_access.CommitUnmountSegment(
                        unmount_segments[i], client_ids[i], dec_capacities[i]);
                    LOG(INFO) << "client_id=" << client_ids[i]
                              << ", segment_name=" << segment_names[i]
                              << ", action=unmount_expired_segment";
                }
            }
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }
}

}  // namespace mooncake
