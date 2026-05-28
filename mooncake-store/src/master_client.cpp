#include "master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <string>
#include <vector>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "cxl_rpc_protocol.h"

namespace mooncake {

// 防止编译器优化的辅助函数
// 使用 asm volatile 阻止编译器消除看似无用的代码
template<typename T>
inline void do_not_optimize(const T& value) {
    asm volatile("" : : "g"(value) : "memory");
}

template<typename T>
inline void force_read(T& value) {
    asm volatile("" : "+r"(value) : : "memory");
}

using namespace coro_rpc;
using namespace async_simple::coro;

MasterClient::MasterClient() = default;

MasterClient::~MasterClient() {
#if defined(STORE_USE_CXL_CHANNEL)
    // 兜底：若上层未调用 ResetCxlChannelRpcClient 就直接销毁 MasterClient，
    // 在这里确保心跳线程被停下来并 join，避免 std::thread 在 joinable
    // 状态下被析构导致 std::terminate()。
    {
        std::lock_guard<std::mutex> lk(heartbeat_mtx_);
        heartbeat_running_.store(false);
    }
    heartbeat_cv_.notify_all();
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
#endif
}

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "MasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    if (client_addr_param_ == master_addr) {
        auto client = client_accessor_.GetClient();
        auto result = coro::syncAwait(client->connect(master_addr));
        if (result.val() != 0) {
            LOG(ERROR) << "Failed to connect to master: " << result.message();
            timer.LogResponse("error_code=", ErrorCode::RPC_FAIL);
            return ErrorCode::RPC_FAIL;
        }
        timer.LogResponse("error_code=", ErrorCode::OK);
        return ErrorCode::OK;
    } else {
        // Once connected to address A, the coro_rpc_client does not support
        // connect to a new address B. So we need to create a new
        // coro_rpc_client if the address is different from the current one.
        auto client = std::make_shared<coro_rpc_client>();
        auto result = coro::syncAwait(client->connect(master_addr));
        if (result.val() != 0) {
            LOG(ERROR) << "Failed to connect to master: " << result.message();
            timer.LogResponse("error_code=", ErrorCode::RPC_FAIL);
            return ErrorCode::RPC_FAIL;
        }
        // Set the client to the accessor and update the address parameter
        client_accessor_.SetClient(client);
        client_addr_param_ = master_addr;
        timer.LogResponse("error_code=", ErrorCode::OK);
        return ErrorCode::OK;
    }
}

tl::expected<bool, ErrorCode> MasterClient::ExistKey(
    const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);

#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::ExistKey;
        req.key = object_key;
        std::string req_str = req.serialize();
        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            return tl::unexpected(ErrorCode::RPC_FAIL);
        }
        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());

        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::ExistKey CXL path failed";
            return tl::unexpected(static_cast<ErrorCode>(resp.error_code));
        }
        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::ExistKey CXL timing: total=" << total_latency << "us" << std::endl;
        return resp.exist;
    }

#endif
    // coro rpc client path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        VLOG(1) << "MasterClient::ExistKey RPC path failed: error=Client not available";
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }
    auto start_time2 = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<&WrappedMasterService::ExistKey>(object_key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<bool, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to check key existence: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    
    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::ExistKey coro timing: total=" << total_latency2 << "us" << std::endl;
    return result;
}

tl::expected<std::string, ErrorCode> MasterClient::cxlChannelHandshake() {
    ScopedVLogTimer timer(1, "MasterClient::cxlChannelHandshake");
    timer.LogRequest("action=handshake");

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::cxlChannelHandshake>();
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<std::string, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to perform handshake: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<bool, ErrorCode> MasterClient::CreateCxlChannelRpcClient() {
#if defined(STORE_USE_CXL_CHANNEL)
    LOG(INFO) << "beginning to initialize cxl channel ...";
    auto channel_name_result = cxlChannelHandshake();
    if (!channel_name_result) {
        LOG(ERROR) << "Failed to get cxl channel name: " << toString(channel_name_result.error());
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }

    LOG(INFO) << "cxl channel name: " << *channel_name_result;
    auto cxl_config = GetCxlChannelConfig();
    auto channel_opt = cxl_shm::Channel::Create(
        *channel_name_result,
        "store_client_" + *channel_name_result,
        cxl_config.dev_dax_path,
        cxl_config.dev_dax_size,
        cxl_config.cxl_channel_master_addr,
        cxl_shm::ChannelMode::CXL_SHM_REQ,
        false,
        cxl_config.dev_dax_offset
    );
    if (!channel_opt.has_value()) {
        LOG(ERROR) << "Failed to create cxl channel: " << *channel_name_result;
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }
    auto channel = *channel_opt;

    // BindAsRpcClient() internally calls Bind() first and then upgrades the
    // channel to RPC_CLIENT mode (spawning an IO thread for response demux).
    // Do NOT call channel->Bind() here separately: Bind() refuses to run when
    // role_ is already CLIENT, which would make the internal Bind() inside
    // BindAsRpcClient() fail with INVALID_PARAMS.
    auto rpc_bind = channel->BindAsRpcClient();
    if (!rpc_bind.has_value()) {
        LOG(ERROR) << "Failed to bind cxl channel as RPC client: "
                   << *channel_name_result;
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }
    LOG(INFO) << "Successfully bound cxl channel as RPC client: "
              << *channel_name_result;

    cxl_channel_accessor_.SetChannel(channel, *channel_name_result);
    LOG(INFO) << "end of initialize cxl channel ...";
    
    // 启动心跳线程
    // 使用 condition_variable::wait_for 替代 sleep_for，使 Reset 时能通过
    // notify_all 立即打断等待；同时放弃 detach，改由 Reset 端 join，消除
    // 心跳线程在 channel Unbind 之后仍访问已失效 channel / 悬空 this 的风险。
    heartbeat_running_.store(true);
    heartbeat_thread_ = std::thread([this, channel]() {
        // 立即发送一次心跳
        this->sendHeartbeat(channel);
        // this->BatchExistKey({"wcg1", "wcg2"});

        // 每隔 30 秒发送一次心跳，或在 heartbeat_running_ 被置 false 后立即退出
        std::unique_lock<std::mutex> lk(heartbeat_mtx_);
        while (heartbeat_running_.load()) {
            // wait_for 返回 true 表示 pred 满足（即被要求停止）
            if (heartbeat_cv_.wait_for(lk, std::chrono::seconds(30),
                                       [this] { return !heartbeat_running_.load(); })) {
                break;
            }
            // 真正发心跳时释放锁，避免阻塞 Reset 侧的 notify/停止动作
            lk.unlock();
            this->sendHeartbeat(channel);
            lk.lock();
        }
    });
    LOG(INFO) << "Heartbeat thread started for CXL channel";
    
    return true;
#else
    return false;
#endif
}

tl::expected<bool, ErrorCode> MasterClient::ResetCxlChannelRpcClient() {
#if defined(STORE_USE_CXL_CHANNEL)
    // 1) 请求心跳线程停止：置 flag 并立即通过 cv 打断其 wait_for。
    //    锁 heartbeat_mtx_ 是为了与心跳线程内部的 wait_for 形成 happens-before，
    //    确保它下一次被唤醒时一定能看到 flag=false。
    {
        std::lock_guard<std::mutex> lk(heartbeat_mtx_);
        heartbeat_running_.store(false);
    }
    heartbeat_cv_.notify_all();

    // 2) 等心跳线程真正退出，保证之后不再有任何人访问即将被 Unbind 的 channel。
    //    由于 wait_for 会被立即唤醒，join 仅消耗 μs 级时间，不会阻塞调用方业务。
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }

    auto channel = cxl_channel_accessor_.GetChannel();
    if (!channel) {
        LOG(ERROR) << "CXL channel not available";
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }

    // Symmetric teardown for BindAsRpcClient(): UnbindAsRpcClient() stops the
    // library's demux IO thread, drains pending request_id -> response
    // mappings, and then performs the underlying Unbind(). This replaces the
    // old StopIo() + Unbind() pairing.
    auto result = channel->UnbindAsRpcClient();
    if (!result.has_value()) {
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }
    cxl_channel_accessor_.SetChannel(nullptr, "");

    return true;
#else
    return false;
#endif
}

std::vector<tl::expected<bool, ErrorCode>> MasterClient::BatchExistKey(
    const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());
#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::BatchExistKey;
        req.keys = object_keys;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            timer.LogResponse("error=CXL Call failed");
            return std::vector<tl::expected<bool, ErrorCode>>(
                object_keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::BatchExistKey CXL path failed";
            timer.LogResponse("error=CXL request failed");
            return std::vector<tl::expected<bool, ErrorCode>>(
                object_keys.size(), tl::make_unexpected(static_cast<ErrorCode>(resp.error_code)));
        }

        // 构建返回结果
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(resp.exists.size());
        for (int8_t exist : resp.exists) {
            results.emplace_back(static_cast<bool>(exist));
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::BatchExistKey CXL timing: total=" << total_latency << "us, keys_count=" << object_keys.size() << std::endl;
        timer.LogResponse("result=", results.size(), " keys (CXL)");
        return results;
    }
#endif
    // coro rpc client path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return std::vector<tl::expected<bool, ErrorCode>>(
            object_keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
    }

    auto start_time2 = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<&WrappedMasterService::BatchExistKey>(object_keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<tl::expected<bool, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to check batch key existence: "
                           << result.error().msg;
                std::vector<tl::expected<bool, ErrorCode>> error_results;
                error_results.reserve(object_keys.size());
                for (size_t i = 0; i < object_keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());

    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::BatchExistKey coro timing: total=" << total_latency2 << "us, keys_count=" << object_keys.size() << std::endl;
    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::GetReplicaList(const std::string& object_key) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", object_key);
#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::GetReplicaList;
        req.key = object_key;
        std::string req_str = req.serialize();
        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.error_code != 0) {
            return tl::make_unexpected(static_cast<ErrorCode>(resp.error_code));
        }
        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::GetReplicaList CXL timing: total=" << total_latency << "us" << std::endl;
        return resp.replica_list;
    }
#endif
    // http RPC path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }
    auto start_time2 = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<&WrappedMasterService::GetReplicaList>(object_key);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get replica list: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::GetReplicaList coro timing: total=" << total_latency2 << "us" << std::endl;
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("keys_count=", object_keys.size());

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            error_results;
        error_results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            error_results.emplace_back(
                tl::make_unexpected(ErrorCode::RPC_FAIL));
        }
        return error_results;
    }

    auto request_result =
        client->send_request<&WrappedMasterService::BatchGetReplicaList>(
            object_keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get batch replica list: "
                           << result.error().msg;
                std::vector<
                    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
                    error_results;
                error_results.reserve(object_keys.size());
                for (size_t i = 0; i < object_keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());

    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::PutStart(const std::string& key,
                       const std::vector<size_t>& slice_lengths,
                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::PutStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

#if 0 //defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::PutStart;
        req.key = key;

        // Convert size_t to uint64_t for CXL RPC
        req.slice_lengths.reserve(slice_lengths.size());
        for (const auto& length : slice_lengths) {
            req.slice_lengths.push_back(static_cast<uint64_t>(length));
        }
        req.config = config;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            timer.LogResponse("error=CXL Call failed");
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::PutStart CXL path failed";
            timer.LogResponse("error=CXL request failed");
            return tl::make_unexpected(static_cast<ErrorCode>(resp.error_code));
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::PutStart CXL timing: " 
                  << "total=" << total_latency
                  << "us, key=" << key << std::endl;

        timer.LogResponse("result=replica_list (CXL)");
        return resp.replica_list;
    }
#endif
    // coro rpc client path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto start_time2 = std::chrono::steady_clock::now();

    // Convert size_t to uint64_t for RPC
    std::vector<uint64_t> rpc_slice_lengths;
    rpc_slice_lengths.reserve(slice_lengths.size());
    for (const auto& length : slice_lengths) {
        rpc_slice_lengths.push_back(length);
    }

    auto request_result = client->send_request<&WrappedMasterService::PutStart>(
        key, rpc_slice_lengths, config);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to start put operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::PutStart coro timing: total=" << total_latency2 << "us, key=" << key << std::endl;
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterClient::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutStart");
    timer.LogRequest("keys_count=", keys.size());

#if 0 //defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();

        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::BatchPutStart;
        req.keys = keys;
        req.batch_slice_lengths = slice_lengths; // 使用 batch_slice_lengths 支持每个 key 独立的 slice_lengths
        req.config = config;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            timer.LogResponse("error=CXL Call failed");
            return std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
                keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::BatchPutStart CXL path failed";
            timer.LogResponse("error=CXL request failed");
            return std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
                keys.size(), tl::make_unexpected(static_cast<ErrorCode>(resp.error_code)));
        }

        // 构建返回结果
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>> results;
        results.reserve(resp.batch_replica_lists.size());
        for (const auto& replica_list : resp.batch_replica_lists) {
            results.emplace_back(replica_list);
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

        std::cout << "MasterClient::BatchPutStart CXL timing: total=" << total_latency 
            << "us, keys_count=" << keys.size() 
            << " req.size=" << req_str.size()
            << " response.size=" << call_result.value().size()
            << std::endl;

        timer.LogResponse("result=", results.size(), " operations (CXL)");
        return results;
    }
#endif
    // coro rpc client path
    auto start_time2 = std::chrono::steady_clock::now();
    
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            error_results(keys.size(),
                          tl::make_unexpected(ErrorCode::RPC_FAIL));
        return error_results;
    }

    auto request_result =
        client->send_request<&WrappedMasterService::BatchPutStart>(
            keys, slice_lengths, config);

    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                // create a vector full of error
                std::vector<
                    tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
                    error_results(keys.size(),
                                  tl::make_unexpected(ErrorCode::RPC_FAIL));
                LOG(ERROR) << "Failed to start batch put operation, error"
                           << result.error().msg;
                co_return error_results;
            }
            co_return result->result();
        }());

    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    
    std::cout << "MasterClient::BatchPutStart coro timing: total=" << total_latency2 << "us, keys_count=" << keys.size() << std::endl;

    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutEnd(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutEnd");
    timer.LogRequest("key=", key);

#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::PutEnd;
        req.key = key;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            timer.LogResponse("error=CXL Call failed");
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::PutEnd CXL path failed";
            timer.LogResponse("error=CXL request failed");
            return tl::make_unexpected(static_cast<ErrorCode>(resp.error_code));
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::PutEnd CXL timing: total=" << total_latency << "us, key=" << key << std::endl;

        timer.LogResponseExpected(tl::expected<void, ErrorCode>{});
        return {};
    }
#endif
    // coro rpc client path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto start_time2 = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<&WrappedMasterService::PutEnd>(key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to end put operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::PutEnd coro timing: total=" << total_latency2 << "us, key=" << key << std::endl;
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", keys.size());

#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::BatchPutEnd;
        req.keys = keys;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            timer.LogResponse("error=CXL Call failed");
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.status != CxlChannelRpcStatus::Success) {
            LOG(ERROR) << "MasterClient::BatchPutEnd CXL path deserialize failed: resp.error_code=" << resp.error_code   << std::endl;
            timer.LogResponse("error=CXL request failed");
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::make_unexpected(static_cast<ErrorCode>(resp.error_code)));
        }

        // 构建返回结果（全部成功）
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back();
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::BatchPutEnd CXL timing: total=" << total_latency << "us, keys_count=" << keys.size() << std::endl;

        timer.LogResponse("result=", results.size(), " operations (CXL)");
        return results;
    }
#endif
    // coro rpc client path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<void, ErrorCode>> error_results;
        error_results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            error_results.emplace_back(
                tl::make_unexpected(ErrorCode::RPC_FAIL));
        }
        return error_results;
    }

    auto start_time2 = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<&WrappedMasterService::BatchPutEnd>(keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<tl::expected<void, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to end batch put operation: "
                           << result.error().msg;
                std::vector<tl::expected<void, ErrorCode>> error_results;
                error_results.reserve(keys.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());
    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::BatchPutEnd coro timing: total=" << total_latency2 << "us, keys_count=" << keys.size() << std::endl;
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutRevoke(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutRevoke");
    timer.LogRequest("key=", key);

#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::PutRevoke;
        req.key = key;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            timer.LogResponse("error=CXL Call failed");
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::PutRevoke CXL path failed";
            timer.LogResponse("error=CXL request failed");
            return tl::make_unexpected(static_cast<ErrorCode>(resp.error_code));
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::PutRevoke CXL timing: total=" << total_latency << "us, key=" << key << std::endl;

        timer.LogResponseExpected(tl::expected<void, ErrorCode>{});
        return {};
    }
#endif
    // coro rpc client path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto start_time2 = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<&WrappedMasterService::PutRevoke>(key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to revoke put operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::PutRevoke coro timing: total=" << total_latency2 << "us, key=" << key << std::endl;
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::BatchPutRevoke;
        req.keys = keys;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(ERROR) << "CXL Call failed";
            timer.LogResponse("error=CXL Call failed");
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());
        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::BatchPutRevoke CXL path failed";
            timer.LogResponse("error=CXL request failed");
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::make_unexpected(static_cast<ErrorCode>(resp.error_code)));
        }

        // 构建返回结果（全部成功）
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back();
        }
        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::BatchPutRevoke CXL timing: total=" << total_latency << "us, keys_count=" << keys.size() << std::endl;
        timer.LogResponse("result=", results.size(), " operations (CXL)");
        return results;
    }
#endif
    // coro rpc client path
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        std::vector<tl::expected<void, ErrorCode>> error_results;
        error_results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            error_results.emplace_back(
                tl::make_unexpected(ErrorCode::RPC_FAIL));
        }
        return error_results;
    }

    auto start_time2 = std::chrono::steady_clock::now();
    auto request_result =
        client->send_request<&WrappedMasterService::BatchPutRevoke>(keys);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<std::vector<tl::expected<void, ErrorCode>>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to revoke batch put operation: "
                           << result.error().msg;
                std::vector<tl::expected<void, ErrorCode>> error_results;
                error_results.reserve(keys.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    error_results.emplace_back(
                        tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                co_return error_results;
            }
            co_return result->result();
        }());
    auto end_time2 = std::chrono::steady_clock::now();
    auto total_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(end_time2 - start_time2).count();
    std::cout << "MasterClient::BatchPutRevoke coro timing: total=" << total_latency2 << "us, keys_count=" << keys.size() << std::endl;
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::Remove(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::Remove>(key);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to remove object: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveAll() {
    ScopedVLogTimer timer(1, "MasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects");

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::RemoveAll>();
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<long, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to remove all objects: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountSegment(
    const Segment& segment, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("base=", segment.base, ", size=", segment.size,
                     ", name=", segment.name, ", id=", segment.id,
                     ", client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
        Lazy<async_rpc_result<tl::expected<void, ErrorCode>>> handler =
            co_await client->send_request<&WrappedMasterService::MountSegment>(
                segment, client_id);
        async_rpc_result<tl::expected<void, ErrorCode>> result =
            co_await handler;
        if (!result) {
            LOG(ERROR) << "Failed to mount segment due to rpc error";
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        co_return result->result();
    }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::ReMountSegment");
    timer.LogRequest("segments_num=", segments.size(),
                     ", client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
        Lazy<async_rpc_result<tl::expected<void, ErrorCode>>> handler =
            co_await client
                ->send_request<&WrappedMasterService::ReMountSegment>(
                    segments, client_id);
        async_rpc_result<tl::expected<void, ErrorCode>> result =
            co_await handler;
        if (!result) {
            LOG(ERROR) << "Failed to remount segment due to rpc error";
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        co_return result->result();
    }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::UnmountSegment>(segment_id,
                                                                    client_id);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to unmount segment: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<PingResponse, ErrorCode>
MasterClient::Ping(const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::Ping");
    timer.LogRequest("client_id=", client_id);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::Ping>(client_id);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<tl::expected<PingResponse, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to ping master: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> MasterClient::GetFsdir() {
    ScopedVLogTimer timer(1, "MasterClient::GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::GetFsdir>();
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<tl::expected<std::string, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to get fsdir: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterClient::MigrateStart(const std::string& key,
                       const std::vector<size_t>& slice_lengths,
                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::MigrateStart");
    timer.LogRequest("key=", key, ", slice_count=", slice_lengths.size());

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    // Convert size_t to uint64_t for RPC
    std::vector<uint64_t> rpc_slice_lengths;
    rpc_slice_lengths.reserve(slice_lengths.size());
    for (const auto& length : slice_lengths) {
        rpc_slice_lengths.push_back(length);
    }

    auto request_result = client->send_request<&WrappedMasterService::MigrateStart>(
        key, rpc_slice_lengths, config);
    auto result = coro::syncAwait(
        [&]() -> coro::Lazy<
                  tl::expected<std::vector<Replica::Descriptor>, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to start migrate operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MigrateRevoke(const std::string& key,
                                                          const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::MigrateRevoke");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::MigrateRevoke>(key, config);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to revoke put operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MigrateEnd(const std::string& key,
                                                       const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::MigrateEnd");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result =
        client->send_request<&WrappedMasterService::MigrateEnd>(key, config);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<void, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                LOG(ERROR) << "Failed to end migrate operation: "
                           << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}

#if defined(STORE_USE_CXL_CHANNEL)
void MasterClient::sendHeartbeat(std::shared_ptr<cxl_shm::Channel> channel) {
    try {
        auto start_time = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::Heartbeat;
        std::string req_str = req.serialize();

        auto call_result = channel->Call(req_str.data(), req_str.size());
        if (!call_result.has_value()) {
            LOG(WARNING) << "CXL heartbeat Call failed, error="
                         << call_result.error();
            return;
        }

        CxlChannelRpcResponse resp =
            CxlChannelRpcResponse::deserialize(call_result.value());

        if (resp.status != CxlChannelRpcStatus::Success || resp.error_code != 0) {
            LOG(WARNING) << "CXL heartbeat response error: status="
                        << static_cast<int>(resp.status)
                        << ", error_code=" << resp.error_code;
            return;
        }

        auto end_time = std::chrono::steady_clock::now();
        auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        std::cout << "MasterClient::sendHeartbeat CXL timing: total=" << total_latency << "us" << std::endl;

        VLOG(1) << "CXL heartbeat sent and received successfully";
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception in sendHeartbeat: " << e.what();
    }
}
#endif

tl::expected<DegradeMsg, ErrorCode> MasterClient::PopMasterMQ(const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::PeakMasterMQ");
    // LOG(WARNING) << "MasterClient::PopMasterMQ, client_id:" << client_id;
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto request_result = client->send_request<&WrappedMasterService::PopMasterMQ>(client_id);
    auto result =
        coro::syncAwait([&]() -> coro::Lazy<tl::expected<DegradeMsg, ErrorCode>> {
            auto result = co_await co_await request_result;
            if (!result) {
                // LOG(ERROR) << "Failed to pop master MQ operation: "
                //            << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            // LOG(WARNING) << "MasterClient::PopMasterMQ, get result, client_id:" << client_id;
            co_return result->result();
        }());
    timer.LogResponseExpected(result);
    return result;
}



}  // namespace mooncake
