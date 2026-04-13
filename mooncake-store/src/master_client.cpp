#include "master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <string>
#include <vector>
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
MasterClient::~MasterClient() = default;

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

    std::cout << "MasterClient::ExistKey: " << object_key << std::endl;;
    // #undef STORE_USE_CXL_CHANNEL
#if defined(STORE_USE_CXL_CHANNEL)
    #if 0
    std::cout << "MasterClient::ExistKey CXL path" << std::endl;;
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        char data[STORE_CXL_CHANNEL_MSG_SIZE] = "12345678";
        for (int i = 0; i < 2; i++) {
            // 测量 Send 时间
            auto send_start = std::chrono::steady_clock::now();
            auto send_result = channel->Send(data, 128);
            auto send_end = std::chrono::steady_clock::now();
            auto send_latency = std::chrono::duration_cast<std::chrono::microseconds>(send_end - send_start);
            
            // 测量 Recv 时间
            auto recv_start = std::chrono::steady_clock::now();
            auto recv_result = channel->Recv(data, 128);
            auto recv_end = std::chrono::steady_clock::now();
            auto recv_latency = std::chrono::duration_cast<std::chrono::microseconds>(recv_end - recv_start);
            
            // 计算总延迟
            auto total_latency = std::chrono::duration_cast<std::chrono::microseconds>(recv_end - send_start);
            
            std::cout << "MasterClient::ExistKey CXL path success, "
                      << "send=" << send_latency.count() << "us, "
                      << "recv=" << recv_latency.count() << "us, "
                      << "total=" << total_latency.count() << "us" << std::endl;

            auto cxl_send2 = std::chrono::steady_clock::now();
            CxlChannelRpcRequest req;
            req.op = CxlChannelRpcOp::ExistKey;
            req.key = object_key;
            std::string req_str = req.serialize();
            auto deserialized_req = CxlChannelRpcRequest::deserialize(req_str);
            
            // 使用 do_not_optimize 防止编译器优化掉序列化和反序列化操作
            do_not_optimize(req_str);
            do_not_optimize(deserialized_req);

            CxlChannelRpcResponse resp;
            resp.status = CxlChannelRpcStatus::Success;
            resp.error_code = 0;
            resp.exist = true;
            std::string response_str = resp.serialize();
            auto deserialized_resp = CxlChannelRpcResponse::deserialize(response_str);
            
            // 使用 do_not_optimize 防止编译器优化掉序列化和反序列化操作
            do_not_optimize(response_str);
            do_not_optimize(deserialized_resp);

            auto cxl_recv2 = std::chrono::steady_clock::now();
            auto cxl_latency2 = std::chrono::duration_cast<std::chrono::microseconds>(cxl_recv2 - cxl_send2);
            
            std::cout << "MasterClient::ExistKey CXL path success, serialize latency=" << cxl_latency2.count() << "us" 
                      << ", req_size=" << req_str.size() << ", resp_size=" << response_str.size() << std::endl;
        }


        return true;
    }
    #endif

    std::cout << "MasterClient::ExistKey CXL path" << std::endl;
    auto channel = cxl_channel_accessor_.GetChannel();
    if (channel) {
        auto cxl_start = std::chrono::steady_clock::now();
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::ExistKey;
        req.key = object_key;
        std::string req_str = req.serialize();
        auto send_result = channel->Send(req_str.data(), req_str.size());
        if (!send_result.has_value()) {
            LOG(ERROR) << "CXL Send failed";
            return tl::unexpected(ErrorCode::RPC_FAIL);
        }
        char resp_buf[STORE_CXL_CHANNEL_MSG_SIZE] = {0};
        auto recv_result = channel->Recv(resp_buf, STORE_CXL_CHANNEL_MSG_SIZE);
        if (!recv_result.has_value()) {
            LOG(ERROR) << "CXL Recv failed";
            return tl::unexpected(ErrorCode::RPC_FAIL);
        }
        CxlChannelRpcResponse resp = CxlChannelRpcResponse::deserialize(std::string(resp_buf, recv_result.value()));
        auto cxl_end = std::chrono::steady_clock::now();
        auto cxl_latency = std::chrono::duration_cast<std::chrono::microseconds>(cxl_end - cxl_start);
        
        if (resp.status != CxlChannelRpcStatus::Success) {
            VLOG(1) << "MasterClient::ExistKey CXL path failed"
                    << ", latency=" << cxl_latency.count() << "us";
            return tl::unexpected(static_cast<ErrorCode>(resp.error_code));
        }
        std::cout << "MasterClient::ExistKey CXL path success"
                    << ", latency=" << cxl_latency.count() << "us" 
                    << ", exist=" << resp.exist << std::endl;
        return resp.exist;
    }

#endif
    // coro rpc client path
    std::cout << "MasterClient::ExistKey http path" << std::endl;
    auto rpc_start = std::chrono::steady_clock::now();
    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        auto rpc_end = std::chrono::steady_clock::now();
        auto rpc_latency = std::chrono::duration_cast<std::chrono::microseconds>(rpc_end - rpc_start);
        VLOG(1) << "MasterClient::ExistKey RPC path failed: error=Client not available, latency=" << rpc_latency.count() << "us";
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }
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
    
    auto rpc_end = std::chrono::steady_clock::now();
    auto rpc_latency = std::chrono::duration_cast<std::chrono::microseconds>(rpc_end - rpc_start);
    
    if (!result) {
        VLOG(1) << "MasterClient::ExistKey RPC path failed" 
                << ", latency=" << rpc_latency.count() << "us";
    } else {
        std::cout << "MasterClient::ExistKey RPC path success"
                << ", latency=" << rpc_latency.count() << "us";
    }
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

tl::expected<bool, ErrorCode> MasterClient::CreateCxlChannelRpcClient(const std::string& master_server_addr) {
#if defined(STORE_USE_CXL_CHANNEL)
    LOG(INFO) << "beginning to initialize cxl channel ...";
    auto channel_name_result = cxlChannelHandshake();
    if (!channel_name_result) {
        LOG(ERROR) << "Failed to get cxl channel name: " << toString(channel_name_result.error());
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }

    LOG(INFO) << "cxl channel name: " << *channel_name_result;
    auto channel_opt = cxl_shm::Channel::Create(
        *channel_name_result,
        "store_client_" + *channel_name_result,
        "/dev/dax0.0",
        1024ULL * 1024ULL * 1024ULL,
        master_server_addr,
        cxl_shm::ChannelMode::CXL_SHM_REQ,
        false);
    if (!channel_opt.has_value()) {
        LOG(ERROR) << "Failed to create cxl channel: " << *channel_name_result;
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }
    auto channel = *channel_opt;
    auto bind_result = channel->Bind();
    if (!bind_result.has_value()) {
        LOG(ERROR) << "Failed to bind cxl channel: " << *channel_name_result;
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }
    LOG(INFO) << "Successfully bound cxl channel: " << *channel_name_result;
    cxl_channel_accessor_.SetChannel(channel, *channel_name_result);
    LOG(INFO) << "end of initialize cxl channel ...";
    
    // 启动心跳线程
    heartbeat_running_.store(true);
    heartbeat_thread_ = std::thread([this, channel]() {
        // 立即发送一次心跳
        this->sendHeartbeat(channel);
        
        // 每隔1分钟发送一次心跳
        while (heartbeat_running_.load()) {
            std::this_thread::sleep_for(std::chrono::minutes(1));
            if (heartbeat_running_.load()) {
                this->sendHeartbeat(channel);
            }
        }
    });
    heartbeat_thread_.detach();
    LOG(INFO) << "Heartbeat thread started for CXL channel";
    
    return true;
#else
    return false;
#endif
}

tl::expected<bool, ErrorCode> MasterClient::ResetCxlChannelRpcClient() {
#if defined(STORE_USE_CXL_CHANNEL)
    auto channel = cxl_channel_accessor_.GetChannel();
    if (!channel) {
        LOG(ERROR) << "CXL channel not available";
        return tl::unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = channel->Unbind();
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

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return std::vector<tl::expected<bool, ErrorCode>>(
            object_keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
    }
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
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::GetReplicaList;
        req.key = object_key;
        std::string req_str = req.serialize();
        auto send_result = channel->Send(req_str.data(), req_str.size());
        if (!send_result.has_value()) {
            LOG(ERROR) << "CXL Send failed";
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        char resp_buf[STORE_CXL_CHANNEL_MSG_SIZE] = {0};
        auto recv_result = channel->Recv(resp_buf, STORE_CXL_CHANNEL_MSG_SIZE);
        if (!recv_result.has_value()) {
            LOG(ERROR) << "CXL Recv failed";
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        CxlChannelRpcResponse resp = CxlChannelRpcResponse::deserialize(std::string(resp_buf, recv_result.value()));
        if (resp.error_code != 0) {
            return tl::make_unexpected(static_cast<ErrorCode>(resp.error_code));
        }
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

    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutEnd(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutEnd");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

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
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutEnd");
    timer.LogRequest("keys_count=", keys.size());

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
    timer.LogResponse("result=", result.size(), " operations");
    return result;
}

tl::expected<void, ErrorCode> MasterClient::PutRevoke(const std::string& key) {
    ScopedVLogTimer timer(1, "MasterClient::PutRevoke");
    timer.LogRequest("key=", key);

    auto client = client_accessor_.GetClient();
    if (!client) {
        LOG(ERROR) << "Client not available";
        timer.LogResponse("error=Client not available");
        return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

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
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> MasterClient::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchPutRevoke");
    timer.LogRequest("keys_count=", keys.size());

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
        CxlChannelRpcRequest req;
        req.op = CxlChannelRpcOp::Heartbeat;
        std::string req_str = req.serialize();
        
        auto send_result = channel->Send(req_str.data(), req_str.size());
        if (!send_result.has_value()) {
            LOG(WARNING) << "CXL heartbeat Send failed";
            return;
        }
        
        // 接收响应
        char resp_buf[STORE_CXL_CHANNEL_MSG_SIZE] = {0};
        auto recv_result = channel->Recv(resp_buf, STORE_CXL_CHANNEL_MSG_SIZE);
        if (!recv_result.has_value()) {
            LOG(WARNING) << "CXL heartbeat Recv failed";
            return;
        }
        
        // 反序列化并验证响应
        CxlChannelRpcResponse resp = CxlChannelRpcResponse::deserialize(std::string(resp_buf, recv_result.value()));
        if (resp.status != CxlChannelRpcStatus::Success || resp.error_code != 0) {
            LOG(WARNING) << "CXL heartbeat response error: status=" 
                        << static_cast<int>(resp.status) 
                        << ", error_code=" << resp.error_code;
            return;
        }
        
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
