
#pragma once

#include <string>
#include <vector>
#include "types.h"
// #include "ylt/struct_json/json_reader.h"
// #include "ylt/struct_json/json_writer.h"
#include "ylt/struct_pack.hpp"
#define STORE_CXL_CHANNEL_MSG_SIZE 4096

namespace mooncake {

enum class CxlChannelRpcStatus {
    Success = 0,
    Fail = 1
};

enum class CxlChannelRpcOp {
    PutStart = 0,
    PutEnd = 1,
    PutRevoke = 2,
    ExistKey = 3,
    GetReplicaList = 4,
    Heartbeat = 5  // 心跳包类型
};
// YLT_REFL_ENUM(CxlChannelRpcOp, PutStart, PutEnd, PutRevoke, ExistKey, GetReplicaList, Heartbeat);

struct CxlChannelRpcRequest {
    CxlChannelRpcOp op;
    std::string key;
    std::vector<uint64_t> slice_lengths; // for PutStart
    ReplicateConfig config;              // for PutStart
    // 可扩展更多字段

    std::string serialize() const {
        // std::string json;
        // struct_json::to_json(*this, json);
        // return json;
        auto buffer = struct_pack::serialize(*this);
        return std::string(buffer.begin(), buffer.end());
    }
    static CxlChannelRpcRequest deserialize(const std::string& data) {
        // CxlChannelRpcRequest req;
        // struct_json::from_json(req, data);
        // return req;
        auto ret = struct_pack::deserialize<CxlChannelRpcRequest>(data);
        if (ret) {
            return std::move(ret.value());
        }
        throw std::runtime_error("Deserialize failed");
    }
};
YLT_REFL(CxlChannelRpcRequest, op, key, slice_lengths, config);

struct CxlChannelRpcResponse {
    CxlChannelRpcStatus status = CxlChannelRpcStatus::Fail;
    int32_t error_code = 0;
    std::string error_msg;
    bool exist = false; // for ExistKey
    std::vector<Replica::Descriptor> replica_list; // for GetReplicaList, PutStart
    // 可扩展

    std::string serialize() const {
        // std::string json;
        // struct_json::to_json(*this, json);
        // return json;
        auto buffer = struct_pack::serialize(*this);
        return std::string(buffer.begin(), buffer.end());
    }
    static CxlChannelRpcResponse deserialize(const std::string& data) {
        // CxlChannelRpcResponse resp;
        // struct_json::from_json(resp, data);
        // return resp;
        auto ret = struct_pack::deserialize<CxlChannelRpcResponse>(data);
        if (ret) {
            return std::move(ret.value());
        }
        throw std::runtime_error("Deserialize failed");
    }

    YLT_REFL(CxlChannelRpcResponse, status, error_code, error_msg, exist, replica_list);
};

} // namespace mooncake