
#pragma once

#include <cstdlib>
#include <string>
#include <vector>
#include "types.h"
#include "ylt/struct_pack.hpp"
#include <iostream>

#define STORE_CXL_CHANNEL_MSG_SIZE 2048000

namespace mooncake {

struct CxlChannelConfig {
    std::string dev_dax_path;
    std::size_t dev_dax_size;
    std::size_t dev_dax_offset;
    std::string cxl_channel_master_addr;
};

inline CxlChannelConfig GetCxlChannelConfig() {
    const char* env_channel_dev_path = std::getenv("MC_CXL_CHANNEL_DEV_PATH");
    const char* env_dev_path = std::getenv("MC_CXL_DEV_PATH");
    const char* env_channel_dev_size = std::getenv("MC_CXL_CHANNEL_DEV_SIZE");
    const char* env_channel_dev_offset = std::getenv("MC_CXL_CHANNEL_DEV_OFFSET");
    const char* env_dev_size = std::getenv("MC_CXL_DEV_SIZE");
    const char* env_dev_offset = std::getenv("MC_CXL_DEV_OFFSET");
    const char* env_cxl_channel_master = std::getenv("MOONCAKE_CXL_CHANNEL_MASTER");
    const char* env_master = std::getenv("MOONCAKE_MASTER");

    std::string dev_dax_path;
    if (env_channel_dev_path && *env_channel_dev_path) {
        dev_dax_path = env_channel_dev_path;
    } else if (env_dev_path && *env_dev_path) {
        dev_dax_path = env_dev_path;
    } else {
        LOG(ERROR) << "MC_CXL_CHANNEL_DEV_PATH and MC_CXL_DEV_PATH are not set or empty";
        std::exit(EXIT_FAILURE);
    }

    std::size_t dev_dax_size = 1073741824ULL;
    if (env_channel_dev_size && *env_channel_dev_size) {
        char* end = nullptr;
        unsigned long long val = std::strtoull(env_channel_dev_size, &end, 10);
        if (end == env_channel_dev_size || *end != '\0') {
            LOG(ERROR) << "Invalid MC_CXL_CHANNEL_DEV_SIZE: " << env_channel_dev_size;
            std::exit(EXIT_FAILURE);
        }
        dev_dax_size = static_cast<std::size_t>(val);
    }

    std::size_t dev_dax_offset = 0;
    if (env_channel_dev_offset && *env_channel_dev_offset) {
        char* end = nullptr;
        unsigned long long val = std::strtoull(env_channel_dev_offset, &end, 10);
        if (end == env_channel_dev_offset || *end != '\0') {
            LOG(ERROR) << "Invalid MC_CXL_CHANNEL_DEV_OFFSET: " << env_channel_dev_offset;
            std::exit(EXIT_FAILURE);
        }
        dev_dax_offset = static_cast<std::size_t>(val);
    }

    if (env_dev_path && *env_dev_path && dev_dax_path == env_dev_path) {
        std::size_t other_offset = 0;
        if (env_dev_offset && *env_dev_offset) {
            char* end = nullptr;
            unsigned long long val = std::strtoull(env_dev_offset, &end, 10);
            if (end != env_dev_offset && *end == '\0') {
                other_offset = static_cast<std::size_t>(val);
            }
        }

        std::size_t other_size = DEFAULT_CXL_SIZE;
        if (env_dev_size && *env_dev_size) {
            char* end = nullptr;
            unsigned long long val = std::strtoull(env_dev_size, &end, 10);
            if (end == env_dev_size || *end != '\0') {
                LOG(ERROR) << "Invalid MC_CXL_DEV_SIZE: " << env_dev_size;
                std::exit(EXIT_FAILURE);
            }
            other_size = static_cast<std::size_t>(val);
        }

        uint64_t channel_begin = dev_dax_offset;
        uint64_t channel_end = dev_dax_offset + dev_dax_size;
        uint64_t other_begin = other_offset;
        uint64_t other_end = other_offset + other_size;
        if (channel_begin < other_end && other_begin < channel_end) {
            LOG(ERROR) << "MC_CXL_CHANNEL_DEV_PATH and MC_CXL_DEV_PATH are the same and the reserved ranges overlap"
                       << ", channel_offset=" << dev_dax_offset
                       << ", channel_size=" << dev_dax_size
                       << ", default_offset=" << other_offset
                       << ", default_size=" << other_size;
            std::exit(EXIT_FAILURE);
        }
    }

    std::string cxl_channel_master_addr;
    if (env_cxl_channel_master && *env_cxl_channel_master) {
        cxl_channel_master_addr = env_cxl_channel_master;
    } else if (env_master && *env_master) {
        std::string master_addr = env_master;
        auto pos = master_addr.rfind(':');
        if (pos == std::string::npos || pos + 1 >= master_addr.size()) {
            LOG(ERROR) << "MOONCAKE_MASTER value is invalid: " << master_addr;
            std::exit(EXIT_FAILURE);
        }
        std::string host = master_addr.substr(0, pos);
        std::string port_str = master_addr.substr(pos + 1);
        char* end = nullptr;
        unsigned long long port = std::strtoull(port_str.c_str(), &end, 10);
        if (end == port_str.c_str() || *end != '\0' || port == 0 || port > 65535) {
            LOG(ERROR) << "MOONCAKE_MASTER port is invalid: " << port_str;
            std::exit(EXIT_FAILURE);
        }
        if (port == 65535ULL) {
            LOG(ERROR) << "MOONCAKE_MASTER port too large to increment: " << port;
            std::exit(EXIT_FAILURE);
        }
        cxl_channel_master_addr = host + ":" + std::to_string(port + 1);
    } else {
        LOG(ERROR) << "MOONCAKE_CXL_CHANNEL_MASTER and MOONCAKE_MASTER are not set or empty";
        std::exit(EXIT_FAILURE);
    }

    LOG(INFO) << "GetCxlChannelConfig dax_path=" << dev_dax_path
              << " dax_size=" << dev_dax_size
              << " dax_offset=" << dev_dax_offset
              << " cxl_channel_master_addr=" << cxl_channel_master_addr;

    return CxlChannelConfig{std::move(dev_dax_path), dev_dax_size, dev_dax_offset, std::move(cxl_channel_master_addr)};
}

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
    Heartbeat = 5,  // 心跳包类型
    BatchExistKey = 6,  // 批量检查key是否存在
    BatchPutStart = 7,  // 批量PutStart操作
    BatchPutEnd = 8,  // 批量PutEnd操作
    BatchPutRevoke = 9  // 批量PutRevoke操作
};
// YLT_REFL_ENUM(CxlChannelRpcOp, PutStart, PutEnd, PutRevoke, ExistKey, GetReplicaList, Heartbeat, BatchExistKey, BatchPutStart, BatchPutEnd, BatchPutRevoke);

struct CxlChannelRpcRequest {
    CxlChannelRpcOp op;
    std::string key;
    std::vector<std::string> keys; // for BatchExistKey and other batch operations
    std::vector<uint64_t> slice_lengths; // for PutStart (single key)
    std::vector<std::vector<uint64_t>> batch_slice_lengths; // for BatchPutStart (each key has its own slice_lengths)
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
        throw std::runtime_error("CxlChannelRpcRequest Deserialize failed");
    }
};
YLT_REFL(CxlChannelRpcRequest, op, key, keys, slice_lengths, batch_slice_lengths, config);

struct CxlChannelRpcResponse {
    CxlChannelRpcOp op;  // 对应请求的操作类型
    CxlChannelRpcStatus status = CxlChannelRpcStatus::Fail;
    int32_t error_code = 0;
    std::string error_msg;
    bool exist = false; // for ExistKey
    std::vector<int8_t> exists; // for BatchExistKey (use int8_t instead of bool for serialization compatibility)
    std::vector<Replica::Descriptor> replica_list; // for GetReplicaList, PutStart
    std::vector<std::vector<Replica::Descriptor>> batch_replica_lists; // for BatchPutStart (batch results)
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
        // Serialize response and print in hexadecimal only for BatchPutEnd
        throw std::runtime_error("CxlChannelRpcResponse Deserialize failed");
    }

    YLT_REFL(CxlChannelRpcResponse, op, status, error_code, error_msg, exist, exists, replica_list, batch_replica_lists);
};

} // namespace mooncake