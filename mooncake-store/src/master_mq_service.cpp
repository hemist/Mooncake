// master_mq_service.cpp

#include "master_mq_service.h"
#include "types.h"

namespace mooncake {

bool illegal_uuid(const UUID& uuid) {
    return uuid == UUID{0, 0};
}

int MasterMQService::push(const UUID& client_id, DegradeMsg &msg) {
    if (illegal_uuid(client_id)) {
        LOG(ERROR) << "Client id is illegal";
        return -1;
    }
    std::unique_lock lock(mu_);
    // LOG(ERROR) << "MasterMQService::push instance: " << this << " >>, degraed_mq_.size: " << degraed_mq_.size();
    degraed_mq_[client_id].push(msg);
    MasterMetricManager::instance().inc_degraded_queue_key_count();
    // LOG(ERROR) << "MasterMQService::push instance: " << this << " <<, degraed_mq_.size: " << degraed_mq_.size();
    return 0;
}

int MasterMQService::pop(const UUID& client_id, DegradeMsg &msg) {
    std::unique_lock lock(mu_);
    auto it = degraed_mq_.find(client_id);
    // LOG(ERROR) << "MasterMQService::pop instance: " << this << " , degraed_mq_.size: " << degraed_mq_.size();
    if (it == degraed_mq_.end() || it->second.empty()) {
        // LOG(ERROR) << "Client id is not found";
        return -1;
    }
    msg = std::move(it->second.front());
    it->second.pop();
    MasterMetricManager::instance().dec_degraded_queue_key_count();
    return 0;
}

int MasterMQService::bind(const UUID& client_id) {
    if (illegal_uuid(client_id)) {
        LOG(ERROR) << "Client id is illegal";
        return -1;
    }
    std::unique_lock lock(mu_);
    auto it = degraed_mq_.try_emplace(client_id);
    return it.second ? 0 : -1;
}

std::optional<DegradeMsg> MasterMQService::peak(const UUID& client_id) {
    std::shared_lock lock(mu_);
    auto it = degraed_mq_.find(client_id);
    if (it == degraed_mq_.end() || it->second.empty()) {
        LOG(ERROR) << "Client id is not found";
        return std::nullopt;
    }
    return it->second.front();
}

int MasterMQService::clear(const UUID& client_id) {
    std::unique_lock lock(mu_);
    int num = degraed_mq_.erase(client_id);
    return num > 0 ? 0 : -1;
}

bool MasterMQService::empty(const UUID& client_id) const {
    std::shared_lock lock(mu_);
    auto it = degraed_mq_.find(client_id);
    return it == degraed_mq_.end() || it->second.empty();
}

} // namespace mooncake
