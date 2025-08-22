#ifndef MASTER_MQ_SERVICE_H
#define MASTER_MQ_SERVICE_H 

#include <queue>
#include <mutex>
#include <shared_mutex>
#include "types.h"

namespace mooncake { 

class MasterMQService {
   public:
    MasterMQService() = default;
    void push(const UUID& client_id, const std::string& key);
    int pop(const UUID& client_id, std::string& key);
    int bind(const UUID& client_id);
    std::optional<std::string> peak(const UUID& client_id);
    int clear(const UUID& client_id);
    bool empty(const UUID& client_id) const;

   private:
    mutable std::shared_mutex mu_;
    std::unordered_map<UUID, std::queue<std::string>, UUIDHash> degraed_mq_;
};

} // namespace mooncake


#endif