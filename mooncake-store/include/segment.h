#pragma once

#include <boost/functional/hash.hpp>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "allocation_strategy.h"
#include "allocator.h"
#include "types.h"

namespace mooncake {
/**
 * @brief Status of a mounted segment in master
 */
enum class SegmentStatus {
    UNDEFINED = 0,  // Uninitialized
    OK,             // Segment is mounted and available for allocation
    UNMOUNTING,     // Segment is under unmounting
};

/**
 * @brief Stream operator for SegmentStatus
 */
inline std::ostream& operator<<(std::ostream& os,
                                const SegmentStatus& status) noexcept {
    static const std::unordered_map<SegmentStatus, std::string_view>
        status_strings{{SegmentStatus::UNDEFINED, "UNDEFINED"},
                       {SegmentStatus::OK, "OK"},
                       {SegmentStatus::UNMOUNTING, "UNMOUNTING"}};

    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

struct MountedSegment {
    Segment segment;
    SegmentStatus status;
    std::shared_ptr<BufferAllocatorBase> buf_allocator;
};

// Forward declarations
class SegmentManager;

/**
 * @brief RAII-style access to segment mutex for thread-safe segment operations
 */
class ScopedSegmentAccess {
   public:
    /**
     * @brief Acquires a lock on the segment mutex
     * @param mutex Reference to the segment mutex
     */
    explicit ScopedSegmentAccess(SegmentManager* segment_manager,
                                 std::shared_mutex& mutex)
        : segment_manager_(segment_manager), lock_(mutex) {}

    /**
     * @brief Mount a segment
     */
    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);

    /**
     * @brief Re-mount a segment. To avoid infinite remount trying, only the
     * errors that may be solved by subsequent remount tryings are considered as
     * errors. When encounters unsolvable errors, the segment will not be mounted
     * while the return value will be OK.
     */
    ErrorCode ReMountSegment(const std::vector<Segment>& segments,
                             const UUID& client_id);

    /**
     * @brief Prepare to unmount a segment by deleting its allocator
     */
    ErrorCode PrepareUnmountSegment(const UUID& segment_id,
                                    size_t& metrics_dec_capacity);

    /**
     * @brief Deleting the segment to complete the unmounting operation
     */
    ErrorCode CommitUnmountSegment(const UUID& segment_id,
                                   const UUID& client_id,
                                   const size_t& metrics_dec_capacity);

    /**
     * @brief Get all the segments of a client
     */
    ErrorCode GetClientSegments(const UUID& client_id,
                                std::vector<Segment>& segments) const;

    /**
     * @brief Get the names of all the segments
     */
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);

    /**
     * @brief Get the segment by name. If there are multiple segments with the
     * same name, return the first one.
     */
    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);

    ErrorCode GetClientBySegmentName(const std::string& segment_name, UUID& client_id);

   private:
    SegmentManager* segment_manager_;
    std::unique_lock<std::shared_mutex> lock_;
};

/**
 * @brief RAII-style access to mounted_segments and allocators for thread-safe usage
 */
class ScopedSegmentAllocatorAccess {
   public:
    explicit ScopedSegmentAllocatorAccess(
        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>&
                            allocators_by_name,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators,
                           std::unordered_map<UUID, MountedSegment, boost::hash<UUID>>&
                            mounted_segments,
                           std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>&
                            client_segments,
                           std::shared_mutex& mutex)
        : allocators_by_name_(allocators_by_name),
          allocators_(allocators),
          mounted_segments_(mounted_segments),
          client_segments_(client_segments),
          lock_(mutex) {}

    const std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>&
    getAllocatorsByName() {
        return allocators_by_name_;
    }

    const std::vector<std::shared_ptr<BufferAllocatorBase>>& getAllocators() {
        return allocators_;
    }

    /**
     * @brief Update the replicate_config by client's segments.
     */
    ErrorCode updateReplicateConfigBySegment(ReplicateConfig& config);

   private:
    const std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>&
        allocators_by_name_;  // segment name -> allocators
    const std::vector<std::shared_ptr<BufferAllocatorBase>>& allocators_;
    std::unordered_map<UUID, MountedSegment, boost::hash<UUID>>
        mounted_segments_;  // segment_id -> mounted segment
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_;  // client_id -> segment_ids
    std::shared_lock<std::shared_mutex> lock_;
};

class SegmentManager {
   public:
    /**
     * @brief Constructor for SegmentManager
     * @param memory_allocator Type of buffer allocator to use for new segments
     */
    explicit SegmentManager(BufferAllocatorType memory_allocator = BufferAllocatorType::CACHELIB,
                            bool enable_cxl = false)
        : memory_allocator_(memory_allocator),
          enable_cxl_(enable_cxl) {}

    /**
     * @brief Get RAII-style access to segment management operations
     * @return ScopedSegmentAccess object that holds the lock
     */
    ScopedSegmentAccess getSegmentAccess() {
        return ScopedSegmentAccess(this, segment_mutex_);
    }

    /**
     * @brief Get RAII-style access to use mounted_segments and allocators
     * @return ScopedAllocatorAccess object that holds the lock
     */
    ScopedSegmentAllocatorAccess getAllocatorAccess() {
        return ScopedSegmentAllocatorAccess(
            allocators_by_name_, 
            allocators_, 
            mounted_segments_, 
            client_segments_, 
            segment_mutex_
        );
    }

    void initializeCxlAllocator();

   private:
    mutable std::shared_mutex segment_mutex_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;
    const BufferAllocatorType memory_allocator_;  // Type of buffer allocator to use
    // This singleton allocator is managed by the master
    // Used for unified allocation and recycling of CXL shared memory.
    const bool enable_cxl_;
    std::shared_ptr<BufferAllocatorBase> cxl_global_allocator_;
    // Each allocator is put into both of allocators_by_name_ and allocators_.
    // These two containers only contain allocators whose segment status is OK.
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name_;  // segment name -> allocators
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators_;  // allocators
    std::unordered_map<UUID, MountedSegment, boost::hash<UUID>>
        mounted_segments_;  // segment_id -> mounted segment
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_;  // client_id -> segment_ids

    friend class ScopedSegmentAccess;
    friend class SegmentTest; // for unit tests
};

}  // namespace mooncake