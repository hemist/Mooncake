project(MooncakeStore)

option(STORE_USE_CXL_CHANNEL "Enable cxl_shm channel integration" OFF)

if (STORE_USE_ETCD)
    set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/)
    set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/libetcd_wrapper.so)
else()
    message(STATUS "STORE_USE_ETCD=OFF, high availability of Store is disabled")
endif()

# CXL_SHM include path (optional)
if (STORE_USE_CXL_CHANNEL)
    find_path(CXL_SHM_INCLUDE_DIR
        NAMES cxl_shm/message_queue.h
        PATHS /usr/local/include /usr/include
        NO_DEFAULT_PATH
    )
    if (NOT CXL_SHM_INCLUDE_DIR)
        message(FATAL_ERROR "cxl_shm include directory not found in /usr/local/include or /usr/include")
    endif()
    message(STATUS "STORE_USE_CXL_CHANNEL=ON, using cxl_shm include: ${CXL_SHM_INCLUDE_DIR}")
endif()

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/mooncake-store/proto/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include
    ${ETCD_WRAPPER_INCLUDE}
)

if (STORE_USE_CXL_CHANNEL)
    include_directories(${CXL_SHM_INCLUDE_DIR})
    add_compile_definitions(STORE_USE_CXL_CHANNEL)
endif()

# Add subdirectories
add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(benchmarks)