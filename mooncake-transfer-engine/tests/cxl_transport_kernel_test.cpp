#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdio>
#include <cstdint>
#include <vector>
#include <cstring>
#include "transport/cxl_transport/cxl_copy.h"
#include "common.h"

#ifdef USE_CXL_CUDA
#include <cuda_runtime.h>

#define CUDA_CHECK(err) do {                                         \
    cudaError_t e = err;                                             \
    if (e != cudaSuccess) {                                          \
        printf("CUDA Error %s:%d  code=%d  %s\n",                    \
               __FILE__, __LINE__, e, cudaGetErrorString(e));        \
        exit(EXIT_FAILURE);                                          \
    }                                                                \
} while(0)

#endif

#define COPY_COUNT 4 

using namespace mooncake;

namespace mooncake {
    
DEFINE_int32(copy_count, 4, "The count of copy");
DEFINE_bool(use_batch, true, "Use batch mode");

class CXLTransportKernelTest : public ::testing::Test {
   public:
    const int64_t len_host[COPY_COUNT] = {1000, 2048, 333, 7777};
    std::vector<uint8_t> src_host[COPY_COUNT];
    std::vector<uint8_t> dst_host[COPY_COUNT];

    void*  src_ptrs[COPY_COUNT];
    void*  dst_ptrs[COPY_COUNT];
    int64_t* d_len;
    void** d_src_list;
    void** d_dst_list;
    

   protected:
    void SetUp() override {
        // static int offset = 0;
        google::InitGoogleLogging("CXLTransportTest");
        FLAGS_logtostderr = 1;

        for (int i = 0; i < COPY_COUNT; ++i) {
            src_host[i].resize(len_host[i]);
            dst_host[i].resize(len_host[i]);
            for (int64_t j = 0; j < len_host[i]; ++j)
                src_host[i][j] = static_cast<uint8_t>(j & 0xFF);
            memset(dst_host[i].data(), 0, len_host[i]);
        }

        for (int i = 0; i < COPY_COUNT; ++i) {
            CUDA_CHECK(cudaMalloc(&src_ptrs[i], len_host[i]));
            CUDA_CHECK(cudaMalloc(&dst_ptrs[i], len_host[i]));
            CUDA_CHECK(cudaMemcpy(src_ptrs[i], src_host[i].data(), len_host[i], cudaMemcpyHostToDevice));
        }

        CUDA_CHECK(cudaMalloc(&d_src_list, sizeof(void*)   * COPY_COUNT));
        CUDA_CHECK(cudaMalloc(&d_dst_list, sizeof(void*)   * COPY_COUNT));
        CUDA_CHECK(cudaMalloc(&d_len,      sizeof(int64_t) * COPY_COUNT));

        CUDA_CHECK(cudaMemcpy(d_src_list, src_ptrs, sizeof(void*)   * COPY_COUNT, cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(d_dst_list, dst_ptrs, sizeof(void*)   * COPY_COUNT, cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(d_len,      len_host, sizeof(int64_t) * COPY_COUNT, cudaMemcpyHostToDevice));
    }

    void TearDown() override {
        for (int i = 0; i < COPY_COUNT; ++i) {
            CUDA_CHECK(cudaFree(src_ptrs[i]));
            CUDA_CHECK(cudaFree(dst_ptrs[i]));
        }
        CUDA_CHECK(cudaFree(d_src_list));
        CUDA_CHECK(cudaFree(d_dst_list));
        CUDA_CHECK(cudaFree(d_len));
        google::ShutdownGoogleLogging();
    }
};

TEST_F(CXLTransportKernelTest, CUCopy) {
    launch_batch_memcpy((const void* const*)d_src_list,
                        (void* const*)d_dst_list,
                        d_len[0], COPY_COUNT, 0);

    CUDA_CHECK(cudaDeviceSynchronize());

    /* 4. 校验结果 */
    bool ok = true;
    for (int i = 0; i < COPY_COUNT; ++i) {
        CUDA_CHECK(cudaMemcpy(dst_host[i].data(), dst_ptrs[i],
                              len_host[i], cudaMemcpyDeviceToHost));
        if (memcmp(src_host[i].data(), dst_host[i].data(), len_host[i]) != 0) {
            printf("segment %d mismatch!\n", i);
            ok = false;
        }
    }
    printf("batch memcpy test %s\n", ok ? "PASSED" : "FAILED");

}

}