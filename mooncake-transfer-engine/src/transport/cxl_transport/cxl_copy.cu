#include <cuda_runtime.h>
#include <cstdint>
#include "transport/cxl_transport/cxl_copy.h"

#ifndef WARP_SIZE
#define WARP_SIZE 32
#endif

__device__ __forceinline__ void
memcpy_warp(uint32_t lane_id, const void* __restrict__ src, void* __restrict__ dst, int64_t nbytes) {
    const uint64_t* s = reinterpret_cast<const uint64_t*>(src);
    uint64_t*       d = reinterpret_cast<uint64_t*>(dst);
    const int64_t n64 = nbytes / sizeof(uint64_t); // nbytes=128k, n64=16k

#pragma unroll
    for (int64_t i = lane_id; i < n64; i += WARP_SIZE) {
#ifndef USE_ROCM
        uint64_t tmp;
        asm volatile("ld.global.nc.b64 %0,[%1];" : "=l"(tmp) : "l"(s + i) : "memory");
        asm volatile("st.global.cg.b64 [%0],%1;" :: "l"(d + i), "l"(tmp) : "memory");
#else
        uint64_t tmp = __builtin_nontemporal_load(s + i);
        __builtin_nontemporal_store(tmp, d + i);
#endif
    }
    int64_t tail = nbytes & 7;
    if (tail && lane_id == 0) {
        const uint8_t* s8 = reinterpret_cast<const uint8_t*>(src) + nbytes - tail;
        uint8_t*       d8 = reinterpret_cast<uint8_t*>(dst)       + nbytes - tail;
        for (int64_t k = 0; k < tail; ++k) d8[k] = s8[k];
    }
}

template <int ITEMS_PER_WARP = 1>
__global__ void batch_memcpy_kernel(const void* const* __restrict__ src_list,
                                    void* const* __restrict__ dst_list,
                                    const int64_t n,
                                    int n_segments) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t wid = tid / WARP_SIZE;
    const uint32_t lane = tid % WARP_SIZE;

    for (int i = 0; i < ITEMS_PER_WARP; ++i) {
        int seg = wid * ITEMS_PER_WARP + i;
        if (seg >= n_segments) return;

        const void* s = src_list[seg];
        void*       d = dst_list[seg];

        memcpy_warp(lane, s, d, n);
    }
}

void launch_batch_memcpy(const void* const* src_list,
    void* const* dst_list,
    int64_t segment_size,
    int n_segments,
    cudaStream_t stream) {

    constexpr int THREADS = 128;
    constexpr int WARPS_PER_BLOCK = THREADS / WARP_SIZE; // 8 warps per block
    constexpr int ITEMS_PER_WARP = 1;

    int blocks = std::max(WARP_SIZE, 2 * (n_segments + WARPS_PER_BLOCK - 1) / WARPS_PER_BLOCK);

    batch_memcpy_kernel<ITEMS_PER_WARP><<<blocks, THREADS, 0, stream>>>(
        src_list, dst_list, segment_size, n_segments);
}


