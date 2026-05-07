#include <cuda_runtime.h>
#include <cstdint>
#include <algorithm>
#include "transport/cxl_transport/cxl_copy.h"

#ifndef WARP_SIZE
#define WARP_SIZE 32
#endif

__device__ __forceinline__ void
memcpy_warp(uint32_t lane_id, const void* __restrict__ src, void* __restrict__ dst, int64_t nbytes) {
    const uint64_t* s = reinterpret_cast<const uint64_t*>(src);
    uint64_t*       d = reinterpret_cast<uint64_t*>(dst);
    const int64_t n64 = nbytes / sizeof(uint64_t);

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

__global__ void batch_memcpy_kernel_small(
    const void* const* __restrict__ src_list,
    void* const* __restrict__ dst_list,
    const int64_t n,
    int n_segments) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t wid = tid / WARP_SIZE;
    const uint32_t lane = tid % WARP_SIZE;

    constexpr int ITEMS_PER_WARP = 2;
    for (int i = 0; i < ITEMS_PER_WARP; ++i) {
        int seg = wid * ITEMS_PER_WARP + i;
        if (seg >= n_segments) return;

        const void* s = src_list[seg];
        void*       d = dst_list[seg];

        memcpy_warp(lane, s, d, n);
    }
}

__global__ void batch_memcpy_kernel_large(
    const void* const* __restrict__ src_list,
    void* const* __restrict__ dst_list,
    const int64_t n,
    int n_segments) {
    const int block_idx = blockIdx.x;
    if (block_idx >= n_segments) return;
    
    const uint32_t tid = threadIdx.x;
    
    const uint64_t* s = reinterpret_cast<const uint64_t*>(src_list[block_idx]);
    uint64_t* d = reinterpret_cast<uint64_t*>(dst_list[block_idx]);
    
    const int64_t n64 = n / sizeof(uint64_t);
    const int stride = blockDim.x;
    
#pragma unroll 4
    for (int64_t i = tid; i < n64; i += stride) {
#ifndef USE_ROCM
        uint64_t tmp;
        asm volatile("ld.global.nc.b64 %0,[%1];" : "=l"(tmp) : "l"(s + i) : "memory");
        asm volatile("st.global.cg.b64 [%0],%1;" :: "l"(d + i), "l"(tmp) : "memory");
#else
        uint64_t tmp = __builtin_nontemporal_load(s + i);
        __builtin_nontemporal_store(tmp, d + i);
#endif
    }
    
    int64_t tail = n & 7;
    if (tail && tid < tail) {
        const uint8_t* s8 = reinterpret_cast<const uint8_t*>(src_list[block_idx]) + n - tail;
        uint8_t* d8 = reinterpret_cast<uint8_t*>(dst_list[block_idx]) + n - tail;
        d8[tid] = s8[tid];
    }
}

void launch_batch_memcpy(const void* const* src_list,
    void* const* dst_list,
    int64_t segment_size,
    int n_segments,
    cudaStream_t stream) {

    constexpr int LARGE_SEGMENT_THRESHOLD = 256 * 1024; // 256KB
    
    if (segment_size <= LARGE_SEGMENT_THRESHOLD) {
        // Small segments: use warp-based kernel
        constexpr int THREADS = 256;
        constexpr int WARPS_PER_BLOCK = THREADS / WARP_SIZE;
        constexpr int ITEMS_PER_WARP = 2;
        
        int blocks = (n_segments + WARPS_PER_BLOCK * ITEMS_PER_WARP - 1) / (WARPS_PER_BLOCK * ITEMS_PER_WARP);
        blocks = std::max(blocks, 1);
        
        batch_memcpy_kernel_small<<<blocks, THREADS, 0, stream>>>(
            src_list, dst_list, segment_size, n_segments);
    } else {
        // Large segments: use block-cooperative kernel (1 block per segment)
        // Increase thread count for better parallelism on large segments
        int threads = 1024;
        if (segment_size < 1024 * 1024) {
            threads = 512;  // 512 threads for 256KB-1MB
        }
        
        int blocks = n_segments;
        
        batch_memcpy_kernel_large<<<blocks, threads, 0, stream>>>(
            src_list, dst_list, segment_size, n_segments);
    }
}

