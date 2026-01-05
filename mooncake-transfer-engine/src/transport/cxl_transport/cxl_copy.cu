#include <cuda_runtime.h>
#include <cstdint>
#include "transport/cxl_transport/cxl_copy.h"

#ifndef WARP_SIZE
#define WARP_SIZE 32
#endif


/* ------------------  warp 级搬运 ------------------ */
__device__ __forceinline__ void
memcpy_warp(uint32_t lane_id, 
            const void* __restrict__ src, 
            void* __restrict__ dst, 
            int64_t nbytes) {
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
    /* ---------- 处理尾部不足 8 B 的部分 ---------- */
    int64_t tail = nbytes & 7;
    if (tail && lane_id == 0) {
        const uint8_t* s8 = reinterpret_cast<const uint8_t*>(src) + nbytes - tail;
        uint8_t*       d8 = reinterpret_cast<uint8_t*>(dst)       + nbytes - tail;
        for (int64_t k = 0; k < tail; ++k) d8[k] = s8[k];
    }
}

__device__ __forceinline__ void
memcpy_warp_array(uint32_t lane_id, 
                 const void** __restrict__ src_array, 
                 void** __restrict__ dst_array, 
                 const int64_t* __restrict__ nbytes_array, 
                 int64_t n) {
    
    // 外层循环：遍历每个独立的拷贝任务
    for (int64_t task = 0; task < n; ++task) {
        const void* src = src_array[task];
        void* dst = dst_array[task];
        int64_t nbytes = nbytes_array[task];
        
        // 如果字节数为0或负数，跳过
        if (nbytes <= 0) continue;
        
        // 原有的拷贝逻辑保持不变
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
        
        /* ---------- 处理尾部不足 8 B 的部分 ---------- */
        int64_t tail = nbytes & 7;
        if (tail && lane_id == 0) {
            const uint8_t* s8 = reinterpret_cast<const uint8_t*>(src) + nbytes - tail;
            uint8_t*       d8 = reinterpret_cast<uint8_t*>(dst)       + nbytes - tail;
            for (int64_t k = 0; k < tail; ++k) d8[k] = s8[k];
        }
        
        // 可选：添加warp同步以确保每个任务独立完成
        // __syncwarp();
    }
}


/* ------------------  每个 warp 负责一个 segment ------------------ */
template <int ITEMS_PER_WARP = 1>
__global__ void batch_memcpy_kernel(const void* const* __restrict__ src_list,
                                    void* const* __restrict__ dst_list,
                                    const int64_t* __restrict__ len_list,
                                    int n_segments,
                                    bool enable_array) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t wid = tid / WARP_SIZE;
    const uint32_t lane = tid % WARP_SIZE;

    if (enable_array) {
        memcpy_warp_array(lane, src_list, dst_list, len_list, n_segments);
    } else {
        for (int i = 0; i < ITEMS_PER_WARP; ++i) {
            int seg = wid * ITEMS_PER_WARP + i;
            if (seg >= n_segments) return;

            const void* s = src_list[seg];
            void*       d = dst_list[seg];
            int64_t     n = len_list[seg];

            memcpy_warp(lane, s, d, n);
        }
    }
}

/* ==================  Host 端 Launch 接口 ================== */
void launch_batch_memcpy(const void* const* src_list,
                         void* const* dst_list,
                         const int64_t* len_list,
                         int n_segments,
                         cudaStream_t stream = 0,
                         bool enable_array = true) {
    /* 每个 warp 负责 1 个 segment，块内 128 线程 => 4 warps */
    constexpr int THREADS = 128;
    constexpr int WARPS_PER_BLOCK = THREADS / WARP_SIZE;
    constexpr int ITEMS_PER_WARP = 1;

    int blocks = (n_segments + WARPS_PER_BLOCK - 1) / WARPS_PER_BLOCK;

    batch_memcpy_kernel<ITEMS_PER_WARP><<<blocks, THREADS, 0, stream>>>(
        src_list, dst_list, len_list, n_segments, enable_array);
}


