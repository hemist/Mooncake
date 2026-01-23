#pragma once
#include <cstdint>
#include <cuda_runtime.h>

void launch_batch_memcpy(
    const void* const* src_list, 
    void* const* dst_list, 
    int64_t segment_size, 
    int n_segments, 
    cudaStream_t stream = 0
);
