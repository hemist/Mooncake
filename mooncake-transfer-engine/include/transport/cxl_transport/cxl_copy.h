#pragma once
#include <cstdint>
#include <cuda_runtime.h>

void launch_batch_memcpy(const void* const* src_list,
                         void* const* dst_list,
                         const int64_t* len_list,
                         int n_segments,
                         cudaStream_t stream = 0,
                         bool enable_array = true);