file(GLOB CXL_SOURCES "*.cpp")

if(USE_CXL_CUDA)
  file(GLOB CXL_CUDA_SOURCES "*.cu")
  list(APPEND CXL_SOURCES ${CXL_CUDA_SOURCES})

  add_library(cxl_transport OBJECT ${CXL_SOURCES} ${CXL_CUDA_SOURCES})
  # 为CUDA代码设置位置无关代码(PIC)支持
  set_property(TARGET cxl_transport PROPERTY POSITION_INDEPENDENT_CODE ON)
  set_property(TARGET cxl_transport PROPERTY CUDA_ARCHITECTURES 75 80 86 89)
else()
  add_library(cxl_transport OBJECT ${CXL_SOURCES})
endif()

