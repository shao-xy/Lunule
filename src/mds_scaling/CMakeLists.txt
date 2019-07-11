set(mds_scaling_srcs
	IPC.cc
)
add_library(mds_scaling STATIC
  ${mds_scaling_srcs}
)
target_link_libraries(mds_scaling)
