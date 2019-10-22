set(ipc_srcs
	IPC.cc
)
add_subdirectory(msg)
add_library(ipc STATIC
	${ipc_srcs}
)
target_link_libraries(ipc ipc_msgr)
