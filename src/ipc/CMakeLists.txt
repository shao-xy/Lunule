set(ipc_srcs
	IPCMessenger.cc
	IPCWorker.cc
	IPCMessagePreDecoder.cc
)
add_library(ipc STATIC
  ${ipc_srcs}
)
target_link_libraries(ipc common)
