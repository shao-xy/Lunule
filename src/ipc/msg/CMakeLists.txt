set(ipc_msgr_srcs
	IPCMessenger.cc
	IPCWorker.cc
	IPCMessagePreDecoder.cc
)
add_library(ipc_msgr STATIC
  ${ipc_msgr_srcs}
)
target_link_libraries(ipc_msgr common)
