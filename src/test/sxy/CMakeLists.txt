add_executable(sxy_test_basic
	basic.cc
)
target_link_libraries(sxy_test_basic mds ${CMAKE_DL_LIBS} global-static common)

add_executable(sxy_test_ipc_send
	ipc_send.cc
)
target_link_libraries(sxy_test_ipc_send ipc global common)

add_executable(sxy_test_ipc_recv
	ipc_recv.cc
)
target_link_libraries(sxy_test_ipc_recv ipc global common)
