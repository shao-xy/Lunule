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

add_executable(sxy_test_mantle test_mantle.cc)
target_link_libraries(sxy_test_mantle common global mds)

add_executable(sxy_test_mantle_manual test_mantle_manual.cc $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(sxy_test_mantle_manual common global mds)

add_executable(sxy_test_lua test_lua.cc)
target_link_libraries(sxy_test_lua liblua)
