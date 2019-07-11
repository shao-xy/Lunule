add_executable(sxy_test_basic
	basic.cc
)
target_link_libraries(sxy_test_basic mds ${CMAKE_DL_LIBS} global-static common)
