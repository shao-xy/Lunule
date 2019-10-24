set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CMAKE_CXX_FLAGS_DEBUG} $ENV{CXXFLAGS} -O0 -Wall -g2 -ggdb")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CMAKE_CXX_FLAGS_RELEASE} $ENV{CXXFLAGS} -O3 -Wall")

add_subdirectory(msg)

set(ipc_raw_srcs
	IPC.cc
)
add_library(ipc_raw STATIC
	${ipc_raw_srcs}
)

add_library(ipc STATIC)
target_link_libraries(ipc ipc_raw ipc_msgr)
