#include <iostream>
#include <cstring>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "include/utime.h"
#include "ipc/IPC.h"

int main(int argc, const char ** argv)
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_ANY,
			       CODE_ENVIRONMENT_UTILITY,
			       0);
	
	ipc_mqid_t mq = IPC_raw_create(100);

	char recvbuf[10000] = {0};
	//const char * expected_msg = "Hello!";
	
	//IPC_raw_recv(mq, recvbuf, strlen(expected_msg));
	IPC_raw_recv(mq, recvbuf, 8);
	IPC_raw_recv(mq, recvbuf + 6, 6);

	std::cout << recvbuf << std::endl;

	utime_t t;
	t.set_from_double(10);
	t.sleep();
	
	return 0;
}
