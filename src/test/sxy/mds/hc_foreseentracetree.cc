#include <iostream>
using std::cout;

#include <unistd.h>

#include "mds/hc_balancer/ForeseenTraceTree.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"

int main(int argc, const char * argv[])
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);
	
	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
	                       CODE_ENVIRONMENT_UTILITY, 0);
	common_init_finish(g_ceph_context);

	cout << "Test" << std::endl;

	//ForeseenTraceTree tree("/home/ceph/cyx/trace/trace_tar_compress");
	ForeseenTraceTree tree(NULL, "/home/ceph/cyx/trace/trace_compilation");
	int total = 0;
	while (!tree.finishedBuilding()) {
		cout << "Not finished. Waiting 3 seconds. Total: " << total << "s" << std::endl;
		total += 3;
		sleep(3);
	}
	//cout << tree.show_tree() << std::endl;
	
	cout << "===============================" << std::endl;
	cout << "Displaying divided rules:" << std::endl;
	cout << tree.dumptable() << std::endl;
	cout << "===============================" << std::endl;

	return 0;
}
