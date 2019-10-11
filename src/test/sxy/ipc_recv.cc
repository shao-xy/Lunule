#include "ipc/TestIPC.h"
#include "include/utime.h"
#include "common/debug.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"

#define dout_subsys ceph_subsys_
#define dout_context g_ceph_context

int main(int argc, const char ** argv)
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_ANY,
			       CODE_ENVIRONMENT_UTILITY,
			       0);

	TestIPCMessenger msgr(21);

	utime_t t;
	t.set_from_double(300);
	t.sleep();

	return 0;
}
