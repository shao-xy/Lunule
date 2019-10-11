#include <random>
using std::default_random_engine;
using std::uniform_int_distribution;

#include "ipc/TestIPC.h"
#include "include/utime.h"
#include "common/debug.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"

#define dout_subsys ceph_subsys_

string random_string(int size)
{
	default_random_engine e; 
    uniform_int_distribution<unsigned> u(0, 25);
	stringstream ss;
	for (int i = 0; i < size; i++) {
		ss << (char) ('a' + u(e));
	}
	return ss.str();
}

int main(int argc, const char ** argv)
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_ANY,
			       CODE_ENVIRONMENT_UTILITY,
			       0);

	TestIPCMessenger msgr(20);

	for (int size = 1024; size < 4194305; size *= 2) {
	//int size = 131072;
		string test_string = random_string(size);
		//for (int i = 0; i < BENCH_SIZE; i++) {
		//	MTest * m = new MTest(i);
		//	m->set_s(test_string);
		//	msgr.send_message(m, 20);
		//}
		MTest * m = new MTest(0);
		m->set_s(test_string);
		msgr.send_message(m, 21);
		getchar();
	}
	
	utime_t t;
	t.set_from_double(3000);
	t.sleep();

	return 0;
}
