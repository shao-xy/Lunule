#include <string>
#include <sstream>
#include <fstream>

#include "common/debug.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "mds/Mantle.h"

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
	
	// Read script from file here
	std::ifstream ifs("/home/ceph/test_mantle.lua");
	std::stringstream ss;
	ss << ifs.rdbuf();
	std::string s = ss.str();
	//dout(0) << "Script content: " << s << dendl;
	boost::string_view script_s = s;
	std::map<mds_rank_t, double> emptymap;

	// Try mantle
	Mantle mantle;
	int ret = mantle.balance(script_s, 0, std::vector<std::map<std::string, double>>(), emptymap);
	if (ret < 0) {
		dout(0) << "Balance failed." << dendl;
	}

	return 0;
}
