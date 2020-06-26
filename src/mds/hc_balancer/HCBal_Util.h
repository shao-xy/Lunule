#ifndef MDS_HCBALANCER_HCBALUTIL_H
#define MDS_HCBALANCER_HCBALUTIL_H

#include <string>
using std::string;

class CDir;
class CInode;

namespace HC_Balancer {
	int level1_dirid(CDir * dir);
	int get_dir_childrennum(CInode * parent);
	string polish(const string path);
	string basename(const string path, bool remove_suffix = false);
};

#endif /* mds/hc_balancer/HCBal_Util.h */
