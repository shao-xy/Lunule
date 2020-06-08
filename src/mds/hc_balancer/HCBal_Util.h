#ifndef MDS_HCBALANCER_HCBALUTIL_H
#define MDS_HCBALANCER_HCBALUTIL_H

class CDir;
class CInode;

namespace HC_Balancer {
	int level1_dirid(CDir * dir);
	int get_dir_childrennum(CInode * parent);
};

#endif /* mds/hc_balancer/HCBal_Util.h */
