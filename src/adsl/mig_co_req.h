#ifndef _ADSL_MIG_CO_REQ_H_
#define _ADSL_MIG_CO_REQ_H_

#include <string>

#include "ADSL_MDRequestRetryPair.h"
#include "mds/Mutation.h"
#include "mds/CDir.h"

#include "common/Mutex.h"

extern Mutex adsl_req_mutex;

std::string adsl_get_all_paths(MDRequestRef& mdr);
uint64_t adsl_get_req_id(MDRequestRef& mdr);

class CInode;
class Migrator;
int adsl_check_inode_migration(CInode * inode, Migrator * migrator);

std::string adsl_req_get_injected_string(MDRequestRef& mdr, int req_count = 0);
std::string adsl_mig_get_injected_string(string mig_path, uint64_t req_id, string mig_state, int count = -1);
#endif /* adsl/mig_co_req.h */
