#ifndef _ADSL_MIG_CO_REQ_H_
#define _ADSL_MIG_CO_REQ_H_

#include <string>

#include "mds/Mutation.h"

std::string get_all_paths(MDRequestRef& mdr);
uint64_t get_req_id(MDRequestRef& mdr);

#endif /* adsl/mig_co_req.h */
