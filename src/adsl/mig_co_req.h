#ifndef _ADSL_MIG_CO_REQ_H_
#define _ADSL_MIG_CO_REQ_H_

#include <string>

#include "ADSL_MDRequestRetryPair.h"
#include "mds/Mutation.h"

std::string adsl_get_all_paths(MDRequestRef& mdr);
uint64_t adsl_get_req_id(MDRequestRef& mdr);

std::string adsl_get_injected_string(MDRequestRef& mdr);

#endif /* adsl/mig_co_req.h */
