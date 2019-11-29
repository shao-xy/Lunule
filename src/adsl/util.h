#ifndef _ADSL_UTIL_H_
#define _ADSL_UTIL_H_

#include <string>

#include "include/utime.h"

std::string adsl_utime2str(utime_t t);
std::string adsl_now2str();

#endif /* adsl/util.h */
