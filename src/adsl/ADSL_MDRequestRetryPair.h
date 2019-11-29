#ifndef _ADSL_MDREQUESTRETRYPAIR_H_
#define _ADSL_MDREQUESTRETRYPAIR_H_

#include "include/utime.h"

struct ADSL_MDRequestRetryPair {
	utime_t start;
	utime_t pend;

	ADSL_MDRequestRetryPair(utime_t start, utime_t pend) :
		start(start), pend(pend) {}
};

#endif /* adsl/ADSL_MDRequestRetryPair.h */
