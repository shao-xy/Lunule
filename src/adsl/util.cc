#include <sstream>

#include "util.h"
#include "common/Clock.h"

std::string adsl_utime2str(utime_t t)
{
	std::stringstream ss;
	ss << t.sec() << '.' << t.nsec();
	return ss.str();
}

std::string adsl_now2str()
{
	return adsl_utime2str(ceph_clock_now());
}
