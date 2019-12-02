#include <sstream>
#include <iomanip>

#include "util.h"
#include "common/Clock.h"

std::string adsl_utime2str(utime_t t)
{
	std::stringstream ss;
	ss << t.sec() << '.' << std::setw(9) << std::setfill('0') << t.nsec();
	return ss.str();
}

std::string adsl_now2str()
{
	return adsl_utime2str(ceph_clock_now());
}
