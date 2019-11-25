#include <stringstream>

#include "util.h"
#include "common/Clock.h"

std::string now2str()
{
	std::stringstream ss;
	utime_t now = ceph_clock_now();
	ss << now.sec() << '.' << now.nsec();
	return ss.str();
}
