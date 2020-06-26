#include <errno.h>
#include <cstdio>
#include <cstring>

#include "LineManipulator.h"
#include "Reader.h"

#include "common/debug.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.reader "
#undef dout
#define dout(lvl) \
  do {\
    auto subsys = ceph_subsys_mds;\
    if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_balancer, lvl)) {\
      subsys = ceph_subsys_mds_balancer;\
    }\
    dout_impl(dout_context, subsys, lvl) dout_prefix
#undef dendl
#define dendl dendl_impl; } while (0)

bool Reader::readline(string & line)
{
	//dout(1) << __func__ << " start" << dendl;
	//if (!ifs.is_open() || ifs.eof())	return false;
	if (!ifs.is_open()) {
		dout(0) << __func__ << " reopen file" << dendl;
		ifs.open("/var/log/ceph/trace_compilation", ios::in);
		if (!ifs.is_open()) {
			//dout(0) << __func__ << " really bad." << dendl;
			dout(0) << __func__ << " errno: " << errno << " (" << strerror(errno) << ") " << dendl;
			return false;
		}
	}
	if (ifs.eof())	return false;

	//dout(1) << __func__ << " file is open" << dendl;

	string bufferline;
	if (!std::getline(ifs, bufferline))	return false;

	//dout(1) << __func__ << " successfully readline: " << bufferline << dendl;

	if (!manip) {
		line = bufferline;
		return true;
	}
	else {
		return manip->manipulate(bufferline, line);
	}
}
