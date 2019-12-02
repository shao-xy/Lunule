#include <sstream>
#include <vector>

#include "util.h"
#include "mig_co_req.h"

#include "mds/CInode.h"

#include "messages/MClientRequest.h"

Mutex adsl_req_mutex("adsl_req_mutex");

std::string adsl_get_all_paths(MDRequestRef& mdr)
{
	MClientRequest * req = mdr->client_request;
	//std::vector<std::string> paths;
	std::string paths[5];
	paths[0] = "";
	paths[1] = "";
	paths[2] = "";
	paths[3] = req->get_path();
	paths[4] = req->get_path2();
	if (mdr->in[0]) {
	  mdr->in[0]->make_path_string(paths[0], true);
	}
	if (mdr->in[1]) {
	  mdr->in[1]->make_path_string(paths[1], true);
	}
	if (mdr->tracei) {
	  mdr->tracei->make_path_string(paths[2], true);
	}

	std::stringstream ss;
	ss << "paths";
	// Check if empty string and output
	//for (std::vector<std::string>::iterator it = paths.begin(); it != paths.end(); it++) {
	//	if (*it == "") {
	//		*it = "*";
	//	}
	//	ss << ' ' << *it;
	//}
	for (int i = 0; i < 5; i++) {
		if (paths[i] == "") {
			paths[i] = "*";
		}
		ss << ' ' << paths[i];
	}
	return ss.str();
}

uint64_t adsl_get_req_id(MDRequestRef& mdr)
{
	return mdr->reqid.tid;
}

std::string adsl_req_get_injected_string(MDRequestRef& mdr, int req_count)
{
	assert(adsl_req_mutex.is_locked_by_me());
    assert(mdr->retry == (int)mdr->retry_ts.size());
    MClientRequest * req = mdr->client_request;

	std::stringstream ss;
	ss << req_count << ' '								// request id
	   << ceph_mds_op_name(req->get_op()) << ' '		// operation
	   << adsl_get_all_paths(mdr) << ' '				// paths
	   << mdr->retry << ' '								// retry times
	   << adsl_utime2str(req->get_recv_stamp()) << ' ';	// arrival
	for (vector<ADSL_MDRequestRetryPair>::iterator it = mdr->retry_ts.begin();
		it != mdr->retry_ts.end(); it++) {
		ss << adsl_utime2str(it->start) << ' '
		   << adsl_utime2str(it->pend) << ' ';
	} // retry pairs
	ss << adsl_utime2str(mdr->last_dispatch) << ' ';	// last dispatch
	ss << adsl_utime2str(ceph_clock_now());				// now it ends
	return ss.str();
}

std::string adsl_mig_get_injected_string(string mig_path, uint64_t req_id, string mig_state, int count){
	std::stringstream ss;
	ss << " " << adsl_utime2str(ceph_clock_now()) << " " << mig_path << " " << req_id << " " << mig_state ;				// now it ends
	if(count>=0){
		ss << " " << count;
	}
	return ss.str();
}