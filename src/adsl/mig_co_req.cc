#include <sstream>
#include <vector>

#include "util.h"
#include "mig_co_req.h"

#include "mds/CInode.h"
#include "mds/Migrator.h"

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

int adsl_check_inode_migration(CInode * inode, Migrator * migrator)
{
	if (!inode || !migrator)	return -1;

	if (!inode->has_dirfrags())	return 0;
	std::list<CDir*> dirfrags;
	inode->get_dirfrags(dirfrags);

	assert(dirfrags.size() > 0); // in that inode->has_dirfrags() returns true
	int count = 0;
	for (CDir * dir : dirfrags) {
		if (migrator->is_exporting(dir))	count++;
	}
	return count;
}

#ifdef ADSLTAG_REQUEST_MANUALCNT
std::string adsl_req_get_injected_string(MDRequestRef& mdr, int req_count)
#else
std::string adsl_req_get_injected_string(MDRequestRef& mdr)
#endif
{
#ifdef ADSLTAG_REQUEST_MANUALCNT
	assert(adsl_req_mutex.is_locked_by_me());
#endif
	assert(mdr->retry == (int)mdr->retry_ts.size());
	MClientRequest * req = mdr->client_request;

	std::stringstream ss;
	ss 
#ifdef ADSLTAG_REQUEST_MANUALCNT
	   << req_count << ' '								// request count
#endif
	   << adsl_get_req_id(mdr) << ' '					// request id
	   << ceph_mds_op_name(req->get_op()) << ' '		// operation
	   << adsl_get_all_paths(mdr) << ' '				// paths
#ifdef ADSLTAG_QUEUEING_OBSERVER 
	   << req->migs_in_queue << ' '						// Queueing migration messages
	   << req->other_in_queue << ' '					// Queueing other items
#endif
	   << mdr->retry << ' '								// retry times
#if (defined ADSLTAG_QUEUEING_OBSERVER) || (defined ADSLTAG_QUEUEING_SHOWSEQ)
	   << adsl_utime2str(req->get_recv_stamp()) << ' '	// arrival
	   << adsl_utime2str(req->get_dispatch_stamp()) << ' '; // dispatch in message queue
#else
	   << adsl_utime2str(req->get_recv_stamp()) << ' ';	// arrival
#endif
	for (vector<ADSL_MDRequestRetryPair>::iterator it = mdr->retry_ts.begin();
		it != mdr->retry_ts.end(); it++) {
		ss << adsl_utime2str(it->start) << ' '
		   << adsl_utime2str(it->pend) << ' '
		   << it->mig_dirfrag_num << ' ';
	} // retry pairs
	ss << adsl_utime2str(mdr->last_dispatch) << ' ';	// last dispatch
	ss << adsl_utime2str(ceph_clock_now());				// now it ends
#ifdef ADSLTAG_QUEUEING_OBSERVER_SHOW_BLM
	ss << ' ' << req->bm_names;
#endif
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
