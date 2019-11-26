#include <sstream>

#include "mig_co_req.h"

#include "mds/CInode.h"

#include "messages/MClientRequest.h"

std::string get_all_paths(MDRequestRef& mdr)
{
	MClientRequest * req = mdr->client_request;
	std::string path1 = "";
	std::string path2 = "";
	std::string path3 = "";
	if (mdr->in[0]) {
	  mdr->in[0]->make_path_string(path1, true);
	}
	if (mdr->in[1]) {
	  mdr->in[1]->make_path_string(path2, true);
	}
	if (mdr->tracei) {
	  mdr->tracei->make_path_string(path3, true);
	}

	std::stringstream ss;
	ss << "paths " << path1 << " " << path2 << " " << path3 << " " << req->get_path() << " " << req->get_path2();
	return ss.str();
}
