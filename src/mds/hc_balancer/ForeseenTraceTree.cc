#include <fstream>
using std::ifstream;
using std::ios;
#include <sstream>
using std::stringstream;

#include <algorithm>
#include <signal.h>

#include "ForeseenTraceTree.h"
#include "HCBal_Util.h"
#include "reader/Reader.h"
#include "reader/ColumnSelector.h"

#include "mds/MDBalancer.h"
#include "mds/MDSRank.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "ForeseenTraceTree "
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
	
std::ostream& operator<<(std::ostream& os, const ForeseenTraceTree::TargetServer& server) {
	if (server.isSingle) {
		os << "TargetServer(" << server.mds << ")";
	}
	else {
		os << "TargetServer(";
		for (int mds: server.multimds) {
			os << mds << ",";
		}
		os << ")";
	}
	return os;
}

bool ForeseenTraceTree::Node::hit(string relative_path, int depth)
{
	// if relative path is null, we succeed here.
	if (relative_path.length() == 0 || depth == MAX_TREE_DEPTH) {
		pop += 1;
		return true;
	}

	if (relative_path[0] != '/')	return false;

	pop += 1;

	size_t pos = relative_path.find('/', 1);
	string name = relative_path.substr(1, pos - 1);
	string child_relpath = (pos != string::npos) ? relative_path.substr(pos) : "";

	// Check if it ends with '/'. (We think it invalid)
	if (name.length() == 0)	return false;

	map<string, Node*>::iterator it = children.find(name);
	Node * child = NULL;
	if (it == children.end()) {
		// We don't have that node
		child = new Node(this);
		children.insert(std::make_pair(name, child));
	}
	else {
		child = it->second;
	}

	return child->hit(child_relpath, depth + 1);
}

string ForeseenTraceTree::Node::show_subtree(string myname, bool withColor, int depth)
{
	stringstream ss;
	// indent
	if (depth) {
		ss << "   ";
	}
	for (int i = 0; i < depth - 1; i++) {
		ss << "|  ";
	}

	int children_size = children.size();
	string flag = children_size ? "*--+" : "*-->";
	ss << flag
	   << (withColor ? " \033[1;31m(" : " (")
	   << pop
	   << (withColor ? ")\033[0m " : ") ")
	   << myname << std::endl;

	for (map<string, Node*>::iterator it = children.begin();
		it != children.end(); it++) {
		ss << it->second->show_subtree(it->first, withColor, depth + 1);
	}
	return ss.str();
}

void * ForeseenTraceTree::BuildThread::entry()
{
	dout(0) << "ForeseenTraceTree::BuildThread start" << dendl;

	if (tree->root.children.size() == 0) {
		// Build the tree first
		Reader r(tracepath, new ColumnSelector(8));
		string s;
		int count = 0;
		while (r.readline(s)) {
			dout(30) << "Reading line: " << ++count << dendl;
			if (s.length()) {
				tree->hit(s);
			}
		}

		dout(0) << "After building tree:\n" << tree->show_tree() << dendl;
	}

	// Divide the tree now
	tree->foreseen_divide();
	dout(0) << "After dividing:\n" << tree->dumptable() << dendl;

	tree->built = true;
	dout(0) << "ForeseenTraceTree::BuildThread exit" << dendl;
	return NULL;
}

ForeseenTraceTree::ForeseenTraceTree(MDBalancer * bal, string tracepath)
	: bal(bal), built(false), building_thrd(this, tracepath),
	cluster_size(bal->mds->get_mds_map()->get_num_in_mds())
{
	building_thrd.create("ForeTreeCreate");
}

ForeseenTraceTree::~ForeseenTraceTree()
{
	if (building_thrd.is_started()) {
		building_thrd.kill(SIGKILL);
		building_thrd.join();
	}
}

void ForeseenTraceTree::foreseen_divide()
{
	dout(0) << __func__ << " start" << dendl;
	if (!bal)	return;

	if (!lp_lut.empty()) {
		lp_lut.clear();
	}

	int cluster_size = bal->mds->get_mds_map()->get_num_in_mds();
	//int cluster_size = 5;
	int average = root.get_pop() / cluster_size;
	map<int, int> * pop_alloc = new map<int, int>();
	for (int i = 0; i < cluster_size; i++) {
		pop_alloc->insert(std::make_pair<int, int>(std::move(i), std::move(average)));
	}
	pop_alloc->at(cluster_size - 1) += root.get_pop() % cluster_size;
	foreseen_divide_recursive(&root, "", pop_alloc);

	dout(0) << __func__ << " end" << dendl;
}

void ForeseenTraceTree::foreseen_divide_recursive(ForeseenTraceTree::Node * root, string curprefix, map<int, int> * pop_alloc)
{
	dout(0) << __func__ << " prefix " << curprefix << " pop_alloc " << *pop_alloc << dendl;
	size_t mdslist_len = pop_alloc->size();
	if (!root || !pop_alloc || !mdslist_len)	return;

	// The last recursive round?
	if (pop_alloc->size() == 1) {
		lp_lut.insert(std::make_pair<string, TargetServer>(std::move(curprefix), TargetServer(pop_alloc->begin()->first)));
		delete pop_alloc;
		return;
	}

	// If we have no child?
	if (root->children.size() == 0) {
		// ... and of course we have more than 1 ranks
		// We just put replicas on those who has the max load
		vector<int> servers;
		int max_load = 0;
		for (map<int, int>::iterator it = pop_alloc->begin();
			 it != pop_alloc->end();
			 it++) {
			if (it->second > max_load) {
				servers.clear();
				servers.push_back(it->first);
				max_load = it->second;
			}
			else if (it->second == max_load) {
				servers.push_back(it->first);
			}
		}
		lp_lut.insert(std::make_pair<string, TargetServer>(std::move(curprefix), TargetServer(servers)));
		delete pop_alloc;
		return;
	}

	// Now we comes to the multiple to multiple case
	// First we sort children due to their popularity.
	vector<pair<string, Node*> > pops(root->children.begin(), root->children.end());
	std::sort(pops.begin(), pops.end(), [](const pair<string, Node*>& lhs, const pair<string, Node*>& rhs) {
		// Here we need descending order.
		return lhs.second->get_pop() > rhs.second->get_pop();
	});

	// Second we sort targets due to their capacity.
	vector<pair<int, int> > targets(pop_alloc->begin(), pop_alloc->end());
	std::sort(targets.begin(), targets.end(), [](const pair<int, int>& lhs, const pair<int, int>& rhs) {
		return lhs.second > rhs.second;
	});

	vector<pair<string, Node*> >::iterator pit = pops.begin();
	vector<pair<int, int> >::iterator tit = targets.begin();
	assert(pit != pops.end()); // since size > 0

	// child info
	string child_name = pit->first;
	Node * child = pit->second;
	int child_pop = child->get_pop();

	// current mds info
	int curmds_rank = tit->first;
	int curmds_capacity = tit->second;

	map<int, int> * childpop_alloc = new map<int, int>();

	while (pit != pops.end() && tit != targets.end()) {
		if (child_pop == curmds_capacity) {
			// too lucky
			childpop_alloc->insert(std::make_pair<int, int>(std::move(curmds_rank), std::move(curmds_capacity)));
			foreseen_divide_recursive(child, curprefix + "/" + child_name, childpop_alloc);
			childpop_alloc = new map<int, int>();
			pit++;
			targets.erase(tit);
			tit = targets.begin();

			// update child info
			if (pit != pops.end()) {
				child_name = pit->first;
				child = pit->second;
				child_pop = child->get_pop();
			}

			// update current mds info
			if (tit != targets.end()) {
				curmds_rank = tit->first;
				curmds_capacity = tit->second;
			}
		}
		else if (child_pop > curmds_capacity) {
			childpop_alloc->insert(std::make_pair<int, int>(std::move(curmds_rank), std::move(curmds_capacity)));
			child_pop -= curmds_capacity;
			targets.erase(tit);
			tit = targets.begin();
			// update current mds info only
			if (tit != targets.end()) {
				curmds_rank = tit->first;
				curmds_capacity = tit->second;
			}
		}
		else {
			// Do we have an exactly same capacity?
			vector<pair<int, int> >::iterator it = std::find_if(tit, targets.end(), [child_pop](const pair<int, int>& element) -> bool { return element.second == child_pop; });
			if (it != targets.end()) {
				// lucky?
				childpop_alloc->insert(make_pair<int, int>(std::move(it->first), std::move(it->second)));
				foreseen_divide_recursive(child, curprefix + "/" + child_name, childpop_alloc);
				childpop_alloc = new map<int, int>();
				pit++;
				// update child info
				if (pit != pops.end()) {
					child_name = pit->first;
					child = pit->second;
					child_pop = child->get_pop();
				}
				// remove that mds from list
				pops.erase(pops.begin());
				tit = targets.begin();
				continue;
			}
			// not lucky. We split the larget mds
			childpop_alloc->insert(std::make_pair<int, int>(std::move(curmds_rank), std::move(child_pop)));
			foreseen_divide_recursive(child, curprefix + "/" + child_name, childpop_alloc);
			childpop_alloc = new map<int, int>();
			// update current capacity
			curmds_capacity -= child_pop;
			it = std::find_if(tit + 1, targets.end(), [curmds_capacity](const pair<int, int>& element) { return element.second < curmds_capacity; });
			targets.insert(it, std::make_pair<int, int>(std::move(curmds_rank), std::move(curmds_capacity)));
			targets.erase(targets.begin());
			tit = targets.begin();
			
			// update child info
			pit++;
			if (pit != pops.end()) {
				child_name = pit->first;
				child = pit->second;
				child_pop = child->get_pop();
			}
		}
	}

	// unallocated: push to mds.0
	for (; pit != pops.end(); pit++) {
		if (childpop_alloc) {
			foreseen_divide_recursive(child, curprefix + "/" + child_name, childpop_alloc);
			childpop_alloc = NULL;
		}
		else {
			lp_lut.insert(std::make_pair<string, TargetServer>(curprefix + "/" + pit->first, TargetServer(0)));
		}
	}
	
	if (childpop_alloc) delete childpop_alloc;

	// Release memory before we exit.
	delete pop_alloc;
}

mds_rank_t ForeseenTraceTree::lookup(string fullpath)
{
	if (!built)		return mds_rank_t(-3);
	if (bal->mds->get_mds_map()->get_num_in_mds() != cluster_size) {
		dout(1) << __func__ << " Tree needs redividing" << dendl;
		cluster_size = bal->mds->get_mds_map()->get_num_in_mds();
		building_thrd.create("ForeTreeCreate");
		return mds_rank_t(-3);
	}

	size_t keylen_max = 0;
	TargetServer * server = NULL;

	string directory_prefix = fullpath + '/';
	bool is_shallow = false;

	dout(1) << __func__ << " append /: " << directory_prefix << dendl;
	
	for (auto it = lp_lut.begin(); it != lp_lut.end(); it++) {
		string key = it->first;
		dout(15) << __func__ << " key: " << key << dendl;

		// maybe not deep?
		if (key.find(directory_prefix) == 0) {
			is_shallow = true;
			break;
		}

		size_t pos = fullpath.find(key);
		if (pos != 0)	continue;

		size_t keylen = key.size();
		if (keylen > keylen_max) {
			keylen_max = keylen;
			server = &(it->second);
		}
	}

	if (is_shallow)		return mds_rank_t(-2);

	if (keylen_max > 0) {
		return server->get();
	}
	return mds_rank_t(-1);
}

bool ForeseenTraceTree::hit(string path)
{
	string abs_path = HC_Balancer::polish(path);
	return root.hit(abs_path);
}

string ForeseenTraceTree::show_tree(bool withColor)
{
	return root.show_subtree("/", withColor);
}

string ForeseenTraceTree::dumptable() {
	stringstream ss;
	for (map<string, TargetServer>::iterator it = lp_lut.begin();
		 it != lp_lut.end();
		 it++) {
		ss << "\"" << it->first << "\":\t\"" << it->second << "\"" << std::endl;
	}
	return ss.str();
}
