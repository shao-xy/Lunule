#ifndef MDS_HCBALANCER_FORESEENTRACETREE_H
#define MDS_HCBALANCER_FORESEENTRACETREE_H

#include <cstdlib>

#include <string>
using std::string;

#include <map>
using std::map;

#include "common/Thread.h"

#include "mds/mdstypes.h"

#define MAX_TREE_DEPTH 5

class MDBalancer;

class ForeseenTraceTree {
	struct TargetServer {
		bool isSingle;
		mds_rank_t mds;
		vector<int> multimds;
		TargetServer(int rank) : isSingle(true), mds(rank) {}
		TargetServer(vector<int> ranks) : isSingle(false), multimds(ranks) {
			if (ranks.size() == 0) {
				isSingle = true;
				mds = 0;
			}
			else if (ranks.size() == 1) {
				isSingle = true;
				mds = ranks[0];
			}
		}
		mds_rank_t get() {
			return isSingle ? mds : multimds[0];
		}
	};
	friend std::ostream& operator<<(std::ostream& os, const TargetServer& server);
	struct Node {
		Node * parent;
		map<string, Node*> children;
		int pop;
		Node(Node * parent = NULL) : parent(parent), pop(0) {}
		bool hit(string relative_path, int depth = 0);

		string show_subtree(string myname, bool withColor = false, int depth = 0);

		int get_pop() { return pop; }
	};

	class BuildThread : public Thread {
	private:
		ForeseenTraceTree * tree;
		string tracepath;
	protected:
		void *entry() override;
	public:
		BuildThread(ForeseenTraceTree * tree = NULL, string path = "") : tree(tree), tracepath(path) {}
	};

	MDBalancer * bal;
	volatile bool built;
	BuildThread building_thrd;
	unsigned cluster_size;
	Node root;

	void foreseen_divide();
	void foreseen_divide_recursive(Node * root, string curprefix, map<int, int> * pop_alloc);

public:
	ForeseenTraceTree(MDBalancer * bal, string tracepath = "");
	~ForeseenTraceTree();

	bool finishedBuilding() { return built; }

	// returns:
	// 	>=0: target
	// 	-1: not found
	// 	-2: lookup table not built yet.
	//
	// 	TODO: multiple MDS ranks
	mds_rank_t lookup(string fullpath);

private:
	bool hit(string path);
public:
	string show_tree(bool withColor = false);

private:
	// We do not need a mutex for this
	// This lookup table is available unless "built" is set to true
	map<string, TargetServer> lp_lut;
public:
	string dumptable();
};

std::ostream& operator<<(std::ostream& os, const ForeseenTraceTree::TargetServer& server);

#endif /* mds/hc_balancer/ForeseenTraceTree.h */
