#include "MDSMonitor.h"

void RequestCollector::collect(string req_path)
{
	map<string, int>::iterator it = coll.find(req_path);
	if (it != coll.end()) {
		it->second++;
	}
	else {
		coll.insert(std::make_pair<string, int>(std::move(req_path), std::move(1)));
	}
}

void RequestCollector::clear()
{
	coll.clear();
}

int RequestCollector::size()
{
	int sum = 0;
	for (auto it = coll.begin(); it != coll.end(); it++) {
		sum += it->second;
	}
	return sum;
}

map<string, int> RequestCollector::fetch()
{
	map<string, int> ret = coll;
	coll.clear();
	return ret;
}

void MDSMonitor::hit_req(string req_path)
{
	req_coll.collect(req_path);
}

map<string, int> MDSMonitor::get_req()
{
	return req_coll.fetch();
}
