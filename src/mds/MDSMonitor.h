// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=4 sw=2 smarttab

#ifndef ADSL_MDSMON_H_
#define ADSL_MDSMON_H_

#include <string>
using std::string;

#include <map>
using std::map;

class RequestCollector {
	map<string, int> coll;
public:
	RequestCollector() {}
	void collect(string req_path);
	void clear();
	int size();
	map<string, int> get() { return coll; }
	map<string, int> fetch();
};

class MDSMonitor {
	RequestCollector req_coll;
public: 
	MDSMonitor() {}
	virtual ~MDSMonitor() {}

	// RequestCollector
	void hit_req(string req_path);
	map<string, int> get_req();
};

#endif
