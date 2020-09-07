#ifndef MDS_ADSL_PATHUTIL_H_
#define MDS_ADSL_PATHUTIL_H_

#include <string>
using std::string;
#include <vector>
using std::vector;
#include <map>
using std::map;

namespace adsl {
class WL_Matcher {
	map<string, string> patterns;
	void insert(const char * key, const char * value);
public:
	WL_Matcher();
	string match(string path);
};
extern WL_Matcher g_matcher;

map<string, int> req2workload(map<string, int> & reqs);
}; // namespace adsl

#endif /* mds/adsl/PathUtil.h */
