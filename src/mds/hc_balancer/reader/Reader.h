#ifndef _READER_READER_H_
#define _READER_READER_H_

#include <fstream>
using std::ifstream;

#include <string>
using std::string;

#include "LineManipulator.h"

class Reader {
	ifstream ifs;
	LineManipulator * manip;
public:
	Reader(string path, LineManipulator * manip = NULL)
		: ifs(path.c_str()), manip(manip) {}
	~Reader() { ifs.close(); }

	bool is_open() { return ifs.is_open(); }
	bool readline(string & line);
};

#endif /* reader/Reader.h */
