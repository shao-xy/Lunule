#ifndef _READER_LINEMANIPULATOR_H_
#define _READER_LINEMANIPULATOR_H_

#include <string>
using std::string;

class LineManipulator {
public:
	LineManipulator() {}
	virtual ~LineManipulator() {}

	virtual bool manipulate(string bufline, string & line) = 0;
};

#endif /* reader/LineManipulator.h */
