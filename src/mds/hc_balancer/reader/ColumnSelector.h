#include "LineManipulator.h"

class ColumnSelector : public LineManipulator {
	int col;
	const char * delimiter;
public:
	ColumnSelector(int col_ = 0, const char * delimiter_ = " ")
		: col(col_), delimiter(delimiter_) {}
	ColumnSelector(int col_, string delimiter_)
		: col(col_), delimiter(delimiter_.c_str()) {}

	bool manipulate(string bufline, string & line) override;
};
