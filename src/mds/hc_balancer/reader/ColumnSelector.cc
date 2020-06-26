#include "ColumnSelector.h"

bool ColumnSelector::manipulate(string bufline, string & line)
{
	size_t pos = 0;
	for (int i = 0; i < col - 1; i++) {
		size_t last_pos = pos;
		pos = bufline.find(delimiter, pos);
		if (pos == string::npos)
			break;

		if (pos == last_pos) {
			i--;
		}
		pos++;
	}

	// Now we try to get a substring.
	// This function also works if pos == string::npos
	if (pos == string::npos)
		line = "";
	else {
		size_t nextpos = bufline.find(delimiter, pos);
		while (nextpos == pos) {
			pos = nextpos + 1;
			nextpos = bufline.find(delimiter, pos);
		}
		line = nextpos == string::npos ? bufline.substr(pos) : bufline.substr(pos, nextpos - pos);
	}
	return true;
}
