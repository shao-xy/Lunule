#include <iostream>
#include "adsl/util.h"

using namespace std;

int main()
{
	utime_t t(1, 2);
	cout << adsl_utime2str(t) << endl;
	return 0;
}
