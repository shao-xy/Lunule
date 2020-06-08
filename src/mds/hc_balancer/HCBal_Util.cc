#include <cstdlib>

#include "HCBal_Util.h"

#include "mds/CDir.h"
#include "mds/CInode.h"

int HC_Balancer::level1_dirid(CDir * dir)
{
	if (!dir)	return -1;
	std::string dirpath;
	dir->get_inode()->make_path_string(dirpath, true);
	size_t pos = dirpath.find('/');
	if (pos == std::string::npos) {
		// no / found in path. root or stray
		// Illegal CDirs are placed onto the default mds.0
		return -1;
	}
	pos++;
	size_t endpos = dirpath.find('/', pos);
	if (endpos != std::string::npos) {
		endpos -= pos;
	}
	std::string level1_dirname = dirpath.substr(pos, endpos);

	if (level1_dirname.length() == 0) {
		return -1;
	}

	pos = level1_dirname.find_last_not_of("0123456789");
	if (pos != std::string::npos) {
		level1_dirname = level1_dirname.substr(pos + 1);
	}

	// FIXME:
	// atoi function cannot tell if the input IS 0 or could not be transformed.
	// We assume that there's no 0 in our experiments.
	return atoi(level1_dirname.c_str());
}

int HC_Balancer::get_dir_childrennum(CInode * parent)
{
	if (!parent)	return -1;

	int count = 0;
	list<CDir*> dirfrags;
	parent->get_dirfrags(dirfrags);
	for (CDir * dir : dirfrags) {
		count += (int) dir->get_num_any();
	}

	return count;
}
