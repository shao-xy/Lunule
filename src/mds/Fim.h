// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Michael Sevilla <mikesevilla3@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

// Faster IPC-based Migration - FIM

#ifndef CEPH_FIM_H
#define CEPH_FIM_H

#include <boost/utility/string_view.hpp>

#include <lua.hpp>
#include <vector>
#include <map>
#include <string>

#include "mdstypes.h"

class Fim{
public:
	Fim(){};
	~Fim(){};

}

#endif