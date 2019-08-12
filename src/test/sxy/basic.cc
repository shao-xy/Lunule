// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

#include <iostream>
#include <string>
using namespace std;

#include "include/ceph_features.h"
#include "include/compat.h"

#include "common/config.h"
#include "common/strtol.h"

#include "mon/MonMap.h"
#include "mds/MDSDaemon.h"

#include "msg/Messenger.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/pick_address.h"

#include "global/global_init.h"
#include "global/signal_handler.h"
#include "global/pidfile.h"

#include "mon/MonClient.h"

#include "auth/KeyRing.h"

#include "perfglue/heap_profiler.h"

#include "include/assert.h"

#ifdef BUILDING_FOR_EMBEDDED
extern "C" int cephd_mds(int argc, const char **argv)
#else
int main(int argc, const char **argv)
#endif
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args,
			 CEPH_ENTITY_TYPE_MDS, CODE_ENVIRONMENT_DAEMON,
			 0, "mds_data");
  common_init_finish(g_ceph_context);

  // global configuration get_val
  std::string public_msgr_type = g_conf->ms_public_type.empty() ? g_conf->get_val<std::string>("ms_type") : g_conf->ms_public_type;
  std::cout << public_msgr_type << std::endl;

  // bufferlist::operator=()
  bufferlist a = bufferlist::static_from_cstring("hello");
  bufferlist b = a;
  b.append("world");
  std::cout << a.buffers().begin()->c_str() << std::endl;
  std::cout << b.buffers().begin()->c_str() << std::endl;
  std::cout << a.buffers().size() << std::endl;
  std::cout << b.buffers().size() << std::endl;

  b.rebuild();
  std::cout << b.buffers().size() << std::endl;
  std::cout << b.buffers().begin()->c_str() << std::endl;
  std::cout << a.buffers().size() << std::endl;
  std::cout << a.buffers().begin()->c_str() << std::endl;
  return 0;
}

