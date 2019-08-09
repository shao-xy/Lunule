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

#include "mdstypes.h"
#include "MDSRank.h"
#include "Fim.h"
#include "msg/Messenger.h"
#include "common/Clock.h"
#include "CInode.h"

#include <fstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds_balancer
#undef dout_prefix
#define dout_prefix *_dout << "mds.fim "
#define fim_dout(lvl) \
  do {\
    auto subsys = ceph_subsys_mds;\
    if ((dout_context)->_conf->subsys.should_gather(ceph_subsys_mds_balancer, lvl)) {\
      subsys = ceph_subsys_mds_balancer;\
    }\
    dout_impl(dout_context, subsys, lvl) dout_prefix

#define fim_dendl dendl; } while (0)

Fim::Fim(Migrator *m) : mig(m){
	fim_dout(0) << " I am Fim, Hi~" << fim_dendl;
}

Fim::~Fim(){
	fim_dout(0) << " Fim say Goodbye~" << fim_dendl;
}