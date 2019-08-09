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
#include "Migrator.h"

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
	fim_dout(7) << " I am Fim, Hi~" << fim_dendl;
}

Fim::~Fim(){
	fim_dout(7) << " Fim say Goodbye~" << fim_dendl;
}

/** fim_export_dir - export dir to dest mds
**/
void Fim::fim_export_dir(CDir *dir, mds_rank_t dest){
	fim_dout(7) << __func__ << "export dir " << *dir << " start." << fim_dendl;

	assert(dir->is_auth());
  	assert(dest != mig->mds->get_nodeid());

  	if(!(mig->mds->is_active() || mig->mds->is_stopping())){
  		fim_dout(7) << __func__ << "i'm not active, no exports for now" << fim_dendl;
  		return;
  	}

  	if(mig->mds->mdcache->is_readonly()){
  		fim_dout(7) << __func__ << "read-only FS, no exports for now" << fim_dendl;
  		return;
  	}

  	if(!mig->mds->mdsmap->is_active(dest)){
  		fim_dout(7) << __func__ << "dest not active, no exports for now" << fim_dendl;
  		return;
  	}

  	if(mig->mds->is_cluster_degraded()){
  		fim_dout(7) << __func__ << "cluster degraded, no exports for now" << fim_dendl;
  		return;
  	}

  	if(dir->inode->is_system()){
  		fim_dout(7) << __func__ << "i won't export system dirs (root, mdsdirs, stray, /.ceph, etc.)" << fim_dendl;
  		return;
  	}

  	CDir *parent_dir = dir->inode->get_projected_parent_dir();
  	if(parent_dir && parent_dir->inode->is_stray()){
  		if(parent_dir->get_parent_dir()->ino() != MDS_INO_MDSDIR(dest)){
  			fim_dout(7) << __func__ << "i won't export anything in stray" << fim_dendl;
  			return;
  		}
  	} else{
  		if(!mig->mds->is_stopping() && !dir->inode->is_exportable(dest)){
  			fim_dout(7) << __func__ << "dir is export pinned" << fim_dendl;
  			return;
  		}
  	}

  	if(dir->is_frozen() || dir->is_freezing()){
  		fim_dout(7) << __func__ << "can't export, freezing|frozen. wait for other exports to finish first." << fim_dendl;
  		return;
  	}

  	if(dir->state_test(CDir::STATE_EXPORTING)){
  		fim_dout(7) << __func__ << "already exporting" << dendl;
  		return;
  	}

  	if(g_conf->mds_thrash_exports){
  		// create random subtree bound (which will not be exported)
  		list<CDir*> ls;
  		for (auto p = dir->begin(); p != dir->end(); ++p){
  			auto dn = p->second;
  			CDentry::linkage_t *dnl = dn->get_linkage();
  			if(dnl->is_primary()){
  				CInode *in = dnl->get_inode();
  				if(in->is_dir())
  					in->get_nested_dirfrags(ls);
  			}
  		}

  		if(ls.size() > 0){
  			int n = rand() % ls.size();
  			auto p = ls.begin();
  			while(n--) ++p;
  			CDir *bd = *p;
  			if(!(bd->is_frozen() || bd->is_freezing())){
  				assert(bd->is_auth());
  				dir->state_set(CDir::STATE_AUXSUBTREE);
  				mig->mds->mdcache->adjust_subtree_auth(dir, mig->mds->get_nodeid());
  				fim_dout(0) << __func__ << "export_dir: create aux subtree" << *bd << " under " << *dir << fim_dendl;
  			}
  		}
  	}

  	mig->mds->hit_export_target(ceph_clock_now(), dest, -1);

  	dir->auth_pin(this);
  	dir->state_set(CDir::STATE_EXPORTING);

  	MDRequestRef mdr = mig->mds->mdcache->request_start_internal(CEPH_MDS_OP_EXPORTDIR);
  	mdr->more()->export_dir = dir;

  	assert(mig->export_state.count(dir) == 0);
  	Migrator::export_state_t& stat = mig->export_state[dir];
  	stat.state = Migrator::EXPORT_LOCKING;
  	stat.peer = dest;
  	stat.tid = mdr->reqid.tid;
  	stat.mut = mdr;

  	return mig->mds->mdcache->dispatch_request(mdr);
}