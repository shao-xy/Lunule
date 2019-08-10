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
#include "msg/Messenger.h"
#include "common/Clock.h"

#include "MDSRank.h"
#include "MDCache.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"
#include "Migrator.h"
#include "Locker.h"
#include "Server.h"

#include "MDBalancer.h"
#include "MDLog.h"
#include "MDSMap.h"
#include "Mutation.h"

#include "include/filepath.h"

#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/ESessions.h"

#include "msg/Messenger.h"

#include "messages/MClientCaps.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"
#include "messages/MGatherCaps.h"

#include "Fim.h"

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

// -- cons --
class MigratorContext : public MDSInternalContextBase {
protected:
  Migrator *mig;
  MDSRank *get_mds() override {
    return mig->mds;
  }
public:
  explicit MigratorContext(Migrator *mig_) : mig(mig_) {
    assert(mig != NULL);
  }
};

class MigratorLogContext : public MDSLogContextBase {
protected:
  Migrator *mig;
  MDSRank *get_mds() override {
    return mig->mds;
  }
public:
  explicit MigratorLogContext(Migrator *mig_) : mig(mig_) {
    assert(mig != NULL);
  }
};

class C_M_ExportDirWait : public MigratorContext {
  MDRequestRef mdr;
  int count;
public:
  C_M_ExportDirWait(Migrator *m, MDRequestRef mdr, int count)
   : MigratorContext(m), mdr(mdr), count(count) {}
  void finish(int r) override {
    if(g_conf->mds_migrator_fim == true)
      mig->fim_dispatch_export_dir(mdr, count);
    else
      mig->dispatch_export_dir(mdr, count);
  }
};

class C_MDC_ExportFreeze : public MigratorContext {
  CDir *ex;   // dir i'm exporting
  uint64_t tid;
public:
  C_MDC_ExportFreeze(Migrator *m, CDir *e, uint64_t t) :
	MigratorContext(m), ex(e), tid(t) {
          assert(ex != NULL);
        }
  void finish(int r) override {
    if (r >= 0){
      if(g_conf->mds_migrator_fim == true)
        mig->fim_export_frozen(ex, tid);
      else
        mig->export_frozen(ex, tid);
    }
  }
};

// ----  Fim ----
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
  		fim_dout(7) << __func__ << "already exporting" << fim_dendl;
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
  			while (n--) ++p;
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
  	stat.state = EXPORT_LOCKING;
  	stat.peer = dest;
  	stat.tid = mdr->reqid.tid;
  	stat.mut = mdr;

  	// return mig->mds->mdcache->dispatch_request(mdr);
  	return mig->dispatch_export_dir(mdr, 0);
}

/* fim_dispatch_export_dir
 * send discover msg to importer
 */
void Fim::fim_dispatch_export_dir(MDRequestRef& mdr, int count){
	fim_dout(7) << __func__ << *mdr << fim_dendl;

	CDir *dir = mdr->more()->export_dir;
	map<CDir *, Migrator::export_state_t>::iterator it = mig->export_state.find(dir);
	if(it == mig->export_state.end() || it->second.tid != mdr->reqid.tid){
		// export must have aborted
		mig->mds->mdcache->request_finish(mdr);
		return;
	}

	assert(it->second.state == EXPORT_LOCKING);
	mds_rank_t dest = it->second.peer;
	if(!mig->mds->is_export_target(dest)){
		fim_dout(7) << __func__ << "dest is not yet an export target" << fim_dendl;
		if(count > 3){
			fim_dout(7) << __func__ << "dest has not been added as export target after three MDSMap epochs, canceling export" << fim_dendl;
			mig->export_try_cancel(dir);
			return;
		}

		mig->mds->locker->drop_locks(mdr.get());
		mdr->drop_local_auth_pins();

		mig->mds->wait_for_mdsmap(mig->mds->mdsmap->get_epoch(), new C_M_ExportDirWait(mig, mdr, count+1));
    	return;
	}

	if(!dir->inode->get_parent_dn()){
		fim_dout(7) << __func__ << "waiting for dir to become stable before export: " << *dir << fim_dendl;
		dir->add_waiter(CDir::WAIT_CREATED, new C_M_ExportDirWait(mig, mdr, 1));
		return;
	}

	if(mdr->aborted || dir->is_frozen() || dir->is_freezing()){
		fim_dout(7) << __func__ << "wouldblock|freezing|frozen, canceling export" << fim_dendl;
		mig->export_try_cancel(dir);
		return;
	}

	// locking
	set<SimpleLock*> rdlocks;
	set<SimpleLock*> xlocks;
	set<SimpleLock*> wrlocks;
	mig->get_export_lock_set(dir, rdlocks);
	wrlocks.insert(&dir->get_inode()->filelock);
	wrlocks.insert(&dir->get_inode()->nestlock);
	if(dir->get_inode()->is_auth()){
		dir->get_inode()->filelock.set_scatter_wanted();
		dir->get_inode()->nestlock.set_scatter_wanted();
	}
	if(!mig->mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks, NULL, NULL, true)){
		if(mdr->aborted)
			mig->export_try_cancel(dir);
		return;
	}

	// discovering step
	assert(g_conf->mds_kill_export_at != 1);
	it->second.state = EXPORT_DISCOVERING;

	filepath path;
	dir->inode->make_path(path);
	MExportDirDiscover *discover = new MExportDirDiscover(dir->dirfrag(), path, mig->mds->get_nodeid(), it->second.tid);
	mig->mds->send_message_mds(discover, dest);

	assert(g_conf->mds_kill_export_at != 2);

	it->second.last_cum_auth_pins_change = ceph_clock_now();

	// start the freeze, but hold it up with an auth_pin.
	dir->freeze_tree();
	assert(dir->is_freezing_tree());
	dir->add_waiter(CDir::WAIT_FROZEN, new C_MDC_ExportFreeze(mig, dir, it->second.tid));
}

void Fim::fim_handle_export_discover(MExportDirDiscover *m){
	fim_dout(7) << __func__ << fim_dendl;

	mds_rank_t from = m->get_source_mds();
	assert(from != mig->mds->get_nodeid());

	fim_dout(7) << __func__ << "recv discover message on " << m->get_path() << fim_dendl;

	dirfrag_t df = m->get_dirfrag();

	if(!mig->mds->is_active()){
		fim_dout(7) << __func__ << "mds is not active, send NACK" << fim_dendl;
		mig->mds->send_message_mds(new MExportDirDiscoverAck(df, m->get_tid(), false), from);
		m->put();
		return;
	}

	Migrator::import_state_t *p_state;
	map<dirfrag_t, Migrator::import_state_t>::iterator it = mig->import_state.find(df);
	if(!m->started){
		assert(it == mig->import_state.end());
		m->started = true;
		p_state = &(mig->import_state[df]);
		p_state->state = IMPORT_DISCOVERING;
		p_state->peer = from;
		p_state->tid = m->get_tid();
	}
	else{
		// am i retrying after ancient path_traverse results?
		if(it == mig->import_state.end() || it->second.peer != from || it->second.tid != m->get_tid()){
			fim_dout(7) << __func__ << "dropping obsolete message" << fim_dendl;
			m->put();
			return;
		}
		assert(it->second.state != IMPORT_DISCOVERING);
		p_state = &it->second;
	}

	if(!mig->mds->mdcache->is_open()){
		fim_dout(7) << __func__ << "waiting for root" << fim_dendl;
		mig->mds->mdcache->wait_for_open(new C_MDS_RetryMessage(mig->mds, m));
		return;
	}

	assert(g_conf->mds_kill_import_at != 1);

	// do we have it?
	CInode *in = mig->cache->get_inode(m->get_dirfrag().ino);
	if(!in){
		// must discover it
		filepath fpath(m->get_path());
		vector<CDentry*> trace;
		MDRequestRef null_ref;
		int r = mig->cache->path_traverse(null_ref, m, NULL, fpath, &trace, NULL, MDS_TRAVERSE_DISCOVER);
		if(r > 0) return;
		if(r < 0){
			fim_dout(7) << __func__ << "failed to discover or not dir" << m->get_path() << fim_dendl;
			ceph_abort(); // this shouldn't happen if the auth pins its path properly!!!!
		}

		ceph_abort(); // this shouldn't happen; the get_inode above would have succeeded.
	}

	// yay
	fim_dout(7) << __func__ << "have " << df << " inode " << *in << fim_dendl;

	p_state->state = IMPORT_DISCOVERED;

	// pin inode in the cache for now
	assert(in->is_dir());
	in->get(CInode::PIN_IMPORTING);

	// reply
	fim_dout(7) << __func__ << "send export_discover_ack on " << *in << fim_dendl;
	mig->mds->send_message_mds(new MExportDirDiscoverAck(df, m->get_tid()), p_state->peer);
	m->put();
	assert(g_conf->mds_kill_import_at != 2); 
}

void Fim::fim_handle_export_discover_ack(MExportDirDiscoverAck *m){
	fim_dout(7) << __func__ << fim_dendl;
	CDir *dir = mig->cache->get_dirfrag(m->get_dirfrag());
	mds_rank_t dest(m->get_source().num());
	utime_t now = ceph_clock_now();
	assert(dir);

	fim_dout(7) << __func__ << "recv MExportDirDiscoverAck from " << m->get_source() << " on " << *dir << fim_dendl;
	mig->mds->hit_export_target(now, dest, -1);

	map<CDir*, Migrator::export_state_t>::iterator it = mig->export_state.find(dir);
	if(it == mig->export_state.end() || it->second.tid != m->get_tid() || it->second.peer != dest){
		// must be aborted
		fim_dout(7) << __func__ << "must have aborted" << fim_dendl;
	}
	else{
		assert(it->second.state == EXPORT_DISCOVERING);

		if(m->is_success()){
			// release locks to avoid deadlock
			MDRequestRef mdr = static_cast<MDRequestImpl*>(it->second.mut.get());
			assert(mdr);
			mig->mds->mdcache->request_finish(mdr);
			it->second.mut.reset();
			// freeze the subtree
			it->second.state = EXPORT_FREEZING;
			dir->auth_unpin(this);
			assert(g_conf->mds_kill_export_at != 3);
		}
		else{
			fim_dout(7) << "peer failed to discover (not active?), canceling" << fim_dendl;
			mig->export_try_cancel(dir, false);
		}
	}

	m->put(); // done
}


void Fim::fim_export_frozen(CDir *dir, uint64_t tid){
	fim_dout(7) << __func__ << *dir << fim_dendl;
}
