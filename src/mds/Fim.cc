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

class C_M_ExportSessionsFlushed : public MigratorContext {
  CDir *dir;
  uint64_t tid;
public:
  C_M_ExportSessionsFlushed(Migrator *m, CDir *d, uint64_t t)
   : MigratorContext(m), dir(d), tid(t) {
    assert(dir != NULL);
  }
  void finish(int r) override {
    mig->export_sessions_flushed(dir, tid);
  }
};

class C_MDS_ImportDirLoggedStart : public MigratorLogContext {
  dirfrag_t df;
  CDir *dir;
  mds_rank_t from;
public:
  map<client_t,entity_inst_t> imported_client_map;
  map<client_t,uint64_t> sseqmap;

  C_MDS_ImportDirLoggedStart(Migrator *m, CDir *d, mds_rank_t f) :
    MigratorLogContext(m), df(d->dirfrag()), dir(d), from(f) {
  }
  void finish(int r) override {
    if(g_conf->mds_migrator_fim == true)
      mig->fim_import_logged_start(df, dir, from, imported_client_map, sseqmap);
    else
      mig->import_logged_start(df, dir, from, imported_client_map, sseqmap);
  }
};

class C_MDS_ExportFinishLogged : public MigratorLogContext {
  CDir *dir;
public:
  C_MDS_ExportFinishLogged(Migrator *m, CDir *d) : MigratorLogContext(m), dir(d) {}
  void finish(int r) override {
    if(g_conf->mds_migrator_fim == true)
      mig->fim_export_logged_finish(dir);
    else
      mig->export_logged_finish(dir);
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
	fim_dout(7) << __func__ << "Flow:[1] send Discover message" << fim_dendl;
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

	fim_dout(7) << __func__ << "Flow:[2] recv Discover message on " << m->get_path() << fim_dendl;

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
	fim_dout(7) << __func__ << "Flow:[3] send DiscoverACK message" << fim_dendl;
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

	fim_dout(7) << __func__ << "Flow:[4] recv DirDiscoverAck from " << m->get_source() << " on " << *dir << fim_dendl;
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
	fim_dout(7) << __func__ << " on " << *dir << fim_dendl;
	map<CDir*,Migrator::export_state_t>::iterator it = mig->export_state.find(dir);
	if (it == mig->export_state.end() || it->second.tid != tid) {
		dout(7) << "export must have aborted" << dendl;
		return;
	}

	assert(it->second.state == EXPORT_FREEZING);
	assert(dir->is_frozen_tree_root());
	assert(dir->get_cum_auth_pins() == 0);

	CInode *diri = dir->get_inode();

	// ok, try to grab all my locks.
	set<SimpleLock*> rdlocks;
	mig->get_export_lock_set(dir, rdlocks);
	if ((diri->is_auth() && diri->is_frozen()) || !mig->mds->locker->can_rdlock_set(rdlocks) || !diri->filelock.can_wrlock(-1) || !diri->nestlock.can_wrlock(-1)) {
		fim_dout(7) << "export_dir couldn't acquire all needed locks, failing. " << *dir << fim_dendl;
		// .. unwind ..
		dir->unfreeze_tree();
		mig->cache->try_subtree_merge(dir);

		mig->mds->send_message_mds(new MExportDirCancel(dir->dirfrag(), it->second.tid), it->second.peer);
		mig->export_state.erase(it);

		dir->state_clear(CDir::STATE_EXPORTING);
		mig->cache->maybe_send_pending_resolves();
		return;
	}

	it->second.mut = new MutationImpl();
	if (diri->is_auth())
		it->second.mut->auth_pin(diri);
	mig->mds->locker->rdlock_take_set(rdlocks, it->second.mut);
	mig->mds->locker->wrlock_force(&diri->filelock, it->second.mut);
	mig->mds->locker->wrlock_force(&diri->nestlock, it->second.mut);

	// can ignore and bypass
	mig->cache->show_subtrees();

	// CDir::_freeze_tree() should have forced it into subtree.
 	assert(dir->get_dir_auth() == mds_authority_t(mig->mds->get_nodeid(), mig->mds->get_nodeid()));

 	set<client_t> export_client_set;
 	mig->check_export_size(dir, it->second, export_client_set);

 	// note the bounds
 	set<CDir*> bounds;
 	mig->cache->get_subtree_bounds(dir, bounds);

 	// generate the prepare message and log the entry
 	MExportDirPrep *prep = new MExportDirPrep(dir->dirfrag(), it->second.tid);

 	// include list of bystanders
 	for(const auto &p : dir->get_replicas()){
 		if(p.first != it->second.peer){
 			fim_dout(7) << __func__ << "bystander mds." << p.first << fim_dendl;
 			prep->add_bystander(p.first);
 		}
 	}

 	// include base dirfrag
 	mig->cache->replicate_dir(dir, it->second.peer, prep->basedir);

	/*
	* include spanning tree for all nested exports.
	* these need to be on the destination _before_ the final export so that
	* dir_auth updates on any nested exports are properly absorbed.
	* this includes inodes and dirfrags included in the subtree, but
	* only the inodes at the bounds.
	*
	* each trace is: df ('-' | ('f' dir | 'd') dentry inode (dir dentry inode)*)
	*/
	set<inodeno_t> inodes_added;
	set<dirfrag_t> dirfrags_added;
	// check bounds
	for(set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p){
		CDir *bound = *p;

		// pin it
		assert(bound->state_test(CDir::STATE_EXPORTBOUND));

		fim_dout(7) << __func__ << "export bound " << *bound << fim_dendl;
		prep->add_bound(bound->dirfrag());

		// trace to bound
		bufferlist tracebl;
		CDir *cur = bound;

		char start = '-';
		if(it->second.residual_dirs.count(bound)){
			start = 'f';
			mig->cache->replicate_dir(bound, it->second.peer, tracebl);
			fim_dout(7) << __func__ << "add " << *bound << fim_dendl;
		}

		while(1){
			// don't repeat inodes
			if(inodes_added.count(cur->inode->ino()))
				break;
			inodes_added.insert(cur->inode->ino());

			// prepend dentry + inode
			assert(cur->inode->is_auth());
			bufferlist bl;
			mig->cache->replicate_dentry(cur->inode->parent, it->second.peer, bl);
			fim_dout(7) << __func__ << "added " << *cur->inode->parent << fim_dendl;
			mig->cache->replicate_inode(cur->inode, it->second.peer, bl, mig->mds->mdsmap->get_up_features());
			fim_dout(7) << __func__ << "added " << *cur->inode << fim_dendl;
			bl.claim_append(tracebl);
			tracebl.claim(bl);

			cur = cur->get_parent_dir();

			// don't repeat dirfrag
			if(dirfrags_added.count(cur->dirfrag()) || cur == dir ){
				start = 'd';
				break;
			}
			dirfrags_added.insert(cur->dirfrag());

			// prepend dir
			mig->cache->replicate_dir(cur, it->second.peer, bl);
			fim_dout(7) << __func__ << "added " << *cur << fim_dendl;

			bl.claim_append(tracebl);
			tracebl.claim(bl);
			start = 'f';
		}
		bufferlist final_bl;
		dirfrag_t df = cur->dirfrag();
		::encode(df, final_bl);
		::encode(start, final_bl);
		final_bl.claim_append(tracebl);
		prep->add_trace(final_bl);
	}

	// send
	it->second.state = EXPORT_PREPPING;
	fim_dout(7) << __func__ << "Flow:[5] send Prepare message" << fim_dendl;
	mig->mds->send_message_mds(prep, it->second.peer);
	assert(g_conf->mds_kill_export_at != 4);

	// make sure any new instantiations of caps are flushed out
	assert(it->second.warning_ack_waiting.empty());

	MDSGatherBuilder gather(g_ceph_context);
	mig->mds->server->flush_client_sessions(export_client_set, gather);
	if (gather.has_subs()) {
		it->second.warning_ack_waiting.insert(MDS_RANK_NONE);
		gather.set_finisher(new C_M_ExportSessionsFlushed(mig, dir, it->second.tid));
		gather.activate();
	}
}

void Fim::fim_handle_export_prep(MExportDirPrep *m){
	fim_dout(7) << __func__ << "Flow:[6] recv Prepare message" << fim_dendl;
	mds_rank_t oldauth = mds_rank_t(m->get_source().num());
	assert(oldauth != mig->mds->get_nodeid());

	CDir *dir;
	CInode *diri;
	list<MDSInternalContextBase*> finished;

	// assimilate root dir
	map<dirfrag_t, Migrator::import_state_t>::iterator it = mig->import_state.find(m->get_dirfrag());
	if(!m->did_assim()){
		assert(it != mig->import_state.end());
		assert(it->second.state == IMPORT_DISCOVERED);
		assert(it->second.peer == oldauth);
		diri = mig->cache->get_inode(m->get_dirfrag().ino);

		assert(diri);
		bufferlist::iterator p = m->basedir.begin();
		dir = mig->cache->add_replica_dir(p, diri, oldauth, finished);
		fim_dout(7) << __func__ << "fim_handle_export_prep on " << *dir << " (first pass)" << fim_dendl;
	}
	else{
		if(it == mig->import_state.end() || it->second.peer != oldauth || it->second.tid != m->get_tid()){
			fim_dout(7) << __func__ << "obsolete message, dropping" << fim_dendl;
			m->put();
			return;
		}
		assert(it->second.state == IMPORT_PREPPING);
		assert(it->second.peer == oldauth);
		dir = mig->cache->get_dirfrag(m->get_dirfrag());
		assert(dir);
		fim_dout(7) << __func__ << "fim_handle_export_prep on " << *dir << " (subsequent pass)" << fim_dendl;
		diri = dir->get_inode();
	}
	assert(dir->is_auth() == false);
	// mig->cache->show_subtrees();

	if(m->get_bounds().empty()){
		fim_dout(7) << __func__ << "bounds is empty" << fim_dendl;
	}
	else{
		fim_dout(7) << __func__ << "bounds is not empty, size " << m->get_bounds().size() << fim_dendl;
	}

	// build import bound map
	map<inodeno_t, fragset_t> import_bound_fragset;
	for(list<dirfrag_t>::iterator p = m->get_bounds().begin(); p != m->get_bounds().end(); ++p){
		fim_dout(7) << __func__ << "bound " << *p << fim_dendl;
		import_bound_fragset[p->ino].insert(p->frag);
	}

	// assimilate contents?
	if(!m->did_assim()){
		fim_dout(7) << __func__ << "doing assim on " << *dir << fim_dendl;
		m->mark_assim();

		// change import state
		it->second.state = IMPORT_PREPPING;
		it->second.bound_ls = m->get_bounds();
		it->second.bystanders = m->get_bystanders();
		assert(g_conf->mds_kill_import_at != 3);

		// bystander list
		fim_dout(7) << __func__ << "bystanders are " << it->second.bystanders << fim_dendl;

		// move pin to dir
		diri->put(CInode::PIN_IMPORTING);
		dir->get(CDir::PIN_IMPORTING);
		dir->state_set(CDir::STATE_IMPORTING);

		// assimilate traces to exports
		// each trace is: df ('-' | ('f' dir | 'd') dentry inode (dir dentry inode)*)
		for(list<bufferlist>::iterator p = m->traces.begin(); p != m->traces.end(); ++p){
			bufferlist::iterator q = p->begin();
			dirfrag_t df;
			::decode(df, q);
			char start;
			::decode(start, q);
			fim_dout(7) << __func__ << "trace from " << df << " start " << start << " len " << p->length() << fim_dendl;

			CDir *cur = 0;
			if (start == 'd') {
				cur = mig->cache->get_dirfrag(df);
				assert(cur);
				fim_dout(7) << __func__ << "had " << *cur << fim_dendl;
			} 
			else if (start == 'f') {
				CInode *in = mig->cache->get_inode(df.ino);
				assert(in);
				fim_dout(7) << __func__ << "had " << *in << fim_dendl;
				cur = mig->cache->add_replica_dir(q, in, oldauth, finished);
				fim_dout(7) << __func__ << "added " << *cur << fim_dendl;
			}
			else if (start == '-') {
				// nothing
			} 
			else
				assert(0 == "unrecognized start char");

			while (!q.end()) {
				CDentry *dn = mig->cache->add_replica_dentry(q, cur, finished);
				fim_dout(7) << __func__ << "added " << *dn << fim_dendl;
				CInode *in = mig->cache->add_replica_inode(q, dn, finished);
				fim_dout(7) << __func__ << "added " << *in << fim_dendl;
				if (q.end())
					break;
				cur = mig->cache->add_replica_dir(q, in, oldauth, finished);
				fim_dout(7) << __func__ << "added " << *cur << fim_dendl;
			}
		}
		// make bound sticky
		for (map<inodeno_t,fragset_t>::iterator p = import_bound_fragset.begin(); p != import_bound_fragset.end();++p) {
			CInode *in = mig->cache->get_inode(p->first);
			assert(in);
			in->get_stickydirs();
			fim_dout(7) << __func__ << "set stickydirs on bound inode " << *in << fim_dendl;
    	}
	}
	else {
    	fim_dout(7) << __func__ << "not doing assim on " << *dir << fim_dendl;
	}

	if (!finished.empty())
    	mig->mds->queue_waiters(finished);

    bool success = true;
	if (mig->mds->is_active()) {
		// open all bounds
		set<CDir*> import_bounds;
		for (map<inodeno_t,fragset_t>::iterator p = import_bound_fragset.begin(); p != import_bound_fragset.end(); ++p) {
			CInode *in = mig->cache->get_inode(p->first);
			assert(in);

			// map fragset into a frag_t list, based on the inode fragtree
			list<frag_t> fglist;
			for (set<frag_t>::iterator q = p->second.begin(); q != p->second.end(); ++q)
				in->dirfragtree.get_leaves_under(*q, fglist);
			fim_dout(7) << __func__ << "bound inode " << p->first << " fragset " << p->second << " maps to " << fglist << fim_dendl;

			for (list<frag_t>::iterator q = fglist.begin(); q != fglist.end(); ++q) {
				CDir *bound = mig->cache->get_dirfrag(dirfrag_t(p->first, *q));
				if (!bound) {
					fim_dout(7) << __func__ << "opening bounding dirfrag " << *q << " on " << *in << fim_dendl;
					mig->cache->open_remote_dirfrag(in, *q, new C_MDS_RetryMessage(mig->mds, m));
					return;
				}

				if (!bound->state_test(CDir::STATE_IMPORTBOUND)) {
			  	fim_dout(7) << __func__ << "pinning import bound " << *bound << fim_dendl;
			  	bound->get(CDir::PIN_IMPORTBOUND);
			  	bound->state_set(CDir::STATE_IMPORTBOUND);
				} 
				else {
			  		fim_dout(7) << __func__ << "already pinned import bound " << *bound << fim_dendl;
				}
				import_bounds.insert(bound);
		  	}
		}

		fim_dout(7) << __func__ << "all ready, noting auth and freezing import region" << fim_dendl; 

		if (!mig->mds->mdcache->is_readonly() && dir->get_inode()->filelock.can_wrlock(-1) && dir->get_inode()->nestlock.can_wrlock(-1)) {
			it->second.mut = new MutationImpl();
			// force some locks.  hacky.
			mig->mds->locker->wrlock_force(&dir->inode->filelock, it->second.mut);
			mig->mds->locker->wrlock_force(&dir->inode->nestlock, it->second.mut);

			// note that i am an ambiguous auth for this subtree.
			// specify bounds, since the exporter explicitly defines the region.

			if(import_bounds.empty()){
				fim_dout(7) << __func__ << "we will adjust the import_bounds, and it is empty " << fim_dendl;
			}
			else{
				fim_dout(7) << __func__ << "we will adjust the import_bounds, and it is NOT empty "<< fim_dendl;
			}

			mig->cache->adjust_bounded_subtree_auth(dir, import_bounds, pair<int,int>(oldauth, mig->mds->get_nodeid()));
			mig->cache->verify_subtree_bounds(dir, import_bounds);
			// freeze.
			dir->_freeze_tree();
			// note new state
			it->second.state = IMPORT_PREPPED;
		} 
		else {
			fim_dout(7) << __func__ << "couldn't acquire all needed locks, failing. " << *dir << fim_dendl;
		  	success = false;
		}
	}
	else {
		fim_dout(7) << __func__ << "not active, failing. " << *dir << fim_dendl;
		success = false;
	}

	if (!success)
		mig->import_reverse_prepping(dir, it->second);

	// ok!
	fim_dout(7) << __func__ << "sending export_prep_ack on " << *dir << fim_dendl;
	fim_dout(7) << __func__ << "Flow:[7] send PrepareACK message" << fim_dendl;
	mig->mds->send_message(new MExportDirPrepAck(dir->dirfrag(), success, m->get_tid()), m->get_connection());

	assert(g_conf->mds_kill_import_at != 4);
	// done 
	m->put();
}

void Fim::fim_handle_export_prep_ack(MExportDirPrepAck *m){
	fim_dout(7) << __func__ << "Flow:[8] recv PrepareACK message" << fim_dendl;
	CDir *dir = mig->cache->get_dirfrag(m->get_dirfrag());
	mds_rank_t dest(m->get_source().num());
	utime_t now = ceph_clock_now();
	assert(dir);

	fim_dout(7) << __func__ << "on " << *dir << fim_dendl;

	mig->mds->hit_export_target(now, dest, -1);

	map<CDir*,Migrator::export_state_t>::iterator it = mig->export_state.find(dir);
	if (it == mig->export_state.end() || it->second.tid != m->get_tid() || it->second.peer != mds_rank_t(m->get_source().num())) {
		// export must have aborted.  
		fim_dout(7) << __func__ << "export must have aborted" << fim_dendl;
		m->put();
		return;
	}
	assert(it->second.state == EXPORT_PREPPING);

	if (!m->is_success()) {
		fim_dout(7) << __func__ << "peer couldn't acquire all needed locks or wasn't active, canceling" << fim_dendl;
		mig->export_try_cancel(dir, false);
		m->put();
		return;
	}

	assert (g_conf->mds_kill_export_at != 5);
	// send warnings
	set<CDir*> bounds;
	mig->cache->get_subtree_bounds(dir, bounds);

	assert(it->second.warning_ack_waiting.empty() || (it->second.warning_ack_waiting.size() == 1 && it->second.warning_ack_waiting.count(MDS_RANK_NONE) > 0));
	assert(it->second.notify_ack_waiting.empty());

	for (const auto &p : dir->get_replicas()) {
		if (p.first == it->second.peer) 
			continue;
		if (mig->mds->is_cluster_degraded() && !mig->mds->mdsmap->is_clientreplay_or_active_or_stopping(p.first))
	  		continue;  // only if active
		it->second.warning_ack_waiting.insert(p.first);
		it->second.notify_ack_waiting.insert(p.first);  // we'll eventually get a notifyack, too!
		MExportDirNotify *notify = new MExportDirNotify(dir->dirfrag(), it->second.tid, true, mds_authority_t(mig->mds->get_nodeid(),CDIR_AUTH_UNKNOWN), mds_authority_t(mig->mds->get_nodeid(),it->second.peer));

		for (set<CDir*>::iterator q = bounds.begin(); q != bounds.end(); ++q){
	  		notify->get_bounds().push_back((*q)->dirfrag());
		}
		fim_dout(7) << __func__ << "Flow:[9] send Notify message!" << fim_dendl;
		mig->mds->send_message_mds(notify, p.first);
	}

	it->second.state = EXPORT_WARNING;

	assert(g_conf->mds_kill_export_at != 6);

	// nobody to warn?
	if (it->second.warning_ack_waiting.empty()){
		fim_dout(7) << __func__ << "start export_go" << fim_dendl;
		mig->export_go(dir);  // start export.
	}

	// done.
	m->put();
}

void Fim::fim_export_go_synced(CDir *dir, uint64_t tid){
	fim_dout(7) << __func__ << fim_dendl;

	map<CDir*,Migrator::export_state_t>::iterator it = mig->export_state.find(dir);
	if (it == mig->export_state.end() || it->second.state == EXPORT_CANCELLING || it->second.tid != tid) {
		// export must have aborted.  
		fim_dout(7) << __func__ << "export must have aborted on " << dir << fim_dendl;
		return;
	}
	assert(it->second.state == EXPORT_WARNING);
	mds_rank_t dest = it->second.peer;

	fim_dout(7) << __func__ << "on dir " << *dir << " to " << dest << fim_dendl;

	mig->cache->show_subtrees();

	it->second.state = EXPORT_EXPORTING;
	assert(g_conf->mds_kill_export_at != 7);

	assert(dir->is_frozen_tree_root());
	assert(dir->get_cum_auth_pins() == 0);

	// set ambiguous auth
	mig->cache->adjust_subtree_auth(dir, mig->mds->get_nodeid(), dest);

	// take away the popularity we're sending.
	utime_t now = ceph_clock_now();
	mig->mds->balancer->subtract_export(dir, now);

	// fill export message with cache data
	MExportDir *req = new MExportDir(dir->dirfrag(), it->second.tid);
	map<client_t,entity_inst_t> exported_client_map;
	uint64_t num_exported_inodes = mig->encode_export_dir(req->export_data,
					      dir,   // recur start point
					      exported_client_map,
					      now);
	::encode(exported_client_map, req->client_map, mig->mds->mdsmap->get_up_features());

	// add bounds to message
	set<CDir*> bounds;
	mig->cache->get_subtree_bounds(dir, bounds);

	for (set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p)
		req->add_export((*p)->dirfrag());

	// send
	fim_dout(7) << __func__ << "Flow:[9] send Export message" << fim_dendl;
	mig->mds->send_message_mds(req, dest);
	assert(g_conf->mds_kill_export_at != 8);

	mig->mds->hit_export_target(now, dest, num_exported_inodes+1);

	// stats
	if (mig->mds->logger) mig->mds->logger->inc(l_mds_exported);
	if (mig->mds->logger) mig->mds->logger->inc(l_mds_exported_inodes, num_exported_inodes);

	// mig->cache->show_subtrees();
}

void Fim::fim_handle_export_dir(MExportDir *m){
	fim_dout(7) << __func__ << "Flow[10] recv Export message" << fim_dendl;
	assert (g_conf->mds_kill_import_at != 5);
	CDir *dir = mig->cache->get_dirfrag(m->dirfrag);
	assert(dir);

	mds_rank_t oldauth = mds_rank_t(m->get_source().num());
	fim_dout(7) << __func__ << "importing " << *dir << " from " << oldauth << fim_dendl;
	assert(!dir->is_auth());

	map<dirfrag_t,Migrator::import_state_t>::iterator it = mig->import_state.find(m->dirfrag);
	assert(it != mig->import_state.end());
	assert(it->second.state == IMPORT_PREPPED);
	assert(it->second.tid == m->get_tid());
	assert(it->second.peer == oldauth);

	utime_t now = ceph_clock_now();

	if (!dir->get_inode()->dirfragtree.is_leaf(dir->get_frag()))
		dir->get_inode()->dirfragtree.force_to_leaf(g_ceph_context, dir->get_frag());

	// mig->cache->show_subtrees();

	C_MDS_ImportDirLoggedStart *onlogged = new C_MDS_ImportDirLoggedStart(mig, dir, oldauth);

	// start the journal entry
	EImportStart *le = new EImportStart(mig->mds->mdlog, dir->dirfrag(), m->bounds, oldauth);
	mig->mds->mdlog->start_entry(le);

	le->metablob.add_dir_context(dir);

	// adjust auth (list us _first_)
	mig->cache->adjust_subtree_auth(dir, mig->mds->get_nodeid(), oldauth);

	// new client sessions, open these after we journal
	// include imported sessions in EImportStart
	bufferlist::iterator cmp = m->client_map.begin();
	::decode(onlogged->imported_client_map, cmp);
	assert(cmp.end());
	le->cmapv = mig->mds->server->prepare_force_open_sessions(onlogged->imported_client_map, onlogged->sseqmap);
	le->client_map.claim(m->client_map);

	bufferlist::iterator blp = m->export_data.begin();
	int num_imported_inodes = 0;
	while (!blp.end()) {
	num_imported_inodes += 
	  mig->decode_import_dir(blp,
			oldauth, 
			dir,                 // import root
			le,
			mig->mds->mdlog->get_current_segment(),
			it->second.peer_exports,
			it->second.updated_scatterlocks,
			now);
	}
	fim_dout(7) << __func__ << " " << m->bounds.size() << " imported bounds" << fim_dendl;

	// include bounds in EImportStart
	set<CDir*> import_bounds;
	for (vector<dirfrag_t>::iterator p = m->bounds.begin(); p != m->bounds.end(); ++p) {
		CDir *bd = mig->cache->get_dirfrag(*p);
		assert(bd);
		le->metablob.add_dir(bd, false);  // note that parent metadata is already in the event
		import_bounds.insert(bd);
	}
	mig->cache->verify_subtree_bounds(dir, import_bounds);

	// adjust popularity
	mig->mds->balancer->add_import(dir, now);

	fim_dout(7) << __func__ << "did " << *dir << fim_dendl;

	// note state
	it->second.state = IMPORT_LOGGINGSTART;
	assert (g_conf->mds_kill_import_at != 6);

	// log it
	mig->mds->mdlog->submit_entry(le, onlogged);
	mig->mds->mdlog->flush();

	// some stats
	if (mig->mds->logger) {
		mig->mds->logger->inc(l_mds_imported);
		mig->mds->logger->inc(l_mds_imported_inodes, num_imported_inodes);
	}

	m->put();
}

void Fim::fim_import_logged_start(dirfrag_t df, CDir *dir, mds_rank_t from, map<client_t,entity_inst_t> &imported_client_map, map<client_t,uint64_t>& sseqmap){
	
	map<dirfrag_t, Migrator::import_state_t>::iterator it = mig->import_state.find(dir->dirfrag());
	if (it == mig->import_state.end() || it->second.state != IMPORT_LOGGINGSTART) {
		fim_dout(7) << __func__ << "import " << df << " must have aborted" << fim_dendl;
		mig->mds->server->finish_force_open_sessions(imported_client_map, sseqmap);
		return;
	}

	fim_dout(7) << __func__ << "import_logged " << *dir << fim_dendl;

	// note state
	it->second.state = IMPORT_ACKING;

	assert (g_conf->mds_kill_import_at != 7);

	// force open client sessions and finish cap import
	mig->mds->server->finish_force_open_sessions(imported_client_map, sseqmap, false);
	it->second.client_map.swap(imported_client_map);

	map<inodeno_t,map<client_t,Capability::Import>> imported_caps;
	for (map<CInode*, map<client_t,Capability::Export>>::iterator p = it->second.peer_exports.begin(); p != it->second.peer_exports.end(); ++p) {
		// parameter 'peer' is NONE, delay sending cap import messages to client
		mig->finish_import_inode_caps(p->first, MDS_RANK_NONE, true, p->second, imported_caps[p->first->ino()]);
	}

	// send notify's etc.
	fim_dout(7) << __func__ << "sending ack for " << *dir << " to old auth mds." << from << fim_dendl;

	// test surviving observer of a failed migration that did not complete
	//assert(dir->replica_map.size() < 2 || mds->get_nodeid() != 0);

	MExportDirAck *ack = new MExportDirAck(dir->dirfrag(), it->second.tid);
	::encode(imported_caps, ack->imported_caps);
	fim_dout(7) << __func__ << "Flow:[11] send ExportAck message!" << fim_dendl;
	mig->mds->send_message_mds(ack, from);
	assert (g_conf->mds_kill_import_at != 8);

	// mig->cache->show_subtrees();
}

void Fim::fim_handle_export_ack(MExportDirAck *m){
	fim_dout(7) << __func__ << "Flow:[12] recv ExportAck message!" << fim_dendl;
	CDir *dir = mig->cache->get_dirfrag(m->get_dirfrag());
	mds_rank_t dest(m->get_source().num());
	utime_t now = ceph_clock_now();
	assert(dir);
	assert(dir->is_frozen_tree_root());  // i'm exporting!

	// yay!
	fim_dout(7) << "on dir " << *dir << fim_dendl;

	mig->mds->hit_export_target(now, dest, -1);

	map<CDir*,Migrator::export_state_t>::iterator it = mig->export_state.find(dir);
	assert(it != mig->export_state.end());
	assert(it->second.state == EXPORT_EXPORTING);
	assert(it->second.tid == m->get_tid());

	bufferlist::iterator bp = m->imported_caps.begin();
	::decode(it->second.peer_imported, bp);

	it->second.state = EXPORT_LOGGINGFINISH;
	assert (g_conf->mds_kill_export_at != 9);
	set<CDir*> bounds;
	mig->cache->get_subtree_bounds(dir, bounds);

	// log completion. 
	//  include export bounds, to ensure they're in the journal.
	EExport *le = new EExport(mig->mds->mdlog, dir, it->second.peer);;
	mig->mds->mdlog->start_entry(le);

	le->metablob.add_dir_context(dir, EMetaBlob::TO_ROOT);
	le->metablob.add_dir(dir, false);
	for (set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p) {
		CDir *bound = *p;
		le->get_bounds().insert(bound->dirfrag());
		le->metablob.add_dir_context(bound);
		le->metablob.add_dir(bound, false);
	}

	// list us second, them first.
	// this keeps authority().first in sync with subtree auth state in the journal.
	mig->cache->adjust_subtree_auth(dir, it->second.peer, mig->mds->get_nodeid());

	// log export completion, then finish (unfreeze, trigger finish context, etc.)
	mig->mds->mdlog->submit_entry(le, new C_MDS_ExportFinishLogged(mig, dir));
	mig->mds->mdlog->flush();
	assert (g_conf->mds_kill_export_at != 10);

	m->put();
}

void Fim::fim_export_logged_finish(CDir *dir){
	fim_dout(7) << __func__ << "start export_logged_finish." << fim_dendl;

	Migrator::export_state_t& stat = mig->export_state[dir];

	// send notifies
	set<CDir*> bounds;
	mig->cache->get_subtree_bounds(dir, bounds);

	for (set<mds_rank_t>::iterator p = stat.notify_ack_waiting.begin(); p != stat.notify_ack_waiting.end(); ++p) {
		MExportDirNotify *notify = new MExportDirNotify(dir->dirfrag(), stat.tid, true,
						    pair<int,int>(mig->mds->get_nodeid(), stat.peer),
						    pair<int,int>(stat.peer, CDIR_AUTH_UNKNOWN));

		for (set<CDir*>::iterator i = bounds.begin(); i != bounds.end(); ++i)
	  		notify->get_bounds().push_back((*i)->dirfrag());

		mig->mds->send_message_mds(notify, *p);
	}

	// wait for notifyacks
	stat.state = EXPORT_NOTIFYING;
	assert (g_conf->mds_kill_export_at != 11);

	// no notifies to wait for?
	if (stat.notify_ack_waiting.empty()) {
		fim_export_finish(dir);  // skip notify/notify_ack stage.
	} 
	else {
		// notify peer to send cap import messages to clients
		if (!mig->mds->is_cluster_degraded() || mig->mds->mdsmap->is_clientreplay_or_active_or_stopping(stat.peer)) {
		  fim_dout(7) << __func__ << "Flow:[13] send ExportFinish message" << fim_dendl;
		  mig->mds->send_message_mds(new MExportDirFinish(dir->dirfrag(), false, stat.tid), stat.peer);
		} 
		else {
	  		fim_dout(7) << __func__ << "not sending MExportDirFinish, dest has failed" << fim_dendl;
		}
	}
}

void Fim::fim_export_finish(CDir *dir){

	fim_dout(7) << __func__ << "Flow:[14] export finish on exporter side!" << fim_dendl;
	assert (g_conf->mds_kill_export_at != 12);
	map<CDir*,Migrator::export_state_t>::iterator it = mig->export_state.find(dir);
	if (it == mig->export_state.end()) {
		fim_dout(7) << __func__ << "target must have failed, not sending final commit message.  export succeeded anyway." << fim_dendl;
		return;
	}

	// send finish/commit to new auth
	if (!mig->mds->is_cluster_degraded() || mig->mds->mdsmap->is_clientreplay_or_active_or_stopping(it->second.peer)) {
		mig->mds->send_message_mds(new MExportDirFinish(dir->dirfrag(), true, it->second.tid), it->second.peer);
	} 
	else {
		fim_dout(7) << __func__ << "not sending MExportDirFinish last, dest has failed" << fim_dendl;
	}
	assert(g_conf->mds_kill_export_at != 13);

	// finish export (adjust local cache state)
	int num_dentries = 0;
	list<MDSInternalContextBase*> finished;
	mig->finish_export_dir(dir, ceph_clock_now(), it->second.peer,
		    it->second.peer_imported, finished, &num_dentries);

	assert(!dir->is_auth());
	mig->cache->adjust_subtree_auth(dir, it->second.peer);

	// unpin bounds
	set<CDir*> bounds;
	mig->cache->get_subtree_bounds(dir, bounds);
	for (set<CDir*>::iterator p = bounds.begin(); p != bounds.end(); ++p) {
		CDir *bd = *p;
		bd->put(CDir::PIN_EXPORTBOUND);
		bd->state_clear(CDir::STATE_EXPORTBOUND);
	}

	if(dir->state_test(CDir::STATE_AUXSUBTREE))
		dir->state_clear(CDir::STATE_AUXSUBTREE);

	// discard delayed expires
	mig->cache->discard_delayed_expire(dir);

	fim_dout(7) << __func__ << "export_finish unfreezing" << fim_dendl;

	// unfreeze tree, with possible subtree merge.
	//  (we do this _after_ removing EXPORTBOUND pins, to allow merges)
	dir->unfreeze_tree();
	mig->cache->try_subtree_merge(dir);
	for (auto bd : it->second.residual_dirs) {
		mig->export_queue.push_front(pair<dirfrag_t,mds_rank_t>(bd->dirfrag(), it->second.peer));
		bd->take_waiting(CDir::WAIT_ANY_MASK, finished);
		bd->unfreeze_tree();
		mig->cache->try_subtree_merge(bd);
	}

	// no more auth subtree? clear scatter dirty
	if (!dir->get_inode()->is_auth() && !dir->get_inode()->has_subtree_root_dirfrag(mig->mds->get_nodeid())) {
		dir->get_inode()->clear_scatter_dirty();
		// wake up scatter_nudge waiters
		dir->get_inode()->take_waiting(CInode::WAIT_ANY_MASK, finished);
	}

	if (!finished.empty())
		mig->mds->queue_waiters(finished);

	MutationRef mut = it->second.mut;
	// remove from exporting list, clean up state
	mig->export_state.erase(it);
	dir->state_clear(CDir::STATE_EXPORTING);

	mig->cache->show_subtrees();
	mig->audit();

	mig->cache->trim(num_dentries); // try trimming exported dentries

	// send pending import_maps?
	mig->mds->mdcache->maybe_send_pending_resolves();

	// drop locks, unpin path
	if (mut) {
		mig->mds->locker->drop_locks(mut.get());
		mut->cleanup();
	}

	mig->maybe_do_queued_export();
}

void Fim::fim_handle_export_finish(MExportDirFinish *m){
	fim_dout(7) << __func__ << "Flow:[14] recv ExportFinish message!" << fim_dendl;
	CDir *dir = cache->get_dirfrag(m->get_dirfrag());
	assert(dir);
	fim_dout(7) << "handle_export_finish on " << *dir << (m->is_last() ? " last" : "") << fim_dendl;

	map<dirfrag_t,Migrator::import_state_t>::iterator it = mig->import_state.find(m->get_dirfrag());
	assert(it != mig->import_state.end());
	assert(it->second.tid == m->get_tid());

	mig->import_finish(dir, false, m->is_last());

	m->put();
}
