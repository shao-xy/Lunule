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
#include "include/types.h"

class MDSRank;
class CDir;
class CInode;
class CDentry;

class MExportDirDiscover;
class MExportDirDiscoverAck;
class MExportDirCancel;
class MExportDirPrep;
class MExportDirPrepAck;
class MExportDir;
class MExportDirAck;
class MExportDirNotify;
class MExportDirNotifyAck;
class MExportDirFinish;

class MExportCaps;
class MExportCapsAck;
class MGatherCaps;

class EImportStart;

class Migrator;

class Fim{
public:
	// export stages.  used to clean up intelligently if there's a failure.
	const static int EXPORT_CANCELLED	= 0;  // cancelled
	const static int EXPORT_CANCELLING	= 1;  // waiting for cancel notifyacks
	const static int EXPORT_LOCKING	= 2;  // acquiring locks
	const static int EXPORT_DISCOVERING	= 3;  // dest is disovering export dir
	const static int EXPORT_FREEZING	= 4;  // we're freezing the dir tree
	const static int EXPORT_PREPPING	= 5;  // sending dest spanning tree to export bounds
	const static int EXPORT_WARNING	= 6;  // warning bystanders of dir_auth_pending
	const static int EXPORT_EXPORTING	= 7;  // sent actual export, waiting for ack
	const static int EXPORT_LOGGINGFINISH	= 8;  // logging EExportFinish
	const static int EXPORT_NOTIFYING	= 9;  // waiting for notifyacks
	static const char *get_export_statename(int s) {
		switch (s) {
		case EXPORT_CANCELLING: return "cancelling";
		case EXPORT_LOCKING: return "locking";
		case EXPORT_DISCOVERING: return "discovering";
		case EXPORT_FREEZING: return "freezing";
		case EXPORT_PREPPING: return "prepping";
		case EXPORT_WARNING: return "warning";
		case EXPORT_EXPORTING: return "exporting";
		case EXPORT_LOGGINGFINISH: return "loggingfinish";
		case EXPORT_NOTIFYING: return "notifying";
		default: ceph_abort(); return 0;
		}
	}

	// -- imports --
	const static int IMPORT_DISCOVERING   = 1; // waiting for prep
	const static int IMPORT_DISCOVERED    = 2; // waiting for prep
	const static int IMPORT_PREPPING      = 3; // opening dirs on bounds
	const static int IMPORT_PREPPED       = 4; // opened bounds, waiting for import
	const static int IMPORT_LOGGINGSTART  = 5; // got import, logging EImportStart
	const static int IMPORT_ACKING        = 6; // logged EImportStart, sent ack, waiting for finish
	const static int IMPORT_FINISHING     = 7; // sent cap imports, waiting for finish
	const static int IMPORT_ABORTING      = 8; // notifying bystanders of an abort before unfreezing
	static const char *get_import_statename(int s) {
		switch (s) {
		case IMPORT_DISCOVERING: return "discovering";
		case IMPORT_DISCOVERED: return "discovered";
		case IMPORT_PREPPING: return "prepping";
		case IMPORT_PREPPED: return "prepped";
		case IMPORT_LOGGINGSTART: return "loggingstart";
		case IMPORT_ACKING: return "acking";
		case IMPORT_FINISHING: return "finishing";
		case IMPORT_ABORTING: return "aborting";
		default: ceph_abort(); return 0;
		}
	}
	// ---- cons ----
	Fim(Migrator *m);
	~Fim();

	friend class C_M_ExportDirWait;
	friend class C_MDC_ExportFreeze;
	// friend class C_MDS_ExportFinishLogged;
	// friend class C_M_ExportGo;
	friend class C_M_ExportSessionsFlushed;
	friend class MigratorContext;
	friend class MigratorLogContext;

	// friend class C_MDS_ImportDirLoggedStart;
	// friend class C_MDS_ImportDirLoggedFinish;
	// friend class C_M_LoggedImportCaps;

	void fim_export_dir(CDir *dir, mds_rank_t dest);
	void fim_dispatch_export_dir(MDRequestRef& mdr, int count);
	void fim_export_frozen(CDir *dir, uint64_t tid);

	void fim_handle_export_discover(MExportDirDiscover *m);
	void fim_handle_export_discover_ack(MExportDirDiscoverAck *m);
	void fim_handle_export_prep(MExportDirPrep *m);
	void fim_handle_export_prep_ack(MExportDirPrepAck *m);
	void fim_export_go_synced(CDir *dir, uint64_t tid);

private:
	Migrator *mig;
};

#endif