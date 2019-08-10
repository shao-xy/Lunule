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
	Fim(Migrator *m);
	~Fim();

	friend class C_M_ExportDirWait;
	friend class C_MDC_ExportFreeze;
	// friend class C_MDS_ExportFinishLogged;
	// friend class C_M_ExportGo;
	// friend class C_M_ExportSessionsFlushed;
	friend class MigratorContext;
	friend class MigratorLogContext;

	// friend class C_MDS_ImportDirLoggedStart;
	// friend class C_MDS_ImportDirLoggedFinish;
	// friend class C_M_LoggedImportCaps;

	void fim_export_dir(CDir *dir, mds_rank_t dest);
	void fim_dispatch_export_dir(MDRequestRef& mdr, int count);
	void fim_export_frozen(CDir *dir, uint64_t tid);

private:
	Migrator *mig;
};

#endif