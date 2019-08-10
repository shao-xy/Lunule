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
#include "Migrator.h"

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
class C_M_ExportDirWait;
class C_MDC_ExportFreeze;

class Fim{
public:
	Fim(Migrator *m);
	~Fim();

	void fim_export_dir(CDir *dir, mds_rank_t dest);
	void fim_dispatch_export_dir(MDRequestRef& mdr, int count);
private:
	Migrator *mig;
};

#endif