#ifndef _CEPH_IPC_MDSIPCMESSENGER_H_
#define _CEPH_IPC_MDSIPCMESSENGER_H_

#include "IPCMessenger.h"

#include "mds/mdstypes.h"
#include "mds/MDSRank.h"

class MDSIPCMessenger : public IPCMessenger {
public:
	explicit MDSIPCMessenger(MDSRank * mdsrank = NULL);
	~MDSIPCMessenger();

	MDSRank * get_mds() { return mdsrank; }

	bool send_message_mds(Message * m, mds_rank_t target);

	static ipc_mqid_t get_conn_key(mds_rank_t from, mds_rank_t to);

private:
	MDSRank * mdsrank;

	void ms_deliver(Message * m);

	Connection * get_conn(mds_rank_t target);
};

#endif /* ipc/MDSIPCMessenger.h */
