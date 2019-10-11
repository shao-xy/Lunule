#ifndef _CEPH_MDS_SCALING_IPC_H_
#define _CEPH_MDS_SCALING_IPC_H_

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include <list>
using std::list;

#include "include/buffer.h"

#include "common/Mutex.h"
#include "common/Thread.h"

#include "mds/mdstypes.h"
#include "mds/MDSRank.h"

//#define IPC_DEFAULT_MSG_TYPE 666
#define IPC_DEFAULT_MSG_TYPE 1
//#define IPC_DEFAULT_MSG_SIZE 255
#define IPC_DEFAULT_MSG_SIZE 4096

#define IPC_CONNECTION_MSG_TYPE 2

struct IPC_entity_t {
	mds_rank_t rank;
	int ipc_id;
};

class IPCMessenger;

class IPCWorker : public Thread {
private:
	IPCMessenger * msgr;
	mds_rank_t src;
	int ipc_id;
	Mutex mutex;
	
	//  Deprecated: connection may change from time to time
	// Connection * conn;	// for fake "Connection"

	friend class IPCMessenger;

	int do_recvmsg(char * buf, size_t len);
	Message * try_recvmsg();

public:
	explicit IPCWorker(IPCMessenger * msgr = NULL) : msgr(msgr), mutex("IPCWorker::mutex") {}
	explicit IPCWorker(IPCMessenger * msgr, mds_rank_t src)
		: msgr(msgr), src(src), ipc_id(-1),
		mutex("IPCWorker(" + std::to_string((int)src) + ")::mutex") {}

	int get_ipc_id() { return ipc_id; }

	void * entry() override;
};

class IPCProcessor : public Thread {
private:
	IPCMessenger * msgr;

	mds_rank_t accept(int ipc_id);
	
public:
	explicit IPCProcessor(IPCMessenger * msgr = NULL) : msgr(msgr) {}

	void * entry() override;
};

class IPCMessenger {
private:
	MDSRank * mdsrank;
	vector<IPC_entity_t> existing;
	vector<IPCWorker *> workers;
	IPCProcessor * processor;
	Mutex msgr_mutex;

	size_t msg_size; // now we only support default 255

	int get_connected_ipc_id(mds_rank_t mds);
	int create_entity(mds_rank_t mds);

	bool do_sendmsg(int ipc_id, char * buffer, size_t len);
	bool do_sendmsg_mds(int ipc_id, const ceph_msg_header& header,
						const ceph_msg_footer& footer, bufferlist blist);

public:
	IPCMessenger(MDSRank *mdsrank = NULL);
	~IPCMessenger();

	MDSRank * get_rank() { return mdsrank; }
	mds_rank_t get_nodeid();
	// now we leave this interface, but only support size 255
	size_t get_msg_size() { return msg_size; }
	void set_msg_size(size_t newsize) {	this->msg_size = newsize; }
	bool send_message_mds(Message * m, mds_rank_t target);

	int ensure_entity_exist(mds_rank_t mds);

	// Call back for IPCWorker and IPCProcessor
	void ms_deliver(Message * m);

	Connection * get_conn(mds_rank_t target);
};

int IPC_getkey(mds_rank_t from, mds_rank_t to);
int IPC_get_listenkey(mds_rank_t mds);

#endif /* mds_scaling/IPC.h */
