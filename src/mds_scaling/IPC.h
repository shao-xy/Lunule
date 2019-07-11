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

#define IPC_DEFAULT_MSG_TYPE 666
#define IPC_DEFAULT_MSG_SIZE 255

struct IPC_entity_t {
	mds_rank_t rank;
	int msg_id;
};

class IPCMessenger;

class IPCWorker : public Thread {
private:
	IPCMessenger * msgr;
	mds_rank_t src;
	int msg_id;
	Mutex mutex;

	friend class IPCMessenger;

	int do_recvmsg(char * buf, size_t len);
	Message * try_recvmsg();

public:
	explicit IPCWorker(IPCMessenger * msgr = NULL) : msgr(msgr), mutex("IPCWorker::mutex") {}
	explicit IPCWorker(IPCMessenger * msgr, mds_rank_t src)
		: msgr(msgr), src(src), msg_id(-1),
		mutex("IPCWorker(" + std::to_string((int)src) + ")::mutex") {}

	int get_msg_id() { return msg_id; }

	void * entry() override;
};

class IPCMessenger {
private:
	MDSRank * mdsrank;
	vector<IPC_entity_t> existing;
	vector<IPCWorker *> workers;

	size_t msg_size; // now we only support default 255

	bool create_entity(mds_rank_t mds);

	bool do_sendmsg(int msg_id, void * buffer, size_t len);
	bool do_sendmsg_mds(int msg_id, const ceph_msg_header& header,
						const ceph_msg_footer& footer, bufferlist blist);

public:
	IPCMessenger(MDSRank *mdsrank = NULL);
	~IPCMessenger();

	int get_connected_msg_id(mds_rank_t mds);

	MDSRank * get_rank() { return mdsrank; }
	mds_rank_t get_nodeid();
	// now we leave this interface, but only support size 255
	size_t get_msg_size() { return msg_size; }
	void set_msg_size(size_t newsize) {	this->msg_size = newsize; }
	bool send_message_mds(Message * m, mds_rank_t target);
};

int IPC_getkey(int from, int to);

#endif /* mds_scaling/IPC.h */
