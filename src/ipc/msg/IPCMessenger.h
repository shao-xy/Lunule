#ifndef _CEPH_IPC_IPCMESSENGER_H_
#define _CEPH_IPC_IPCMESSENGER_H_

#include <vector>
using std::vector;

#include "ipc/ipctypes.h"

#include "include/buffer.h"

#include "common/Mutex.h"
#include "common/Thread.h"

#include "msg/Message.h"

class IPCWorker;
class IPCMessenger;

class IPCProcessor : public Thread {
private:
	IPCMessenger * msgr;

	ipc_rank_t accept(ipc_mqid_t mq_id);

public:
	explicit IPCProcessor(IPCMessenger * msgr = NULL) : msgr(msgr) {}

protected:
	void * entry() override;
};

class IPCMessenger {
public:
	explicit IPCMessenger(ipc_rank_t rank = -1);
	virtual ~IPCMessenger();

	ipc_rank_t get_nodeid() { return whoami; }

	size_t get_msg_size() { return msg_size; }
	void set_msg_size(size_t newsize) {	this->msg_size = newsize; }

	bool send_message(Message * m, ipc_rank_t target);
	bool sendto(ipc_rank_t rank, char * buffer, size_t len);
	
	IPC_entity_t * create_or_get(ipc_rank_t rank);

	virtual Connection * get_conn(ipc_rank_t rank) { return NULL; }

	void shutdown();

	static int get_listen_key(ipc_rank_t rank);
	static int get_conn_key(ipc_rank_t from, ipc_rank_t to);
protected:
	ipc_rank_t whoami;	// my rank

	vector<IPC_entity_t *> conn_list;
	vector<IPCWorker *> workers;
	IPCProcessor * proc;

	Mutex msgr_mutex;

	virtual IPCWorker * create_worker(ipc_rank_t rank) = 0;
private:
	size_t msg_size;	// current message size

	IPC_entity_t * _create_or_get(ipc_rank_t rank);
	IPC_entity_t * _get_connected_entity(ipc_rank_t rank);
	IPC_entity_t * _create_connected_entity(ipc_rank_t rank);

	bool _send_message(Message * m, ipc_rank_t target);
	bool _send_message_fragments(ipc_rank_t target, const ceph_msg_header & header, const ceph_msg_footer & footer, bufferlist &blist);
	bool _sendto(ipc_rank_t rank, char * buffer, size_t len);
};

#endif /* ipc/IPCMessenger.h */
