#ifndef _CEPH_IPC_IPCWORKER_H_
#define _CEPH_IPC_IPCWORKER_H_

#include "ipc/ipctypes.h"
#include "IPCMessagePreDecoder.h"

#include "common/Mutex.h"
#include "common/Thread.h"

#include "msg/Message.h"

class IPCMessenger;
class IPCMessagePreDecoder;

class IPCWorker : public Thread {
	friend class IPCMessenger;
protected:
	IPCMessenger * msgr;
	IPC_entity_t src;
private:
	IPCMessagePreDecoder * pre_decoder;
	Mutex mutex;

	int _recv_raw(char * buf, size_t len);
	Message * _receive_message();
	Message * pre_decode_message(CephContext * cct, ceph_msg_header & header, ceph_msg_footer & footer, bufferlist & front, bufferlist & middle, bufferlist & data, Connection * conn);

public:
	explicit IPCWorker(IPCMessenger * msgr = NULL) : msgr(msgr), src(-1, -1), pre_decoder(get_pre_decoder()), mutex("IPCWorker::mutex") {}
	IPCWorker(IPCMessenger * msgr, ipc_rank_t from)
		: msgr(msgr), src(from, -1), pre_decoder(get_pre_decoder()), mutex("IPCWorker(" + std::to_string((int)from) + ")::mutex") {}
	virtual ~IPCWorker() {}

	int recv_raw(char * buf, size_t len);
	Message * receive_message();

	ipc_rank_t get_peer() { return src.rank; }
	ipc_mqid_t get_mqueue_id() { return src.mq_id; }
	virtual string name();

protected:
	void * entry() override;
	virtual void * handle_message(Message * m) = 0;
	virtual IPCMessagePreDecoder * get_pre_decoder() { return NULL; }
};

#endif /* ipc/IPCWorker.h */
