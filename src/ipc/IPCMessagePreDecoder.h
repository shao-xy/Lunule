#ifndef _CEPH_IPC_IPCMESSAGEPREDECODER_H_
#define _CEPH_IPC_IPCMESSAGEPREDECODER_H_

#include "msg/Message.h"

class IPCMessagePreDecoder {
public:
	IPCMessagePreDecoder() {}
	virtual ~IPCMessagePreDecoder() {}

	Message * decode(CephContext * cct, ceph_msg_header & header, ceph_msg_footer & footer, bufferlist & front, bufferlist & middle, bufferlist & data, Connection * conn);

protected:
	virtual Message * generate_typed_message(int type) = 0;
};

#endif /* ipc/IPCMessagePreDecoder.h */
