#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include <errno.h>
#include <cstring>

#include "common/debug.h"

#include "IPC.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ipc

#undef dout_prefix
#define dout_prefix *_dout << "ipc.raw "

int IPC_raw_send(ipc_mqid_t mq_id, char * buf, int len, long message_type, int msg_size)
{
	static int msg_id = 0;

	ipcmsg_t msgbuf;
	size_t msglen = IPC_DEFAULT_MSG_SIZE;
	msgbuf.mtype = message_type;
	memset(&msgbuf.mtext, 0, IPC_DEFAULT_MSG_SIZE);
	msgbuf.msg_id = msg_id++;
	int msg_seq = 0;
	while (len > msg_size) {
		msgbuf.msize = msg_size + 1;
		msgbuf.msg_seq = msg_seq++;
		memcpy(&msgbuf.mtext, buf, msg_size);
		len -= msg_size;
		buf += msg_size;

		dout(20) << __func__ << " mq_id = " << mq_id << ", trunk size= " << msglen
				 << ", msg_id = " << msg_id << ", msg_seq = " << msgbuf.msg_seq
				 << ", send size = " << msg_size << ", left: " << len << dendl;

		int ret = msgsnd(mq_id, &msgbuf, msglen, 0);
		if (ret < 0) {
			// something bad happens
			//dout(1) << __func__ << " IPC failed for this chunk." << dendl;
			dout(1) << __func__ << " IPC failed for this chunk. (return value: " << ret << ", ERRNO: " << strerror(errno) << ")" << dendl;
			return false;
		}
	}

	msgbuf.msize = len;
	msgbuf.msg_seq = msg_seq++;
	memcpy(&msgbuf.mtext, buf, len);
	dout(20) << __func__ << " IPC (mq_id = " << mq_id << ", trunk size= " << msglen
			 << ", msg_id = " << msg_id << ", msg_seq = " << msgbuf.msg_seq 
			 << "): send size = " << len << ", no more left." << dendl;

	int ret = msgsnd(mq_id, &msgbuf, msglen, 0);
	if (ret < 0) {
		// something bad happens
		dout(1) << __func__ << " IPC failed for this chunk. (return value: " << ret << ", ERRNO: " << strerror(errno) << ")" << dendl;
	}
	
	return ret;
}

int IPC_raw_recv(ipc_mqid_t mq_id, char * buf, int * plen, long message_type, int msg_size)
{
	return 0;
}
