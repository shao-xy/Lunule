#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include <errno.h>
#include <cstring>
#include <cstdlib>

#include "common/debug.h"

#include "IPC.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ipc

#undef dout_prefix
#define dout_prefix *_dout << "ipc.raw "

class IPCMessageRegister {
	struct ipcmsg_t * reg;
public:
	IPCMessageRegister() : reg(NULL) {}
	~IPCMessageRegister() {
		if (reg)	free(reg);
	}
	struct ipcmsg_t * claim() {
		struct ipcmsg_t * m = reg;
		reg = 0;
		return m;
	}
	bool save(struct ipcmsg_t * m) {
		if (!m)	return false;
		struct ipcmsg_t * dup = (struct ipcmsg_t *)malloc(sizeof(struct ipcmsg_t));
		if (!dup)	return false;
		dup->mtype = m->mtype;
		dup->msize = m->msize;
		dup->msg_id = m->msg_id;
		dup->msg_seq = m->msg_seq;
		strcpy(dup->mtext, m->mtext);
		return true;
	}
};

ipc_mqid_t IPC_raw_create(int key)
{
	return msgget(key, IPC_CREAT | 0666);
}

int IPC_raw_send(ipc_mqid_t mq_id, const char * buf, size_t len, long message_type, size_t msg_size)
{
	static int msg_id = 0;

	ipcmsg_t msgbuf;
	size_t msglen = IPCMSG_BODY_LEN;
	msgbuf.mtype = message_type;
	memset(&msgbuf.mtext, 0, IPC_DEFAULT_MSG_SIZE);
	msgbuf.msg_id = msg_id++;
	int msg_seq = 0;
	size_t sent = 0;
	while (len > msg_size) {
		msgbuf.msize = msg_size + 1;
		msgbuf.msg_seq = msg_seq++;
		memcpy(&msgbuf.mtext, buf, msg_size);
		len -= msg_size;
		buf += msg_size;

		dout(20) << __func__ << " mq_id = " << mq_id << ", trunk size= " << msglen
				 << ", msg_id = " << msgbuf.msg_id << ", msg_seq = " << msgbuf.msg_seq
				 << ", send size = " << msg_size << ", left: " << len << dendl;

		int ret = msgsnd(mq_id, &msgbuf, msglen, 0);
		if (ret < 0) {
			// something bad happens
			//dout(1) << __func__ << " IPC failed for this chunk." << dendl;
			dout(1) << __func__ << " IPC failed for this chunk. (return value: " << ret << ", ERRNO: " << strerror(errno) << ")" << dendl;
			return false;
		}
		sent += msg_size;
	}

	msgbuf.msize = len;
	msgbuf.msg_seq = msg_seq++;
	memcpy(&msgbuf.mtext, buf, len);
	dout(20) << __func__ << " mq_id = " << mq_id << ", trunk size= " << msglen
			 << ", msg_id = " << msgbuf.msg_id << ", msg_seq = " << msgbuf.msg_seq 
			 << ", send size = " << len << ", no more left." << dendl;

	int ret = msgsnd(mq_id, &msgbuf, msglen, 0);
	if (ret < 0) {
		// something bad happens
		dout(1) << __func__ << " IPC failed for this chunk. (return value: " << ret << ", ERRNO: " << strerror(errno) << ")" << dendl;
		return ret;
	}
	sent += msgbuf.msize;
	
	return sent;
}

int IPC_raw_recv(ipc_mqid_t mq_id, char * buf, size_t len, long * p_message_type, size_t msg_size)
{
	dout(20) << __func__ << " mq_id " << mq_id << " len " << len << dendl;

	static IPCMessageRegister headreg;

	size_t orig_len = len;

	bool drop_same_id = false;
	size_t recved = 0;
	int cur_seq = 0;
	int cur_id = -1;

	ipcmsg_t * head = headreg.claim();
	if (head) {
		// has head
		assert(head->msg_seq == 0);
		if (p_message_type) (*p_message_type) = head->mtype;

		if (head->msize <= msg_size) {
			// only one piece situation
			if (len < head->msize) {
				dout(1) << __func__ << " Warning: less then expected received. Current message size: " << head->msize << " expected: " << len << dendl;
				memcpy(buf, head->mtext, len);
				return len;
			}
			memcpy(buf, head->mtext, head->msize);
			return head->msize;
		}

		if (len < msg_size) {
			dout(1) << __func__ << " Warning: less then expected received. Current message size: " << msg_size << " expected: " << len << dendl;
			memcpy(buf, head->mtext, len);
			drop_same_id = true;
		}
		else {
			memcpy(buf, head->mtext, msg_size);
			len -= msg_size;
			buf += msg_size;
		}
		recved += msg_size;
		cur_id = head->msg_id;
		cur_seq = 1;

		free(head);
	}

	if (len == 0) {
		return recved;
	}

	// has more or no head buffered.

	ipcmsg_t msgbuf;
	
	while (len > 0) {
		int ret = msgrcv(mq_id, &msgbuf, IPCMSG_BODY_LEN, 0, 0);
		if (ret < 0) {
			dout(1) << __func__ << " Raw msgrcv() failed." << dendl;
			return ret;
		}

		if (msgbuf.msg_seq != cur_seq++) {
			// 'cur_id' could not be -1 now:
			// if this 'if' above matches, either 'cur_id' is modified,
			// which means it's not the first time here but still got
			// a mismatch,
			// or 'msg_seq' happens to be non-zero at the first time
			//
			// if non-zero happens, 'msg_seq' mismatches with 'cur_seq',
			// which means an illegal message sequence
			// otherwise, we still need to check 'msg_id'
			// We assume that id of next message should be different
			if (msgbuf.msg_seq == 0 && msgbuf.msg_id != cur_id) {
				// next message
				headreg.save(&msgbuf);
				break;
			}
			dout(1)<< __func__ << " Drop chunk msg_id " << msgbuf.msg_id << " msg_seq " << msgbuf.msg_seq << dendl;
			// TODO : Maybe we need to buffer these messages?
			continue;
		}

		// now we get an IPC message
		// check if first piece?
		if (cur_id == -1) {
			cur_id = msgbuf.msg_id;
			if (cur_id < 0) {
				dout(1) << __func__ << " Failed to receive message with negative ID serial number." << dendl;
				return cur_id;
			}
		}
		
		if (cur_id == msgbuf.msg_id) {
			size_t cur_len = MIN(msgbuf.msize, msg_size);
			bool last_chunk_recved = msgbuf.msize <= msg_size;

			if (drop_same_id) {
				dout(1)<< __func__ << " Drop chunk msg_id " << msgbuf.msg_id << " msg_seq " << msgbuf.msg_seq << dendl;
				recved += cur_len;
				if (last_chunk_recved)	break;

				continue;
			}

			if (len < cur_len) {
				drop_same_id = true;
				memcpy(buf, msgbuf.mtext, len);
				recved += len;
			}
			else {
				dout(20) << __func__ << " Received " << (last_chunk_recved ? "last " : "") << "chunk msg_id " << cur_id << " seq " << cur_seq << dendl;
				memcpy(buf, msgbuf.mtext, cur_len);
				buf += cur_len;
				len -= cur_len;
				recved += cur_len;
			}

			if (last_chunk_recved)	break;
		}
		else {
			dout(1) << __func__ << " Drop chunk msg_id " << msgbuf.msg_id << " msg_seq " << msgbuf.msg_seq << dendl;
			// TODO : Maybe we need to buffer these messages?
			continue;
		}
	}

	if (drop_same_id) {
		dout(1) << __func__ << " More than expected received. Received " << recved << " expected " << orig_len << dendl;
	}
	else if (len > 0) {
		dout(1) << __func__ << " Less than expected received. Received " << recved << " expected " << orig_len << dendl;
	}

	return recved;
}
