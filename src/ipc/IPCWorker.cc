#include <cstdlib>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include "IPCWorker.h"
#include "IPCMessenger.h"

#include "msg/Message.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ipc

#undef dout_prefix
#define dout_prefix *_dout << "ipc." << msgr->get_nodeid() << ".worker(<=>" << src.rank << ") "

void * IPCWorker::entry()
{
	int ipckey = IPCMessenger::get_conn_key(src.rank, msgr->get_nodeid());

	ipc_mqid_t conn_mqid = msgget(ipckey, IPC_CREAT | 0666);
	src.mq_id = conn_mqid;

	dout(0) << __func__ << " IPC key: " << ipckey << ", mqueue id:" << conn_mqid << dendl;


	if (conn_mqid < 0) {
		dout(0) << "Fatal: Create IPC pipe failed." << dendl;
		// TODO: Here we need to give out information to admin. However we do not do this now.
		return NULL;
	}

	dout(0) << "IPCWorker from IPCMessenger " << msgr->get_nodeid() << " starts listening to " << src.rank << " for new messages. Message Queue ID: " << conn_mqid << dendl;

	// Never stops. Killed by IPCMessenger destructor.
	while (true) {
		dout(20) << "IPCWorker: Blocking and waiting for new message." << dendl;
		Message * new_msg = receive_message();
		if (new_msg) {
			handle_message(new_msg);
		}
		else {
			dout(1) << "Receive failed. Ignored." << dendl;
		}
	}

	// Fake return.
	return NULL;
}

int IPCWorker::recv_raw(char * buf, size_t len)
{
	Mutex::Locker l(mutex);
	return _recv_raw(buf, len);
}

Message * IPCWorker::receive_message()
{
	Mutex::Locker l(mutex);
	return _receive_message();
}

int IPCWorker::_recv_raw(char * buf, size_t len)
{
	assert(mutex.is_locked());

	if (!buf)	return -1;
	if (!len)	return 0;

	size_t msg_size = msgr->get_msg_size();

	size_t left = len;
	size_t recved = 0; // single chunk
	size_t recved_total = 0;

	ipcmsg_t msgbuf;
	bool more_than_expected = false;
	bool has_more_msg = true;

	ipc_mqid_t mq_id = src.mq_id;
	
	do {
		if (msgrcv(mq_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0, 0) < 0) { // blocking
			return -1;
		}

		dout(20) << __func__ << " Received msg msize = " << msgbuf.msize
				 << ", msg_id = " << msgbuf.msg_id << ", msg_seq = " << msgbuf.msg_seq
				 << dendl;

		// TODO: We do not care about mtype now

		if (msgbuf.msize <= msg_size)
			has_more_msg = false;

		recved = has_more_msg ? msg_size : msgbuf.msize;
		recved_total += recved;

		if (recved > left) {
			dout(1) << __func__ << " More msg seems available than expected(" << left << "). Mark dropping..." << dendl;
			more_than_expected = true;
			// only keeps expected size
			memcpy(buf, msgbuf.mtext, left);
			left = 0;
		}
		else {
			memcpy(buf, msgbuf.mtext, recved);
			buf += recved;
			left -= msg_size;
		}
	} while (left > 0 && has_more_msg);

	if (more_than_expected) {
		dout(1) << __func__ << " Checking if more msg trunks need to be dropped." << dendl;

		while (has_more_msg) {
			// drop them
			if (msgrcv(mq_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0, 0) < 0) { // blocking
				return -1;
			}

			dout(1) << __func__ << " Dropping msg.msize = " << msgbuf.msize
				 	<< ", msg_id = " << msgbuf.msg_id << ", msg_seq = " << msgbuf.msg_seq
					<< dendl;

			if (msgbuf.msize <= msg_size)
				has_more_msg = false;
		}

		return 0;
	}

	dout(20) << __func__ << " Totally received: " << recved_total << " (expected: " << len << ")" << dendl;

	return recved_total;
}

Message * IPCWorker::_receive_message()
{
	assert(mutex.is_locked());

	ceph_msg_header header;
	ceph_msg_footer footer;

	int r = 0;

	// Receive header
	r = _recv_raw((char*)&header, sizeof(header));
	if (r < (int)sizeof(header))	return NULL;	// Here if we receive more information(i.e. r=0), still consider failed.
	
	dout(20) << "IPC reader got envelope type = " << header.type
    	<< " src " << entity_name_t(header.src)
    	<< " front=" << header.front_len
		<< " data=" << header.data_len
		<< " off " << header.data_off
    	<< dendl;

	// TODO: We do not verify header crc now.
	
	bufferlist front, middle, data;
	//utime_t recv_stamp = ceph_clock_now();

	bufferlist message_body;
	uint64_t message_size = header.front_len + header.middle_len + header.data_len;
	if (message_size) {
		bufferptr bp = buffer::create(message_size);
		if (_recv_raw(bp.c_str(), message_size) < (int)message_size)	return NULL;
		message_body.push_back(std::move(bp));

		message_body.splice(0, header.front_len, &front);
		message_body.splice(0, header.middle_len, &middle);

		message_body.splice(0, header.data_len, &data);
		
		if (message_body.length() > 0)	{
			dout(1) << __func__ << " Warning: still data left in IPC message, length: " << message_body.length() << dendl;
		}
	}
	
  	dout(20) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
	   << " byte(s) message" << dendl;

	// Receive footer
	r = _recv_raw((char*)&footer, sizeof(footer));
	if (r < (int)sizeof(footer))	return NULL;	// Here if we receive more information(i.e. r=0), still consider failed.

	Connection * conn = msgr->get_conn(src.rank);
	Message * msg = pre_decode_message(g_ceph_context, header, footer, front, middle, data, conn);
	if (!msg) {
		msg = decode_message(g_ceph_context, 0, header, footer, front, middle, data, conn);
	}

	if (msg) {
		dout(10) << "decode message success, type: " << msg->get_type() << dendl;
	}
	else {
		dout(1) << "decode message failed" << dendl;
	}
	return msg;
}

Message * IPCWorker::pre_decode_message(CephContext * cct, ceph_msg_header & header, ceph_msg_footer & footer, bufferlist & front, bufferlist & middle, bufferlist & data, Connection * conn)
{
	IPCMessagePreDecoder * decoder = get_pre_decoder();
	return decoder ? decoder->decode(cct, header, footer, front, middle, data, conn) : NULL;
}

string IPCWorker::name()
{
	string sname = "IPCWkr(";
	sname += msgr ? std::to_string(msgr->get_nodeid()) : "?";
	sname += "<=>";
	sname += std::to_string(src.rank);
	sname += ")";
	return sname;
}