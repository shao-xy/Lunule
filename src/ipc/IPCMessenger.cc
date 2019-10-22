#include <iostream>
#include <cstdlib>

#include <errno.h>
#include <cstring>

#include <signal.h>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#include "IPC.h"
#include "IPCMessenger.h"
#include "IPCWorker.h"

#include "common/debug.h"

#define IPC_KEY_BASE 10000
#define IPC_LISTEN_KEY_BASE 9900

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_ipc

#undef dout_prefix
#define dout_prefix *_dout << "ipc." << msgr->get_nodeid() << ".processor "

ipc_rank_t IPCProcessor::accept(ipc_mqid_t mq_id)
{
	dout(1) << __func__ << " IPCProcessor waiting for new connections" << dendl;
	ipcmsg_t msgbuf;
	if (msgrcv(mq_id, &msgbuf, IPCMSG_BODY_LEN, 0, 0) < 0) {
		dout(1) << __func__ << "IPCProcessor: receive message failed (" << strerror(errno) << ")" << dendl;
		return -1;
	}

	if (msgbuf.mtype == IPC_MSG_TYPE_SHUTDOWN) {
		return IPC_TARGET_FAKE_SHUTDOWN;
	}
	
	assert(msgbuf.mtype == IPC_MSG_TYPE_CONNECTION);
	ipc_rank_t rank = (ipc_rank_t)atoi(msgbuf.mtext);

	dout(10) << __func__ << " Receive connection request for ipc target " << rank << dendl;
	return rank;
}

void * IPCProcessor::entry()
{
	assert(msgr);

	int key = IPCMessenger::get_listen_key(msgr->get_nodeid());
	ipc_mqid_t mq_id = msgget(key, IPC_CREAT | 0666);
	dout(0) << "IPCProcessor listening on IPC tunnel key = " << key << ", mqueue id = " << mq_id << dendl;

	ipc_rank_t target;
	while (true) {
		dout(20) << "IPCProcessor(" << msgr->get_nodeid() << "): Waiting for new message." << dendl;
		if ((target = accept(mq_id)) != -1) {
			if (target == IPC_TARGET_FAKE_SHUTDOWN)	break;
			if (!msgr->create_or_get(target)) {
				dout(1) << __func__ << " FATAL: Cannot create socket for target " << target << dendl;
			}
		}
	}

	dout(0) << "IPCProcessor shutdown." << dendl;
	return NULL;
}

#undef dout_prefix
#define dout_prefix *_dout << "ipc." << whoami << " "

IPCMessenger::IPCMessenger(ipc_rank_t rank)
	: whoami(rank), proc(NULL),
	msgr_mutex("IPCMessenger(" + std::to_string(rank) + ")"),
	msg_size(IPC_DEFAULT_MSG_SIZE)
{
	if (rank < 0) {
		dout(0) << "Negative rank given. Suicide." << dendl;
		ceph_abort();
	}

	proc = new IPCProcessor(this);
	proc->create("IPCProcessor");
}

IPCMessenger::~IPCMessenger()
{
	// Kill workers since they do blocking read.
	for (IPCWorker * worker : workers) {
		assert(!worker->am_self());
		Mutex::Locker l(worker->mutex);
		worker->kill(SIGKILL);
	}
	proc->kill(SIGKILL);
}

bool IPCMessenger::send_message(Message * m, ipc_rank_t target)
{
	Mutex::Locker l(msgr_mutex);
	return _send_message(m, target);
}

bool IPCMessenger::_send_message(Message * m, ipc_rank_t target)
{
	assert(msgr_mutex.is_locked());
	dout(20) << __func__ << " target = " << target << ", Message type: " << m->get_type() << dendl;

	// encode message
	m->set_connection(NULL);
	m->encode(0, 0);
	
	// prepare everything
	const ceph_msg_header& header = m->get_header();
	const ceph_msg_footer& footer = m->get_footer();

	dout(20) << __func__ << "  merging bufferlists:"
			 << " payload size: " << m->get_payload().length()
			 << " middle size: " << m->get_middle().length()
			 << " data size: " << m->get_data().length()
			 << dendl;

	bufferlist blist = m->get_payload();
	blist.append(m->get_middle());
	blist.append(m->get_data());

	//dout(0) << __func__ << " Dump payload before sending:" << dendl;
	//blist.hexdump(*_dout);

	// now we have id of message pipe.
	return _send_message_fragments(target, header, footer, blist);
}

bool IPCMessenger::_send_message_fragments(const ipc_rank_t target, const ceph_msg_header & header, const ceph_msg_footer & footer, bufferlist &blist)
{
	assert(msgr_mutex.is_locked());

	dout(20) << __func__ << " ipc target = " << target << dendl;

	dout(20) << __func__ << " IPC sender prepare envelope type =  " << header.type
    	<< " src " << entity_name_t(header.src)
    	<< " front=" << header.front_len
    	<< " middle=" << header.middle_len
		<< " data=" << header.data_len
		<< " off " << header.data_off
		<< dendl;

	// send header
	if (!_sendto(target, (char *)&header, sizeof(ceph_msg_header))) {
		dout(1) << __func__ << " Failed to send message header (size: " << sizeof(ceph_msg_header) << ")" << dendl;
		return false;
	}

	// payload (front+data)
	list<bufferptr>::const_iterator pb = blist.buffers().begin();
	unsigned b_off = 0;  // carry-over buffer offset, if any
	unsigned bl_pos = 0; // blist pos
	unsigned left = blist.length();
	unsigned total = left;

	dout(20) << __func__ <<  " blist size = " << left << dendl;

	bufferptr combined = buffer::create(total);
	char * const target_buffer = combined.c_str();
	char * current_pos = target_buffer;

	while (left > 0) {
		unsigned donow = MIN(left, pb->length() - b_off);
		if (donow == 0) {
			dout(20) << "donow = " << donow << " left " << left << " pb->length " << pb->length()
				<< " b_off " << b_off << dendl;
		}
		assert(donow > 0);
		dout(30) << " bl_pos " << bl_pos << " b_off " << b_off
			<< " leftinchunk " << left
			<< " buffer len " << pb->length()
			<< " writing " << donow 
			<< dendl;

		memcpy(current_pos, (void*)(pb->c_str() + b_off), donow);
		current_pos += donow;

		assert(left >= donow);
		left -= donow;
		b_off += donow;
		bl_pos += donow;
		if (left == 0)
			break;
		while (b_off == pb->length()) {
			++pb;
			b_off = 0;
		}
	}
	assert(left == 0);

	if (!_sendto(target, target_buffer, total))	return false;

	// send footer
	return _sendto(target, (char *)&footer, sizeof(ceph_msg_footer));
}

bool IPCMessenger::sendto(ipc_rank_t rank, char * buffer, size_t len)
{
	Mutex::Locker l(msgr_mutex);
	return _sendto(rank, buffer, len);
}

bool IPCMessenger::_sendto(ipc_rank_t rank, char * buffer, size_t len)
{
	assert(msgr_mutex.is_locked());

	static int msg_id = -1;
	msg_id++;
	dout(20) << __func__ << " IPC Message ID: " << msg_id << dendl;

	ipc_mqid_t mq_id = _create_or_get(rank)->mq_id;
	dout(20) << __func__ << " IPC [mq_id = " << mq_id << "] length = " << len << dendl;

	assert(msg_size == IPC_DEFAULT_MSG_SIZE);
	if (len <= 0)	return false;

	ipcmsg_t msgbuf;
	size_t msglen = sizeof(ipcmsg_t) - sizeof(long);
	msgbuf.mtype = IPC_MSG_TYPE_DEFAULT;
	memset(&msgbuf.mtext, 0, IPC_DEFAULT_MSG_SIZE);
	msgbuf.msg_id = msg_id;
	int msg_seq = 0;
	while (len > msg_size) {
		msgbuf.msize = msg_size + 1;
		msgbuf.msg_seq = msg_seq++;
		memcpy(&msgbuf.mtext, buffer, msg_size);
		len -= msg_size;
		buffer += msg_size;

		dout(20) << __func__ << " IPC (mq_id = " << mq_id << ", trunk size= " << msglen
				 << ", msg_id = " << msg_id << ", msg_seq = " << msgbuf.msg_seq
				 << "): send size = " << msg_size << ", left: " << len << dendl;

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
	memcpy(&msgbuf.mtext, buffer, len);
	dout(20) << __func__ << " IPC (mq_id = " << mq_id << ", trunk size= " << msglen
			 << ", msg_id = " << msg_id << ", msg_seq = " << msgbuf.msg_seq 
			 << "): send size = " << len << ", no more left." << dendl;

	int ret = msgsnd(mq_id, &msgbuf, msglen, 0);
	if (ret < 0) {
		// something bad happens
		dout(1) << __func__ << " IPC failed for this chunk. (return value: " << ret << ", ERRNO: " << strerror(errno) << ")" << dendl;
		return false;
	}
	return true;
}

IPC_entity_t * IPCMessenger::create_or_get(ipc_rank_t rank)
{
	Mutex::Locker l(msgr_mutex);

	return _create_or_get(rank);
}

IPC_entity_t * IPCMessenger::_create_or_get(ipc_rank_t rank)
{
	assert(msgr_mutex.is_locked());
	dout(20) << __func__ << " target: " << rank << dendl;
	IPC_entity_t * entity = _get_connected_entity(rank);
	if (!entity)
		return _create_connected_entity(rank);
	else
		return entity;
}

IPC_entity_t * IPCMessenger::_get_connected_entity(ipc_rank_t rank)
{
	assert(msgr_mutex.is_locked());
	IPC_entity_t * entity = NULL;
	for (vector<IPC_entity_t *>::iterator it = conn_list.begin();
		 it != conn_list.end(); it++) {
		if ((*it)->rank == rank) {
			entity = *it;
			break;
		}
	}
	return entity;
}

IPC_entity_t * IPCMessenger::_create_connected_entity(ipc_rank_t target)
{
	assert(msgr_mutex.is_locked());
	dout(0) << __func__ << " target: " << target << dendl;

	IPC_entity_t * try_get = _get_connected_entity(target);
	if (try_get) {
		// already exists, abort
		dout(0) << __func__ << " target already exists." << dendl;
		return try_get;
	}

	// ensure that target listen to us
	dout(20) << "Create handshape socket for rank " << target << " and acquire for listening" << dendl;
	int listen_key = IPCMessenger::get_listen_key(target);
	ipc_mqid_t listen_mqid = msgget(listen_key, IPC_CREAT | 0666);
	if (listen_mqid  < 0) {
		dout(5) << "Create handshape socket failed (ERRNO: " << strerror(errno) << ", Listen Key = " << listen_key << ", Message Queue ID = " << listen_mqid << dendl;
		return NULL;
	}
	ipcmsg_t msgbuf;
	memset(&msgbuf, 0, sizeof(ipcmsg_t));
	msgbuf.mtype = IPC_MSG_TYPE_CONNECTION;
	sprintf(msgbuf.mtext, "%d", whoami);
	if (msgsnd(listen_mqid, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0) < 0) {
		dout(5) << "Create handshape socket failed (ERRNO: " << strerror(errno) << ", Listen Key = " << listen_key << ", Message Queue ID = " << listen_mqid << dendl;
		return NULL;
	}
	// No wait here.
	// If listener is faster, then its IPCProcessor will be blocked until the message arrives.
	// If I am faster, messages will remain in the message queue. We could just hope 
	// listener will soon receive. If the queue is full, our sending thread will be blocked.

	// send tunnel
	int conn_key = IPCMessenger::get_conn_key(whoami, target);
	ipc_mqid_t conn_mqid = msgget(conn_key, IPC_CREAT | 0666);
	if (conn_mqid < 0) {
		dout(5) << "Create IPC pipe failed for target " << target << dendl;
		return NULL;
	}
	dout(0) << __func__ << " Get IPC Message Queue ID: " << conn_mqid << dendl;
	IPC_entity_t * new_entity = new IPC_entity_t(target, conn_mqid);
	conn_list.push_back(new_entity);
	
	IPCWorker * new_worker = create_worker(target);
	new_worker->create(new_worker->name().c_str());
	workers.push_back(new_worker);

	return new_entity;
}

int IPCMessenger::get_listen_key(ipc_rank_t rank)
{
	assert(rank < 100);
	return IPC_LISTEN_KEY_BASE + rank;
}

int IPCMessenger::get_conn_key(ipc_rank_t from, ipc_rank_t to)
{
	if (from < 0 || from > 99 || to < 0 || to > 99)	return -1;
	return IPC_KEY_BASE + 100 * from + to;
}

void IPCMessenger::shutdown()
{
	Mutex::Locker l(msgr_mutex);

	// shutdown processor
	// shutdown workers
	for (IPCWorker * worker : workers) {
	
	}
}
