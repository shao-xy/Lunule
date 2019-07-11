#include <memory.h>

#include "mds_scaling/IPC.h"

#include "common/debug.h"

#include "mds/MDSRank.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

#define IPC_KEY_BASE 10000

struct ipcmsg_t {
	long mtype;
	size_t msize; // message size. plus 1 if has more chunks
	char mtext[IPC_DEFAULT_MSG_SIZE];
};

#undef dout_prefix
#define dout_prefix *_dout << "mds." << msgr->get_nodeid() << ".ipc_worker[listening to " << src << "] "

// Returns:
// -1: Error happens
// 0: IPC trys to give longer message than expected. Only keeps information of size 'len'
// equal len: Expected
// less than len: Shorter message
int IPCWorker::do_recvmsg(char * buf, size_t len)
{
	if (!buf)	return -1;
	if (!len)	return 0;

	size_t msg_size = msgr->get_msg_size();

	size_t left = len;
	size_t recved = 0; // single chunk
	size_t recved_total = 0;

	ipcmsg_t msgbuf;
	bool more_than_expected = false;
	bool has_more_msg = true;

	do {
		if (msgrcv(msg_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0, 0) < 0) { // blocking
			return -1;
		}

		dout(20) << __func__ << " Received msg msize = " << msgbuf.msize << dendl;

		// TODO: We do not care about mtype now

		if (msgbuf.msize <= msg_size)
			has_more_msg = false;

		recved = has_more_msg ? msg_size : msgbuf.msize;
		recved_total += recved;

		if (recved > left) {
			dout(5) << __func__ << " More msg seems available than expected. Mark dropping..." << dendl;
			more_than_expected = true;
			// only keeps expected size
			memcpy(buf, msgbuf.mtext, left);
			left = 0;
		}
		else {
			dout(30) << __func__ << " Received chunk size = " << recved << dendl;
			memcpy(buf, msgbuf.mtext, recved);
			buf += recved;
			left -= msg_size;
		}
	} while (left > 0 && has_more_msg);

	if (more_than_expected) {
		dout(5) << __func__ << " Checking if more msg trunks need to be dropped." << dendl;

		while (has_more_msg) {
			// drop them
			if (msgrcv(msg_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0, 0) < 0) { // blocking
				return -1;
			}

			dout(5) << __func__ << " Dropping msg.msize = " << msgbuf.msize << dendl;

			if (msgbuf.msize <= msg_size)
				has_more_msg = false;
		}

		return 0;
	}

	dout(20) << __func__ << " Totally received: " << recved_total << " (expected: " << len << ")" << dendl;
	
	return recved_total;
}

Message * IPCWorker::try_recvmsg()
{
	Mutex::Locker recv_lock(mutex);

	ceph_msg_header header;
	ceph_msg_footer footer;

	int r = 0;

	// Receive header
	r = do_recvmsg((char*)&header, sizeof(header));
	if (r < (int)sizeof(header))	return NULL;	// Here if we receive more information(i.e. r=0), still consider failed.
	
	dout(20) << "IPC reader got envelope type = " << header.type
    	<< " src " << entity_name_t(header.src)
    	<< " front=" << header.front_len
		<< " data=" << header.data_len
		<< " off " << header.data_off
    	<< dendl;

	// TODO: We do not verify header crc now.
	
	bufferlist front, middle, data;
	utime_t recv_stamp = ceph_clock_now();

	bufferlist message_body;
	uint64_t message_size = header.front_len + header.middle_len + header.data_len;
	if (message_size) {
		bufferptr bp = buffer::create(message_size);
		if (do_recvmsg(bp.c_str(), message_size) < (int)message_size)	return NULL;
		message_body.push_back(std::move(bp));
		message_body.splice(0, header.front_len, &front);
		message_body.splice(0, header.middle_len, &middle);
		data.claim(message_body);
	}
	
  	dout(20) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
	   << " byte message" << dendl;

	// Receive footer
	r = do_recvmsg((char*)&footer, sizeof(footer));
	if (r < (int)sizeof(footer))	return NULL;	// Here if we receive more information(i.e. r=0), still consider failed.

	return decode_message(g_ceph_context, 0, header, footer, front, middle, data, NULL);
}

void * IPCWorker::entry() {
	int ipckey = IPC_getkey(src, msgr->get_nodeid());

	msg_id = msgget(ipckey, IPC_CREAT | 0666);
	if (msg_id == -1) {
		dout(5) << "Fatal: Create IPC pipe failed." << dendl;
		// TODO: Here we need to give out information to admin. However we do not do this now.
		return NULL;
	}

	// Never stops. Killed by IPCMessenger destructor.
	while (true) {
		Message * new_msg = try_recvmsg();
		if (new_msg) {
			MDSRankDispatcher * rank_disp = static_cast<MDSRankDispatcher *>(msgr->get_rank());
			rank_disp->ms_dispatch(new_msg);
		}
	}

	// Fake return.
	return NULL;
}

#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdsrank->whoami << ".ipc "
	
bool IPCMessenger::do_sendmsg(int msg_id, void * buffer, size_t len)
{
	assert(msg_size == IPC_DEFAULT_MSG_SIZE);
	if (len <= 0)	return false;

	ipcmsg_t msgbuf;
	size_t msglen = sizeof(ipcmsg_t) - sizeof(long);
	msgbuf.mtype = IPC_DEFAULT_MSG_TYPE;
	memset(&msgbuf.mtext, 0, IPC_DEFAULT_MSG_SIZE);
	while (len > msg_size) {
		msgbuf.msize = msg_size + 1;
		memcpy(&msgbuf.mtext, buffer, msg_size);
		len -= msg_size;

		dout(20) << __func__ << " IPC: send size = " << msg_size << ", left: " << len << dendl;

		if (msgsnd(msg_id, &msgbuf, msglen, 0) < 0) {
			// something bad happens
			dout(1) << __func__ << " IPC failed for this chunk." << dendl;
			return false;
		}
	}

	msgbuf.msize = len;
	memcpy(&msgbuf.mtext, buffer, len);
	dout(20) << __func__ << " IPC: send size = " << msg_size << ", no more left." << dendl;
	if (msgsnd(msg_id, &msgbuf, msglen, 0) < 0) {
		// something bad happens
		dout(1) << __func__ << " IPC failed for this chunk." << dendl;
		return false;
	}
	return true;
}

bool IPCMessenger::do_sendmsg_mds(int msg_id, const ceph_msg_header& header,
						const ceph_msg_footer& footer, bufferlist blist)
{
	// send header
	if (!do_sendmsg(msg_id, (char *)&header, sizeof(ceph_msg_header)))	return false;

	// payload (front+data)
	list<bufferptr>::const_iterator pb = blist.buffers().begin();
	unsigned b_off = 0;  // carry-over buffer offset, if any
	unsigned bl_pos = 0; // blist pos
	unsigned left = blist.length();

	while (left > 0) {
		unsigned donow = MIN(left, pb->length() - b_off);
		if (donow == 0) {
			dout(0) << "donow = " << donow << " left " << left << " pb->length " << pb->length()
				<< " b_off " << b_off << dendl;
		}
		assert(donow > 0);
		dout(30) << " bl_pos " << bl_pos << " b_off " << b_off
			<< " leftinchunk " << left
			<< " buffer len " << pb->length()
			<< " writing " << donow 
			<< dendl;

		if (!do_sendmsg(msg_id, (void*)(pb->c_str() + b_off), donow))	return false;

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

	// send footer
	return do_sendmsg(msg_id, (char *)&footer, sizeof(ceph_msg_footer));
}

IPCMessenger::IPCMessenger(MDSRank *mdsrank)
	: mdsrank(mdsrank), msg_size(IPC_DEFAULT_MSG_SIZE)
{
	if (!mdsrank) {
		dout(10) << "NULL MDSRank given. Suicide." << dendl;
		ceph_abort();
	}
}

IPCMessenger::~IPCMessenger()
{
	// Kill workers since they do blocking read.
	for (IPCWorker * worker : workers) {
		assert(!worker->am_self());
		Mutex::Locker l(worker->mutex);
		worker->kill(9);
	}
}

bool IPCMessenger::create_entity(mds_rank_t target)
{
	int my_rank = mdsrank->get_nodeid();
	// send tunnel
	int ipckey = IPC_getkey(my_rank, target);
	int msg_id = msgget(ipckey, IPC_CREAT | 0666);
	if (msg_id == -1) {
		dout(5) << "Create IPC pipe failed, returned to normal network stack." << dendl;
		return false;
	}
	existing.push_back(IPC_entity_t{target, msg_id});
	
	// receive tunnel
	IPCWorker * new_worker = new IPCWorker(this, target);
	string thrd_name = "IPCWorker(" + std::to_string(target) + ")";
	new_worker->create(thrd_name.c_str());
	workers.push_back(new_worker);

	return true;
}

int IPCMessenger::get_connected_msg_id(mds_rank_t mds)
{
	int msg_id = -1;
	for (vector<IPC_entity_t>::iterator it = existing.begin();
		 it != existing.end(); it++) {
		if (it->rank == mds) {
			msg_id = it->msg_id;
			break;
		}
	}
	return msg_id;
}
	
mds_rank_t IPCMessenger::get_nodeid() {
	return mdsrank->get_nodeid();
}

bool IPCMessenger::send_message_mds(Message * m, mds_rank_t target)
{
	int msg_id = get_connected_msg_id(target);
	if (msg_id == -1 && !create_entity(target)) { // not found
		return false;
	}

	// prepare everything
	const ceph_msg_header& header = m->get_header();
	const ceph_msg_footer& footer = m->get_footer();

	bufferlist blist = m->get_payload();
	blist.append(m->get_middle());
	blist.append(m->get_data());

	// now we have id of message pipe.
	return do_sendmsg_mds(msg_id, header, footer, blist);
}

int IPC_getkey(int from, int to)
{
	if (from < 0 || from > 99 || to < 0 || to > 99)	return -1;
	return IPC_KEY_BASE + 100 * from + to;
}
