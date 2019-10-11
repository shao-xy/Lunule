#include <sstream>
#include <cstdlib>
#include <cstring>
#include <memory.h>
#include <errno.h>

#include "mds_scaling/IPC.h"

#include "common/debug.h"

#include "mds/MDSDaemon.h"
#include "mds/MDSRank.h"

#include "msg/async/AsyncMessenger.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

#define IPC_KEY_BASE 10000
#define IPC_LISTEN_KEY_BASE 9900

struct ipcmsg_t {
	long mtype;
	size_t msize; // message size. plus 1 if has more chunks
	int msg_id;
	int msg_seq;
	char mtext[IPC_DEFAULT_MSG_SIZE];
};

static inline void show_memory(char * buf, int len)
{
	stringstream ss;
	ss << std::hex;
	for (int i = 0; i < len; i++) {
		char c = *buf++;
		ss << (unsigned int)(unsigned char)c << ' ';
	}
	dout(30) << __func__ << " " << ss.str() << dendl;
}

static inline void show_bl(bufferlist& bl)
{
	// clone new
	bufferlist blist = bl;
	blist.rebuild();
	list<bufferptr> bufptrlist = blist.buffers();
	assert(bufptrlist.size() == 1);
	list<bufferptr>::iterator pbi = bufptrlist.begin();
	char * cpbi = pbi->c_str();
	unsigned dbgtotal = blist.length();

	dout(30) << __func__ << " blist content: " << dendl;
	show_memory(cpbi, dbgtotal);
}

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

	char * const orig_buf = buf;

	size_t msg_size = msgr->get_msg_size();

	size_t left = len;
	size_t recved = 0; // single chunk
	size_t recved_total = 0;

	ipcmsg_t msgbuf;
	bool more_than_expected = false;
	bool has_more_msg = true;

	do {
		if (msgrcv(ipc_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0, 0) < 0) { // blocking
			return -1;
		}

		dout(0) << __func__ << " Received msg msize = " << msgbuf.msize
				 << ", msg_id = " << msgbuf.msg_id << ", msg_seq = " << msgbuf.msg_seq
				 << dendl;

		// TODO: We do not care about mtype now

		if (msgbuf.msize <= msg_size)
			has_more_msg = false;

		recved = has_more_msg ? msg_size : msgbuf.msize;
		recved_total += recved;

		if (recved > left) {
			dout(5) << __func__ << " More msg seems available than expected(" << left << "). Mark dropping..." << dendl;
			more_than_expected = true;
			// only keeps expected size
			memcpy(buf, msgbuf.mtext, left);
			dout(30) << __func__ << "  Message fragment:" << dendl;
			show_memory(buf, left);
			left = 0;
		}
		else {
			memcpy(buf, msgbuf.mtext, recved);
			//dout(20) << __func__ << "  Message fragment:" << dendl;
			show_memory(buf, recved);
			buf += recved;
			left -= msg_size;
		}
	} while (left > 0 && has_more_msg);

	if (more_than_expected) {
		dout(5) << __func__ << " Checking if more msg trunks need to be dropped." << dendl;

		while (has_more_msg) {
			// drop them
			if (msgrcv(ipc_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0, 0) < 0) { // blocking
				return -1;
			}

			dout(5) << __func__ << " Dropping msg.msize = " << msgbuf.msize
				 	<< ", msg_id = " << msgbuf.msg_id << ", msg_seq = " << msgbuf.msg_seq
					<< dendl;

			if (msgbuf.msize <= msg_size)
				has_more_msg = false;

			// dout(20) << __func__ << "  Dropped fragment:" << dendl;
			show_memory(msgbuf.mtext, has_more_msg ? msg_size : msgbuf.msize);
		}

		return 0;
	}

	dout(20) << __func__ << " Totally received: " << recved_total << " (expected: " << len << ")" << dendl;

	// dout(20) << __func__ << " IPC message in hex:" << dendl;
	show_memory(orig_buf, len);
	
	return recved_total;
}

static void alloc_aligned_buffer(bufferlist& data, unsigned len, unsigned off)
{
  // create a buffer to read into that matches the data alignment
  unsigned left = len;
  if (off & ~CEPH_PAGE_MASK) {
    // head
    unsigned head = 0;
    head = MIN(CEPH_PAGE_SIZE - (off & ~CEPH_PAGE_MASK), left);
    data.push_back(buffer::create(head));
    left -= head;
  }
  unsigned middle = left & CEPH_PAGE_MASK;
  if (middle > 0) {
    data.push_back(buffer::create_page_aligned(middle));
    left -= middle;
  }
  if (left) {
    data.push_back(buffer::create(left));
  }
}

/**
 * \brief Try to receive a piece of message from IPC tunnel
 * \details Invoked in IPC Receive worker loop.
 *
 * @param[in] empty
 * @param[out] empty
 * @return Pointer to the message got, or NULL if failed.
 **/
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

		show_bl(message_body);

		message_body.splice(0, header.front_len, &front);
		message_body.splice(0, header.middle_len, &middle);

		message_body.splice(0, header.data_len, &data);
		
		if (message_body.length() > 0)	{
			dout(1) << __func__ << " Warning: still data left in IPC message, length: " << message_body.length() << dendl;
		}
		/*
		unsigned data_len = le32_to_cpu(header.data_len);
		unsigned data_off = le32_to_cpu(header.data_off);
		unsigned offset = 0;
		unsigned left = data_len;

		bufferlist newbuf;

		if (message_body.length() != header.data_len) {
			dout(1) << __func__ << " Data length not equal described in header, dropping." << dendl;
			return NULL;
		}

		// set up data bufferlist
	  	alloc_aligned_buffer(newbuf, data_len, data_off);
		bufferlist::iterator blp = newbuf.begin();
		while (left) {
			bufferptr bp = blp.get_current_ptr();
			int read = MIN(bp.length(), left);
			message_body.copy(offset, read, bp.c_str());
			blp.advance(read);
			data.append(bp, 0, read);
			offset += read;
			left -= read;
		}
		*/
	}
	
  	dout(20) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
	   << " byte message" << dendl;

	// Receive footer
	r = do_recvmsg((char*)&footer, sizeof(footer));
	if (r < (int)sizeof(footer))	return NULL;	// Here if we receive more information(i.e. r=0), still consider failed.

	Connection * conn = msgr->get_conn(src);
	Message * msg = decode_message(g_ceph_context, 0, header, footer, front, middle, data, conn);
	if (msg) {
		dout(0) << "decode message success, type: " << msg->get_type() << dendl;
	}
	else {
		dout(0) << "decode message failed" << dendl;
	}
	return msg;
}

void * IPCWorker::entry() {
	int ipckey = IPC_getkey(src, msgr->get_nodeid());

	dout(0) << "IPCWorker " << msgr->get_nodeid() << " starts listening for new messages." << dendl;

	ipc_id = msgget(ipckey, IPC_CREAT | 0666);
	if (ipc_id == -1) {
		dout(5) << "Fatal: Create IPC pipe failed." << dendl;
		// TODO: Here we need to give out information to admin. However we do not do this now.
		return NULL;
	}

	// Never stops. Killed by IPCMessenger destructor.
	while (true) {
		dout(20) << "IPCWorker: Blocking for new message." << dendl;
		Message * new_msg = try_recvmsg();
		if (new_msg) {
			//MDSRankDispatcher * rank_disp = static_cast<MDSRankDispatcher *>(msgr->get_rank());
			//{
			//	Mutex::Locker l(rank_disp->mds_lock);
			//	rank_disp->ms_dispatch(new_msg);
			//}
			msgr->ms_deliver(new_msg);
		}
	}

	// Fake return.
	return NULL;
}

#undef dout_prefix
#define dout_prefix *_dout << "mds." << msgr->get_nodeid() << ".ipc_processor "

mds_rank_t IPCProcessor::accept(int ipc_id)
{
	ipcmsg_t msgbuf;
	if (msgrcv(ipc_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0, 0) < 0) {
		dout(1) << "IPCProcessor: receive message failed (" << strerror(errno) << ")" << dendl;
		return -1;
	}
	
	assert(msgbuf.mtype == IPC_CONNECTION_MSG_TYPE);
	mds_rank_t rank = (mds_rank_t)atoi(msgbuf.mtext);

	dout(20) << __func__ << " Receive connection request for mds rank " << rank << dendl;
	return rank;
}

void * IPCProcessor::entry()
{
	assert(msgr);

	int ipckey = IPC_get_listenkey(msgr->get_nodeid());

	dout(0) << "IPCProcessor listening on IPC tunnel " << ipckey << dendl;

	int ipc_id = msgget(ipckey, IPC_CREAT | 0666);
	if (ipc_id == -1) {
		dout(5) << "Fatal: Create IPC pipe failed." << dendl;
		// TODO: Here we need to give out information to admin. However we do not do this now.
		return NULL;
	}

	mds_rank_t target_mds;
	// Never stops. Killed by IPCMessenger destructor.
	while (true) {
		dout(20) << "IPCProcessor: Blocking for new message." << dendl;
		if ((target_mds = accept(ipc_id)) != -1) {
			if (msgr->ensure_entity_exist(target_mds) == -1) {
				dout(1) << __func__ << " FATAL: Cannot create socket for target " << target_mds << dendl;
			}
		}
	}

	// Fake return.
	return NULL;
}

#undef dout_prefix
#define dout_prefix *_dout << "mds." << mdsrank->whoami << ".ipc "
	
bool IPCMessenger::do_sendmsg(int ipc_id, char * buffer, size_t len)
{
	static int msg_id = -1;
	dout(0) << __func__ << " IPC Message ID: " << msg_id << dendl;
	msg_id++;

	assert(msgr_mutex.is_locked());

	dout(20) << __func__ << " IPC [ipc_id = " << ipc_id << "] length = " << len << dendl;

	// dout(20) << __func__ << " IPC message in hex:" << dendl;
	show_memory(buffer, len);

	assert(msg_size == IPC_DEFAULT_MSG_SIZE);
	if (len <= 0)	return false;

	ipcmsg_t msgbuf;
	size_t msglen = sizeof(ipcmsg_t) - sizeof(long);
	msgbuf.mtype = IPC_DEFAULT_MSG_TYPE;
	memset(&msgbuf.mtext, 0, IPC_DEFAULT_MSG_SIZE);
	msgbuf.msg_id = msg_id;
	int msg_seq = 0;
	while (len > msg_size) {
		msgbuf.msize = msg_size + 1;
		msgbuf.msg_seq = msg_seq++;
		memcpy(&msgbuf.mtext, buffer, msg_size);
		len -= msg_size;
		buffer += msg_size;

		dout(20) << __func__ << " IPC (ipc_id = " << ipc_id << ", trunk size= " << msglen
				 << ", msg_id = " << msg_id << ", msg_seq = " << msgbuf.msg_seq
				 << "): send size = " << msg_size << ", left: " << len << dendl;
		
		// dout(20) << __func__ << "  Message fragment:" << dendl;
		show_memory(buffer, msg_size);

		int ret = msgsnd(ipc_id, &msgbuf, msglen, 0);
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
	dout(20) << __func__ << " IPC (ipc_id = " << ipc_id << ", trunk size= " << msglen
			 << ", msg_id = " << msg_id << ", msg_seq = " << msgbuf.msg_seq 
			 << "): send size = " << len << ", no more left." << dendl;
		
	// dout(20) << __func__ << "  Message fragment:" << dendl;
	show_memory(buffer, len);

	int ret = msgsnd(ipc_id, &msgbuf, msglen, 0);
	if (ret < 0) {
		// something bad happens
		dout(1) << __func__ << " IPC failed for this chunk. (return value: " << ret << ", ERRNO: " << strerror(errno) << ")" << dendl;
		return false;
	}
	return true;
}

bool IPCMessenger::do_sendmsg_mds(int ipc_id, const ceph_msg_header& header,
						const ceph_msg_footer& footer, bufferlist blist)
{
	Mutex::Locker l(msgr_mutex);

	dout(20) << __func__ << " ipc_id = " << ipc_id << dendl;

	dout(20) << __func__ << " IPC sender prepare envelope type =  " << header.type
    	<< " src " << entity_name_t(header.src)
    	<< " front=" << header.front_len
    	<< " middle=" << header.middle_len
		<< " data=" << header.data_len
		<< " off " << header.data_off
		<< dendl;

	// send header
	if (!do_sendmsg(ipc_id, (char *)&header, sizeof(ceph_msg_header))) {
		dout(1) << __func__ << " Failed to send message header (size: " << sizeof(ceph_msg_header) << ")" << dendl;
		return false;
	}

	show_bl(blist);

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
			dout(0) << "donow = " << donow << " left " << left << " pb->length " << pb->length()
				<< " b_off " << b_off << dendl;
		}
		assert(donow > 0);
		dout(30) << " bl_pos " << bl_pos << " b_off " << b_off
			<< " leftinchunk " << left
			<< " buffer len " << pb->length()
			<< " writing " << donow 
			<< dendl;

		//if (!do_sendmsg(ipc_id, (void*)(pb->c_str() + b_off), donow))	return false;
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

	if (!do_sendmsg(ipc_id, target_buffer, total))	return false;

	// send footer
	return do_sendmsg(ipc_id, (char *)&footer, sizeof(ceph_msg_footer));
}

IPCMessenger::IPCMessenger(MDSRank *mdsrank)
	: mdsrank(mdsrank), processor(NULL),
	msgr_mutex("IPCMessenger(" + std::to_string(mdsrank->get_nodeid()) + ")"),
	msg_size(IPC_DEFAULT_MSG_SIZE)
{
	if (!mdsrank) {
		dout(10) << "NULL MDSRank given. Suicide." << dendl;
		ceph_abort();
	}

	processor = new IPCProcessor(this);
	processor->create("IPCProcessor");
}

IPCMessenger::~IPCMessenger()
{
	// Kill workers since they do blocking read.
	for (IPCWorker * worker : workers) {
		assert(!worker->am_self());
		Mutex::Locker l(worker->mutex);
		worker->kill(SIGKILL);
	}
	processor->kill(SIGKILL);
}

int IPCMessenger::create_entity(mds_rank_t target)
{
	assert(msgr_mutex.is_locked());
	dout(0) << __func__ << " target: " << target << dendl;

	mds_rank_t my_rank = mdsrank->get_nodeid();
	
	if (get_connected_ipc_id(target) != -1) {
		// already exists, abort
		dout(0) << __func__ << " target already exists." << dendl;
		return false;
	}

	// ensure that target listen to us
	dout(20) << "Create handshape socket for rank " << target << " and acquire for listening" << dendl;
	int ipc_connect_key = IPC_get_listenkey(target);
	int msg_connect_id = msgget(ipc_connect_key, IPC_CREAT | 0666);
	if (msg_connect_id == -1) {
		dout(5) << "Create handshape socket failed (ERRNO: " << strerror(errno) << dendl;
		return false;
	}
	ipcmsg_t msgbuf;
	memset(&msgbuf, 0, sizeof(ipcmsg_t));
	msgbuf.mtype = IPC_CONNECTION_MSG_TYPE;
	sprintf(msgbuf.mtext, "%d", my_rank);
	if (msgsnd(msg_connect_id, &msgbuf, sizeof(ipcmsg_t) - sizeof(long), 0) < 0) {
		dout(5) << "Create handshape socket failed (ERRNO: " << strerror(errno) << dendl;
		return false;
	}
	// No wait here.
	// If listener is faster, then its IPCWorker will be blocked until the messages are sent.
	// If we are faster, messages will remain in the message queue. We could just hope 
	// listener will soon receive. If the queue is full, our sending thread will be blocked.

	// send tunnel
	int ipckey = IPC_getkey(my_rank, target);
	int ipc_id = msgget(ipckey, IPC_CREAT | 0666);
	if (ipc_id == -1) {
		dout(5) << "Create IPC pipe failed, returned to normal network stack." << dendl;
		return false;
	}
	dout(0) << __func__ << "Get IPC Message Queue ID: " << ipc_id << dendl;
	existing.push_back(IPC_entity_t{target, ipc_id});
	
	IPCWorker * new_worker = new IPCWorker(this, target);
	string thrd_name = "IPCWorker(" + std::to_string(target) + ")";
	new_worker->create(thrd_name.c_str());
	workers.push_back(new_worker);

	return ipc_id;
}

int IPCMessenger::get_connected_ipc_id(mds_rank_t mds)
{
	assert(msgr_mutex.is_locked());
	int ipc_id = -1;
	for (vector<IPC_entity_t>::iterator it = existing.begin();
		 it != existing.end(); it++) {
		if (it->rank == mds) {
			ipc_id = it->ipc_id;
			break;
		}
	}
	return ipc_id;
}
	
mds_rank_t IPCMessenger::get_nodeid() {
	return mdsrank->get_nodeid();
}

bool IPCMessenger::send_message_mds(Message * m, mds_rank_t target)
{
	dout(20) << __func__ << " target = " << target << ", Message type: " << m->get_type() << dendl;
	int ipc_id = ensure_entity_exist(target);

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

	// now we have id of message pipe.
	return do_sendmsg_mds(ipc_id, header, footer, blist);
}

int IPCMessenger::ensure_entity_exist(mds_rank_t mds)
{
	dout(20) << __func__ << " target: " << mds << dendl;
	Mutex::Locker l(msgr_mutex);
	int ipc_id = get_connected_ipc_id(mds);
	if (ipc_id == -1)
		return create_entity(mds);
	else
		return ipc_id;
}

void IPCMessenger::ms_deliver(Message * m)
{
	assert(mdsrank->mds_daemon);
	mdsrank->mds_daemon->ms_dispatch(m);
}

Connection * IPCMessenger::get_conn(mds_rank_t target) {
	// receive tunnel
	Connection * conn = static_cast<AsyncMessenger*>(mdsrank->messenger)->lookup_conn(mdsrank->mdsmap->get_addr(target)).get();
	if (!conn) {
		dout(1) << __func__ << " Warning: cannot find connection for target " << target << " in mdsmap. Wait for 5 seconds." << dendl;
		
		utime_t t;
		t.set_from_double(5.0);	// Wait for 5 seconds in case new connection may be created
		t.sleep();

		conn = static_cast<AsyncMessenger*>(mdsrank->messenger)->lookup_conn(mdsrank->mdsmap->get_addr(target)).get();

		if (!conn) {
			dout(1) << __func__ << "  Still cannot found after 5 seconds. Not found." << dendl;
		}
	}
	return conn;
}

int IPC_getkey(mds_rank_t from, mds_rank_t to)
{
	if (from < 0 || from > 99 || to < 0 || to > 99)	return -1;
	return IPC_KEY_BASE + 100 * from + to;
}

int IPC_get_listenkey(mds_rank_t mds) 
{
	assert(mds < 100);
	return IPC_LISTEN_KEY_BASE + mds;
}
