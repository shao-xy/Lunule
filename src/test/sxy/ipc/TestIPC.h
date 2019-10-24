#ifndef _TEST_SXY_IPC_TESTIPC_H_
#define _TEST_SXY_IPC_TESTIPC_H_

#include <iostream>
using std::cout;

#include "ipcbench.h"

#include "ipc/msg/IPCMessenger.h"
#include "ipc/msg/IPCWorker.h"
#include "ipc/msg/IPCMessagePreDecoder.h"

#include "../msg/MTest.h"

#define dout_subsys ceph_subsys_
#define dout_context g_ceph_context

class TestIPCMessenger;

class TestIPCDecoder : public IPCMessagePreDecoder {
public:
	TestIPCDecoder() {}
	~TestIPCDecoder() override {}
protected:
	Message * generate_typed_message(int type) override {
		Message * m = 0;
		if (type == CEPH_MSG_TEST) {
			m = new MTest;
		}
		return m;
	}
};

class TestIPCWorker : public IPCWorker {
private:
	//utime_t acc;
	int counter;
	int tail_counter;
	bool reach_10;
	//long acc_sec;
	//long acc_usec;
	//long acc;
public:
	TestIPCWorker(TestIPCMessenger * msgr, ipc_rank_t from) : IPCWorker((IPCMessenger*)msgr, from), counter((from+1) % 2),
	//acc_sec(0), acc_usec(0)
	//acc(0) {}
	tail_counter(0), reach_10(false)
	{}
protected:
	void * handle_message(Message * m) override {
		//dout(0) << __func__  << " Get message <Type: " << m->get_type() << ">" << dendl;
		int type = m->get_type();
		assert(type == CEPH_MSG_TEST);
		MTest * msg = static_cast<MTest*>(m);
		//acc += msg->arrived - msg->takeoff;
		//acc_sec += msg->arrived.tv_sec - msg->takeoff.tv_sec;
		//acc_usec += msg->arrived.tv_usec - msg->takeoff.tv_usec;
		//acc += (msg->arrived.tv_sec - msg->takeoff.tv_sec) * 1000000 + msg->arrived.tv_usec - msg->takeoff.tv_usec;
		//counter++;

		//dout(0) << __func__ << " Get message data " << msg->get_data() << " acc = " << (double) acc << dendl;
		//dout(0) << __func__ << " Get message data " << msg->get_data() << " acc = " << acc_sec << '.' << acc_usec << dendl;
		//dout(0) << __func__ << " Get message data " << msg->get_data() << " acc = " << msg->acc << dendl;
		//dout(0) << __func__ << " Get message data " << msg->data << " acc = " << msg->acc << " counter = " << msg->counter << dendl;
		
		//dout(0) << __func__ << " Get message data " << msg->get_data() << " takeoff = " << msg->takeoff.tv_sec << '.' << msg->takeoff.tv_usec << ", arrived = " << msg->arrived.tv_sec << '.' << msg->arrived.tv_usec << dendl;
		
		//counter = msg->counter;
		counter += 2;
		tail_counter += 2;

		if (counter % 2 == 0) {
			//dout(0) << __func__ << " Accumulated latency: " << msg->acc << dendl;

			if (counter >= 0.1 * BENCH_SIZE && !reach_10) {
				//dout(0) << __func__ << " Reach 90%, restart accumulate latency." << dendl;
				reach_10 = true;
				msg->acc = 0;
				//dout(0) << __func__ << "  msg->acc = " << msg->acc << dendl;
				tail_counter = 0;
			}
		}

		if (counter >= BENCH_SIZE && counter % 2 == 0) {
			long acc = msg->acc;
			//dout(0) << __func__ << " Average latency: " << ((double)acc / counter) << dendl;
			dout(0) << __func__ << " Average latency: " << ((double)acc / tail_counter) << dendl;
			//dout(0) << __func__ << " Average latency: " << ((acc_sec + (double)acc_usec / 1000000) / BENCH_SIZE) << dendl;
			//acc.set_from_double(0.0);
			//acc_sec = 0;
			//acc_usec = 0;
			msg->acc = 0;
			msg->counter = 0;
			counter = 0;
			reach_10 = false;
			tail_counter = 0;
			return NULL;
		}
		else {
			//msg->inc_data();
			//msg->data++;
			//dout(0) << "Message data set to: " << msg->get_data() << dendl;
			//msgr->send_message(msg, src.rank);
		}

		//if (msgr->get_nodeid() % 2 == 1)
			//dout(0) << __func__ << "Message data set to: " << msg->data << dendl;
		msg->clear_payload();
		msgr->send_message(msg, src.rank);

		//delete msg;
		
		return NULL;
	}
	IPCMessagePreDecoder * get_pre_decoder() override { return new TestIPCDecoder(); }
};

class TestIPCMessenger : public IPCMessenger {
public:
	explicit TestIPCMessenger(ipc_rank_t whoami) : IPCMessenger(whoami) {}
protected:
	IPCWorker * create_worker(ipc_rank_t rank) override {
		IPCWorker * worker =  static_cast<IPCWorker *>(new TestIPCWorker(this, rank));
		dout(0) << "TestIPCMessenger::" << __func__ << " Create worker for rank " << worker->get_peer() << dendl;
		return worker;
	}
};

#endif /* test/sxy/ipc/TestIPC.h */
