#ifndef __TEST_SXY_MSG_MTEST_H__
#define __TEST_SXY_MSG_MTEST_H__

#include <sys/time.h>

#include <iostream>
using namespace std;

#include "common/Clock.h"

#define CEPH_MSG_TEST 10000

class MTest : public Message {
public:
	string s;
	int data;
	struct timeval takeoff;
	struct timeval arrived;
	//utime_t takeoff;
	//utime_t arrived;
	long acc;
	int counter;

	MTest() : Message(CEPH_MSG_TEST), data(0),
		acc(0), counter(0)
		{}
	MTest(int data) : Message(CEPH_MSG_TEST), data(data),
		acc(0), counter(0)
		{}
	~MTest() override {}

	int get_data() { return data; }
	void set_data(int data) { this->data = data; }
	void inc_data() { data++; }
	void set_s(string s_) { s = s_; }
	const char *get_type_name() const override { return "Test"; }
	void encode_payload(uint64_t features) override {
		//cout << "\nEncode payload: acc = " << acc << std::endl;
		::encode(s, payload);
		::encode(data, payload);
		::encode(acc, payload);
		::encode(counter, payload);
		//takeoff = ceph_clock_now();
		//::encode(takeoff, payload);
		gettimeofday(&takeoff, NULL);
		::encode(takeoff.tv_sec, payload);
		::encode(takeoff.tv_usec, payload);
	}

	void decode_payload() override {
		gettimeofday(&arrived, NULL);

		//arrived = ceph_clock_now();
		bufferlist::iterator p = payload.begin();
		::decode(s, p);
		::decode(data, p);
		::decode(acc, p);
		::decode(counter, p);

		::decode(takeoff.tv_sec, p);
		::decode(takeoff.tv_usec, p);
		//cout << "\nDecode payload: acc = " << acc << std::endl;
		//::decode(takeoff, p);
		//long temp = (arrived.tv_sec - takeoff.tv_sec) * 1000000 + arrived.tv_usec - takeoff.tv_usec;
		//cout << "Decode payload: " << temp << std::endl;
		acc += (arrived.tv_sec - takeoff.tv_sec) * 1000000 + arrived.tv_usec - takeoff.tv_usec;
		counter++;
		data++;
	}
};

#endif /* test/sxy/msg/MTest.h */
