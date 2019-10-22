#ifndef _CEPH_IPC_IPCTYPES_H_
#define _CEPH_IPC_IPCTYPES_H_

typedef int ipc_rank_t;
typedef int ipc_mqid_t;

struct IPC_entity_t {
	ipc_rank_t rank;
	ipc_mqid_t mq_id;

	IPC_entity_t(ipc_rank_t rank, ipc_mqid_t mq_id) :
		rank(rank), mq_id(mq_id) {}
};

#define IPC_MSG_TYPE_DEFAULT 1
#define IPC_MSG_TYPE_CONNECTION 2
#define IPC_MSG_TYPE_SHUTDOWN 3

//#define IPC_DEFAULT_MSG_SIZE 1024
#define IPC_DEFAULT_MSG_SIZE 1048576

#define IPC_TARGET_FAKE_SHUTDOWN 100

struct ipcmsg_t {
	long mtype;
	size_t msize; // message size. plus 1 if has more chunks
	int msg_id;
	int msg_seq;
	char mtext[IPC_DEFAULT_MSG_SIZE];
};

#define IPCMSG_BODY_LEN (sizeof(ipcmsg_t) - sizeof(long))

#endif /* ipc/ipctypes.h */
