#ifndef _CEPH_IPC_MDSIPCWORKER_H_
#define _CEPH_IPC_MDSIPCWORKER_H_

class MDSIPCWorker : public IPCWorker {
public:
	MDSIPCWorker();
	~MDSIPCWorker() {}

protected:
	void * work() override;
};

#endif /* ipc/MDSIPCWorker.h */
