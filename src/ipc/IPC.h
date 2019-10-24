#ifndef __IPC_IPC_H__
#define __IPC_IPC_H__

#include "ipctypes.h"

ipc_mqid_t IPC_raw_create(int key);
int IPC_raw_send(ipc_mqid_t mq_id, const char * buf, size_t len, long message_type = IPC_MSG_TYPE_DEFAULT, size_t msg_size = IPC_DEFAULT_MSG_SIZE);
int IPC_raw_recv(ipc_mqid_t mq_id, char * buf, size_t len = 0, long * p_message_type = 0, size_t msg_size = IPC_DEFAULT_MSG_SIZE);

#endif /* ipc/IPC.h */
