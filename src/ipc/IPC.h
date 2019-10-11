#ifndef __IPC_IPC_H__
#define __IPC_IPC_H__

#include "ipctypes.h"

int IPC_raw_send(ipc_mqid_t mq_id, char * buf, int len, long message_type = IPC_MSG_TYPE_DEFAULT);
int IPC_raw_recv(ipc_mqid_t mq_id, char * buf, int len, long message_type = IPC_MSG_TYPE_DEFAULT);

#endif /* ipc/IPC.h */
