#include <sys/ipc.h>
#include <sys/msg.h>

#include "IPC.h"

int IPC_raw_send(ipc_mqid_t mq_id, char * buf, int len, long message_type = IPC_MSG_TYPE_DEFAULT)
{

}

int IPC_raw_recv(ipc_mqid_t mq_id, char * buf, int * plen, long * p_message_type)
{

}
