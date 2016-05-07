#include "msgq.h"
#include "msgq.c"

static int snd_msgqid, rcv_msgqid;

void msgq_client_init()
{
    key_t snd_key, rcv_key;

    rcv_key = msgq_create_key_file(FTOK_CLI_KEY);
    snd_key = msgq_create_key_file(FTOK_SRV_KEY);

    snd_msgqid = msgq_init(snd_key, MSGQ_MODE);
    rcv_msgqid = msgq_init(rcv_key, MSGQ_MODE);
}

/*
 * Sends json data to server by writing it out to a file
 */
void msgq_client_send(const char *data)
{
    int fd;

    /* write json data out to tmp file */
    fd = open(TMP_FILE, O_RDWR |  O_TRUNC);
    assert(fd > 0);

    write(fd, data, strlen(data) + 1);
    close(fd);

    /* send out file name */
    msgq_send(snd_msgqid, TMP_FILE);
}

/*
 * Returns back file name passed from server. 
 * malloced string must be freed by client
 */
char *msgq_client_receive()
{
    return msgq_receive(rcv_msgqid);
}

void msgq_client_disconnect()
{
    if (msgctl(snd_msgqid, IPC_RMID, NULL) == -1) {
        perror("msgctl");
        exit(1);
    }

    if (msgctl(rcv_msgqid, IPC_RMID, NULL) == -1) {
        perror("msgctl");
        exit(1);
    }

}


