#include <linux/limits.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

struct msg_buf {
    long mtype;
    char mtext[PATH_MAX];
};

// replace with an autogenerated file name if necessary
#define TMP_FILE "/tmp/query.json"
#define FTOK_CLI_KEY "/tmp/cli_key"
#define FTOK_SRV_KEY "/tmp/srv_key"
#define FTOK_ID 'Z'
#define MSGQ_MODE 0666

void msgq_client_init(void);
void msgq_client_send(const char *data);
void msgq_client_disconnect(void);
char *msgq_client_receive(void);

void msgq_server_init();
void msgq_server_send(const char *data);
char *msgq_server_receive();