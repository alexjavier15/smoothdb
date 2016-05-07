static int msgq_create_key_file(char *fname)
{
    int fd;
    key_t key;

    fd = open(fname, O_RDWR | O_CREAT | O_EXCL, 
            S_IRWXU | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    if (fd > 0)
        close(fd);            
    else
        perror("error open key file.\n");

    if ((key = ftok(fname, FTOK_ID)) == -1) {
        perror("ftok");
        exit(1);
    }

    return key;

}

static int msgq_init(key_t key, int flags)
{
    int msqid;

    if ((msqid = msgget(key, MSGQ_MODE | IPC_CREAT)) == -1) {
        perror("msgget");
        exit(1);
    }

    return msqid;

}

static void msgq_send(int msgqid, const char *fname)
{
    struct msg_buf buf;

    buf.mtype = 1;
    strcpy(buf.mtext, fname);

    if (msgsnd(msgqid, &buf, strlen(buf.mtext) + 1, 0) == -1) {
            perror("msgsnd");
    }
}


static char *msgq_receive(int msgqid)
{
    char *data;
    struct msg_buf buf;

    if (msgrcv(msgqid, &buf, sizeof(buf.mtext), 0, 0) == -1) {
        perror("msgrcv");
        exit(1);
    }

    data = malloc(strlen(buf.mtext) + 1);
    assert(data);

    strcpy(data, buf.mtext);

    return data;
}
