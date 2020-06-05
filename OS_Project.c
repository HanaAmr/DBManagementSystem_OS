#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdio.h>
#include <time.h>
#define numberOfRecords 1000

struct msgStruct
{
    long mtype;
    int senderPid;
    struct recordStruct
    {
        int key;
        char name[15];
        int salary;
    } Record;
};

struct loggerStruct
{
    char time[30];
    char message[100];
};

struct initialMsg
{
    long mtype;
    char jobTitle[15];
    int queryLoggerPid;
    int loggerPid;
};

struct QueryLoggerAndLoggerMsg
{
    long mtype;
    long senderPid;
    char operation[10];
};

char line[80], operation[15] = "";
int numberForked;
FILE *filePointer;
char *parentFile = "parent.txt";
key_t msgqid;
FILE *QueryFile;
int numOfDead;
int shmid2;
int nextKey = 1;
key_t msgqid;

int numberOfDBClients;
void shiftArrayOfQueuesOneStep(int row, int *queuesForEachRecord);
void sendAddRequestToManager( char *name, int salary);
//void receiveAnyMessageFromClients(struct msgStruct receivedMessage, struct recordStruct *DBData, int *nextKey, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid);
//void receiveAddMessageFromClient(struct msgStruct addMessage, struct recordStruct *DBData, int *nextKey);
void receiveAnyMessageFromClients(struct msgStruct receivedMessage, struct recordStruct *DBData, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid);
void receiveAddMessageFromClient(struct msgStruct addMessage, struct recordStruct *DBData);
int findFirstEmptyPlaceInQueue(int row, int *queuesForEachRecord);
void sendAcquireRequestToManager( int key);
void receiveAcquireMessageFromClient(int recordNum, int clientId, int *semaphoreLocks, int *queuesForEachRecord);
void receiveModifyMessageFromClient(struct msgStruct modifyMessage, struct recordStruct *DBData);
void sendModifyRequestToManager( int key, char *name, int salary);
void sendReleaseRequestToManager( int key);
void receiveReleaseMessageFromClient(int recordNum, int *semaphoreLocks, int *queuesForEachRecord);
char *QueryOperationType();
void whatOperationIsThis();
void hypridQuery(char *filterByName, char *filterOn, char *filterBySalary, int salaryFilter, struct recordStruct *DBData);
int RecieveInitialMessage( char RecievedJobTitle[], int *loggerPid);
void SendInitialMessage( char jobTitle[], int idofReciever, int queryLoggerPid, int loggerPid);

void greaterThanOrEqual(struct recordStruct *DBData, int filter);
void lessThanOrEqual(struct recordStruct *DBData, int filter);
void greaterThan(struct recordStruct *DBData, int filter);
void lessThan(struct recordStruct *DBData, int filter);
void equal(struct recordStruct *DBData, int filter);

void nameEquals(struct recordStruct *DBData, char filter[]);
void startsWith(struct recordStruct *DBData, char filter[]);

void nameEqualsAndSalaryEquals(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameEqualsAndSalaryLess(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameEqualsAndSalaryLessOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameEqualsAndSalaryGreater(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameEqualsAndSalaryGreaterOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter);

void nameStartsWithAndSalaryEquals(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameStartsWithAndSalaryLess(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameStartsWithAndSalaryLessOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameStartsWithAndSalaryGreater(struct recordStruct *DBData, char filter[], int salaryFilter);
void nameStartsWithAndSalaryGreaterOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter);

void receiveLoggerAcquire(int senderPid, int *empty, int *mutexLock, int *queuesForIndex, int indexProducer);
void receiveLoggerRelease(int *full, int *mutexLock, int *queuesForIndex, int *indexProducer);
void sendReleaseToLogger(int loggerPid);
int sendAcquireToLogger(int loggerPid);

void getTime(char *timeNow);

void DBManager(int *pidsOfForkedArray, int shmid);
void Client(int shmid, char *RecievedJobTitle, int queryLoggerPid, int loggerPid);
void QuerryLogger(int shmid);
void Logger(int shmid);

void handler(int signum);

void shiftArrayOfQueryQueuesOneStep(int *queuesForFile);
int findFirstEmptyPlaceInQueryQueue(int *queuesForFile);
void receiveQueryAcquire(int *queuesForFile, int *semaphoreLockOfFile, int senderPid);
void receiveQueryRelease(int *queuesForFile, int *semaphoreLockOfFile);

void sendAcquireToQueryLogger(int queryLoggerPid);
void sendReleaseToQueryLogger(int queryLoggerPid);

int main()
{
    // && DBData[record].key!=
    struct recordStruct Record[numberOfRecords] = {{0, "", 0}};
    struct loggerStruct LoggerSharedMemory[100] = {{"", ""}};

    /*    for(int recordnum=0; recordnum<numberOfRecords; recordnum ++)
   {
     Record[recordnum].key=-1;
   } */

    msgqid = msgget(IPC_PRIVATE, 0644);
    if (msgqid == -1)
    {
        perror("Error in creating message box");
        exit(-1);
    }

    key_t key = ftok("shmfile", 65);
    int shmid = shmget(key, sizeof(Record), 0666 | IPC_CREAT);

    key_t key2 = ftok("shmfile", 65);
    shmid2 = shmget(key2, sizeof(LoggerSharedMemory), 0666 | IPC_CREAT);

    int pid;

    printf("\nI am the parent, my pid = %d and my parent's pid = %d\n\n", getpid(), getppid());
    int parentPid = getpid();
    filePointer = fopen(parentFile, "r");

    if (filePointer == NULL)
    {
        printf("Could not open file %s", parentFile);
    }
    else
    {
        fscanf(filePointer, "%d", &numberOfDBClients);
    }

    numberForked = numberOfDBClients + 2; // Where 2 is the count of Logger and querry logger

    int pidsOfForkedArray[numberForked];
    char RecievedJobTitle[15];

    for (int forkingChild = 1; forkingChild <= numberForked; forkingChild++)
    {
        if (getpid() == parentPid)
        {
            pidsOfForkedArray[forkingChild - 1] = fork();
            int pid1 = pidsOfForkedArray[forkingChild - 1];
            if (pid1 == -1)
            {
                printf("Error in Forking");
            }
        }
    }

    if (getpid() == parentPid)
    {
        DBManager(pidsOfForkedArray, shmid);
    }
    else
    {
        int loggerPid;
        int queryLoggerPid = RecieveInitialMessage( RecievedJobTitle, &loggerPid);
        if (strstr(RecievedJobTitle, "client") != NULL)
            Client(shmid, RecievedJobTitle, queryLoggerPid, loggerPid);
        else if (strcmp(RecievedJobTitle, "querylogger") == 0)
            QuerryLogger(shmid);
        else if (strcmp(RecievedJobTitle, "logger") == 0)
            Logger(shmid);
    }
}

void DBManager(int *pidsOfForkedArray, int shmid)
{
    signal(SIGUSR1, handler);
    for (int child = 1; child <= numberForked; child++)
    {
        char jobTitle[20];
        if (child == (numberForked))
        {
            snprintf(jobTitle, sizeof jobTitle, "%s", "logger");
        }
        else if (child == (numberForked - 1))
        {
            snprintf(jobTitle, sizeof jobTitle, "%s", "querylogger");
        }
        else
        {
            snprintf(jobTitle, sizeof jobTitle, "%s%d", "client", child);
        }

        SendInitialMessage(jobTitle, pidsOfForkedArray[child - 1], pidsOfForkedArray[numberForked - 2], pidsOfForkedArray[numberForked - 1]);
    }

    struct recordStruct *DBData = (struct recordStruct *)shmat(shmid, (void *)0, 0);
    struct loggerStruct *LoggerSharedMemory = (struct loggerStruct *)shmat(shmid2, (void *)0, 0);

   // printf("Received data (Manager): %d,%s,%d\n", DBData[0].key, DBData[0].name, DBData[0].salary);
   // printf("Received data (Manager): %d,%s,%d\n", DBData[1].key, DBData[1].name, DBData[1].salary);
    //printf("Received data (Manager): %d,%s,%d\n", DBData[2].key, DBData[2].name, DBData[2].salary);

    // int nextKey = 0;
    int queuesForEachRecord[numberOfRecords * numberOfDBClients];
    int semaphoreLocks[numberOfRecords] = {[0 ... 999] = 1};

    struct msgStruct receivedMessage;
    int iShouldKillMyself = 0;

    while (1)
    {
        while (msgrcv(msgqid, &receivedMessage, sizeof(receivedMessage) - sizeof(long), getpid(), IPC_NOWAIT) == -1)
        {
            if (numOfDead == numberOfDBClients)
            {
                iShouldKillMyself = 1;
                break;
            }
            if (errno != ENOMSG)
                printf("Error receiving message: %s\n", strerror(errno));
        }

        if (iShouldKillMyself == 1)
            break;

        printf("\n Received a message\n");
        receiveAnyMessageFromClients(receivedMessage, DBData, semaphoreLocks, queuesForEachRecord, msgqid);

  //      printf("Received data (Manager): %d,%s,%d\n", DBData[0].key, DBData[0].name, DBData[0].salary);
    //    printf("Received data (Manager): %d,%s,%d\n", DBData[1].key, DBData[1].name, DBData[1].salary);
      //  printf("Received data (Manager): %d,%s,%d\n", DBData[2].key, DBData[2].name, DBData[2].salary);

        if (numOfDead == numberOfDBClients)
            break;
    }
    sleep(5);
 //   printf(" \n Pid of Querry logger %d\n", pidsOfForkedArray[numberForked - 2]);
    kill(pidsOfForkedArray[numberForked - 2], SIGINT);
    shmdt(DBData);
    shmdt(LoggerSharedMemory);
    shmctl(shmid, IPC_RMID, NULL);
    shmctl(shmid2, IPC_RMID, NULL);
}
void handler(int signum)
{
    numOfDead++;
    printf("\n Number of dead =%d\n", numOfDead);
}

void SendInitialMessage( char jobTitle[], int idofReciever, int queryLoggerPid, int loggerPid)
{
    int send_val;
    struct initialMsg message;

    message.mtype = idofReciever;
    strcpy(message.jobTitle, jobTitle);
    message.queryLoggerPid = queryLoggerPid;
    message.loggerPid = loggerPid;

    send_val = msgsnd(msgqid, &message, sizeof(message) - sizeof(long), !IPC_NOWAIT);

    if (send_val == -1)
        perror("Errror in send");
    else
        printf("\n\n My id is %d Message sent that contains jobtitle: %s , queryPid= %d , loggerPid= %d\n", getpid(), message.jobTitle, message.queryLoggerPid, message.loggerPid);
}

int RecieveInitialMessage(char RecievedJobTitle[], int *loggerPid)
{
    int rec_val;
    struct initialMsg message;

    rec_val = msgrcv(msgqid, &message, sizeof(message) - sizeof(long), getpid(), !IPC_NOWAIT);

    if (rec_val == -1)
        perror("Error in receive");
    else
    {
        printf("\n My id is %d Message received  contains jobtitle: %s , queryPid= %d \n", getpid(), message.jobTitle, message.queryLoggerPid);
        strcpy(RecievedJobTitle, message.jobTitle);
        (*loggerPid) = message.loggerPid;
        return message.queryLoggerPid;
    }
}

void QuerryLogger(int shmid)
{
    printf("Querry Logger alive %d\n", getpid());

    int queuesForFile[numberOfDBClients];
    int semaphoreLockOfFile = 1;
    for (int i = 0; i < numberOfDBClients; i++)
    {
        queuesForFile[i] = 0;
    }

    struct QueryLoggerAndLoggerMsg receivedMessage;

    while (1)
    {
        while (msgrcv(msgqid, &receivedMessage, sizeof(receivedMessage) - sizeof(long), getpid(), IPC_NOWAIT) == -1)
        {
            if (errno != ENOMSG)
                printf("Error receiving message: %s\n", strerror(errno));
        }

        printf("\n \n Querry Logger received a message\n");
        if (strcmp(receivedMessage.operation, "_acquire") == 0)
            receiveQueryAcquire(queuesForFile, &semaphoreLockOfFile, receivedMessage.senderPid);
        else
            receiveQueryRelease(queuesForFile, &semaphoreLockOfFile);
    }
}

void Logger(int shmid)
{
    struct loggerStruct *LoggerSharedMemory = (struct loggerStruct *)shmat(shmid2, (void *)0, 0);
    FILE *LoggerFile;
    LoggerFile = fopen("LoggerFile.txt", "a");
    int full = 0;
    int empty = 100;
    int mutexLock = 1;
    int queuesForIndex[numberOfDBClients];
    for (int i = 0; i < numberOfDBClients; i++)
    {
        queuesForIndex[i] = 0;
    }

    int indexProducer = 0;
    int indexConsumer = 0;

    struct QueryLoggerAndLoggerMsg receivedMessage;

    for (int i = 0; i < 1000000; i++)
    {
        if (msgrcv(msgqid, &receivedMessage, sizeof(receivedMessage) - sizeof(long), getpid(), IPC_NOWAIT) != -1)
        {
            printf(" \n Logger received a message\n");
            if (strcmp(receivedMessage.operation, "_acquire") == 0)
                receiveLoggerAcquire(receivedMessage.senderPid, &empty, &mutexLock, queuesForIndex, indexProducer);
            else
                receiveLoggerRelease(&full, &mutexLock, queuesForIndex, &indexProducer);
        }

        if (full != 0)
        {
            // printf("  Should consume here ,indexConsumer = %d\n",indexConsumer);
            printf("\n\n %s %s \n\n", LoggerSharedMemory[indexConsumer].time, LoggerSharedMemory[indexConsumer].message);

            fprintf(LoggerFile, "At %s  %s \n\n", LoggerSharedMemory[indexConsumer].time, LoggerSharedMemory[indexConsumer].message);

            indexConsumer++;
            if (indexConsumer >= 100)
                indexConsumer = 0;
            full--;
            empty++;
        }
        else
        {
            // raise(SIGSTOP);
        }
    }

    fclose(LoggerFile);
    shmdt(LoggerSharedMemory);
}

void receiveLoggerAcquire(int senderPid, int *empty, int *mutexLock, int *queuesForIndex, int indexProducer)
{
    printf("\n Inside logger Acquire\n");
    struct QueryLoggerAndLoggerMsg responseToAcquireMessage;
    responseToAcquireMessage.mtype = senderPid;

    if ((*mutexLock) == 0 | (*empty) == 0)
    {
        int emptySlotInQueue = findFirstEmptyPlaceInQueryQueue(queuesForIndex);
        queuesForIndex[emptySlotInQueue] = senderPid;
        strcpy(responseToAcquireMessage.operation, "denied");
        printf("\n queuesForIndex[emptySlotInQueue]: %d, emptySlotInQueue %d\n", emptySlotInQueue, queuesForIndex[0]);
    }
    else
    {
        (*empty)--;
        (*mutexLock) = 0;
        strcpy(responseToAcquireMessage.operation, "granted");
        responseToAcquireMessage.senderPid = indexProducer;
    }

    int send_val = msgsnd(msgqid, &responseToAcquireMessage, sizeof(responseToAcquireMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Errror in send");
    else
    {
        printf("Sent response to logger acquire message to pid %d \n", senderPid);
        printf("Acquire response message sent (Logger)= %s, %ld, semaphoreLockOfFile: %d\n", responseToAcquireMessage.operation, responseToAcquireMessage.mtype, (*mutexLock));
    }
    printf("In Acquire mutexLock: %d \n", (*mutexLock));
}

void receiveLoggerRelease(int *full, int *mutexLock, int *queuesForIndex, int *indexProducer)
{
    (*mutexLock) = 1;
    (*full)++;
    (*indexProducer)++;
    if ((*indexProducer) >= 100)
        (*indexProducer) = 0;
    int clientToBeReleased = queuesForIndex[0];
    if (clientToBeReleased != 0)
    {
        shiftArrayOfQueryQueuesOneStep(queuesForIndex);
        kill(clientToBeReleased, SIGCONT);
    }
    printf("In release mutexLock: %d \n", (*mutexLock));
}

int sendAcquireToLogger(int loggerPid)
{
    for (int i = 0; i < 2; i++)
    {
        int send_val;
        struct QueryLoggerAndLoggerMsg acquireMessage;
        acquireMessage.mtype = loggerPid;
        acquireMessage.senderPid = getpid();
        strcpy(acquireMessage.operation, "_acquire");

        send_val = msgsnd(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), !IPC_NOWAIT);
        if (send_val == -1)
            perror("Error in send");
        else
        {
            printf("\n Sent acquire message to logger whose pid is  %d \n", loggerPid);
            printf("\n Acquire message sent (Client) = %ld, %s mypid= %d \n", acquireMessage.senderPid, acquireMessage.operation, getpid());
        }
        // kill(loggerPid,SIGCONT);

        if (msgrcv(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), getpid(), !IPC_NOWAIT) == -1)
        {
            perror("Error in receiving response to acquire request from logger");
            return 0;
        }
        else
        {
            printf("\n Received acquire response from logger  %s\n", acquireMessage.operation);
            if (strcmp(acquireMessage.operation, "denied") == 0)
            {
                printf("\n I will sleep now due to logger my pid: %d \n", getpid());
                raise(SIGSTOP);
                printf("\n I woke up now due to logger\n");
            }
            else
            {
                printf(" \n IndexProducer receied(CLient)  %ld \n", acquireMessage.senderPid);
                int indexProducer = acquireMessage.senderPid;
                return indexProducer;
            }
        }
    }
}

void sendReleaseToLogger(int loggerPid)
{
    int send_val;
    struct QueryLoggerAndLoggerMsg releaseMessage;

    releaseMessage.mtype = loggerPid;
    releaseMessage.senderPid = getpid();
    strcpy(releaseMessage.operation, "_release");

    send_val = msgsnd(msgqid, &releaseMessage, sizeof(releaseMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("\n \n Sent release message to Logger\n");
        printf("\n \n Release message sent (Client) = %ld, %s \n", releaseMessage.senderPid, releaseMessage.operation);
    }
    kill(loggerPid, SIGCONT);
}

void Client(int shmid, char *RecievedJobTitle, int queryLoggerPid, int loggerPid)
{
    struct recordStruct *DBData = (struct recordStruct *)shmat(shmid, (void *)0, 0);
    struct loggerStruct *LoggerSharedMemory = (struct loggerStruct *)shmat(shmid2, (void *)0, 0);

    FILE *filePointer;
    char MyFile[20];
    snprintf(MyFile, sizeof MyFile, "%s%s", RecievedJobTitle, ".txt");
    char *fileName = MyFile;
    filePointer = fopen(fileName, "r");
    if (filePointer == NULL)
    {
        printf("Could not open file %s", fileName);
    }
    else
    {
        // for(int i=0 ; i<3 ; i++)
        // {
        //     int indexProducer=sendAcquireToLogger(loggerPid);
        //     sleep(3);
        //     printf("****Should write here Returned indexProducer %d", indexProducer);
        //     sendReleaseToLogger(loggerPid);

        // }

        while (fgets(line, sizeof(line), filePointer) != NULL)
        {
            sscanf(line, "%s", operation);
            whatOperationIsThis(DBData, queryLoggerPid, loggerPid, LoggerSharedMemory);
            sleep(2);
        }
    }

    fclose(filePointer);
    shmdt(DBData);
    shmdt(LoggerSharedMemory);
    printf("pid  %d will die", getpid());
    sleep(4);
    kill(getppid(), SIGUSR1);
    exit(0);
}

void getTime(char *timeNow)
{
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    strftime(timeNow, 30, "%c", tm);
}

void whatOperationIsThis(struct recordStruct *DBData, int queryLoggerPid, int loggerPid, struct loggerStruct *LoggerSharedMemory)
{
    if (strcmp(operation, "add") == 0)
    {
        char name[15];
        int salary;
        sscanf(line, "%s%s%d", operation, name, &salary);
        sendAddRequestToManager( name, salary);

        //uncomment
        //      int indexProducer=sendAcquireToLogger(loggerPid);

        // printf("\n\n %d \n\n", indexProducer);
        char messageToLogger[200];
        char timeNow[30];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request an add request to add name: %s  and salary: %d to database......", getpid(), name, salary);
        // printf("\n\n %s \n\n", messageToLogger);
        getTime(timeNow);
        // printf("\n\n %s \n\n", messageToLogger);

        // Uncomment
        //      strcpy(LoggerSharedMemory[indexProducer].message,messageToLogger);
        //    strcpy(LoggerSharedMemory[indexProducer].time,timeNow);
        //   printf("\n\n %s %s \n\n",LoggerSharedMemory[indexProducer].time,LoggerSharedMemory[indexProducer].message);
        // printf("\n\n %s \n\n", messageToLogger);
        //   sendReleaseToLogger(loggerPid);
    }
    else if (strcmp(operation, "modify") == 0)
    {
        char operationType[10];
        char sign[10]; // sign is just a dummy variable that is only used for correct Formatting
        int key;
        int modificationValue;
        sscanf(line, "%s%d%s%d", operation, &key, sign, &modificationValue);
        char *Mode = QueryOperationType();
        sendModifyRequestToManager(key, Mode, modificationValue);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request a modify request to %s key number: %d by %d ......", getpid(), Mode, key, modificationValue);
        printf("%s", messageToLogger);
    }
    else if (strcmp(operation, "release") == 0)
    {
        int key;
        sscanf(line, "%s%d", operation, &key);
        sendReleaseRequestToManager( key);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request a release request to key number: %d......", getpid(), key);
        printf("%s", messageToLogger);
    }
    else if (strcmp(operation, "acquire") == 0)
    {
        int key;
        sscanf(line, "%s%d", operation, &key);
        sendAcquireRequestToManager( key);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request an acquire request to key number: %d  ......", getpid(), key);
        printf("%s", messageToLogger);
    }
    else if (strcmp(operation, "query") == 0)
    {
        sendAcquireToQueryLogger(queryLoggerPid);
        printf("queryLoggerPid: %d", queryLoggerPid);
        QueryFile = fopen("QueryFile.txt", "a");
        char QueryType[10];
        char filterOn[10];
        char filterBy[10];

        sscanf(line, "%s%s", operation, QueryType);
        if (strcmp(QueryType, "name") == 0)
        {
            sscanf(line, "%s%s%s%s", operation, QueryType, filterBy, filterOn);
            fprintf(QueryFile, "\n   My Pid: %d, %s %s %s %s  \n", getpid(), operation, QueryType, filterBy, filterOn);

            if (strcmp(filterBy, "startsWith") == 0)
                startsWith(DBData, filterOn);
            else
                nameEquals(DBData, filterOn);

            char messageToLogger[200];
            snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I created a query to get names that are %s : 	 %s ......", getpid(), filterBy, filterOn);
            printf("%s", messageToLogger);
        }
        else if (strcmp(QueryType, "salary") == 0)
        {
            int salaryFilter;
            sscanf(line, "%s%s%s%d", operation, QueryType, filterBy, &salaryFilter);
            char *filterBy = QueryOperationType();
            fprintf(QueryFile, "\n   My Pid: %d, %s %s %s %d  \n", getpid(), operation, QueryType, filterBy, salaryFilter);

            if (strcmp(filterBy, "_greaterOrEqual") == 0)
                greaterThanOrEqual(DBData, salaryFilter);
            else if (strcmp(filterBy, "_lessOrEqual") == 0)
                lessThanOrEqual(DBData, salaryFilter);
            else if (strcmp(filterBy, "_greater") == 0)
                greaterThan(DBData, salaryFilter);
            else if (strcmp(filterBy, "_less") == 0)
                lessThan(DBData, salaryFilter);
            else if (strcmp(filterBy, "_equal") == 0)
                equal(DBData, salaryFilter);

            char messageToLogger[200];
            snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I created a query to get salaries that are %s : %d", getpid(), filterBy, salaryFilter);
            printf("%s", messageToLogger);
        }
        else if (strcmp(QueryType, "hybrid") == 0)
        {
            fprintf(QueryFile, "   My Pid: %d query hybrid \n", getpid());
            char filterByName[10];
            int salaryFilter;
            char nameDummyVariable[10];
            char salaryDummyVariable[10];

            sscanf(line, "%s%s%s%s%s%s%s%d", operation, QueryType, nameDummyVariable, filterByName, filterOn, salaryDummyVariable, filterBy, &salaryFilter);
            fprintf(QueryFile, " %s %s %s %s %s %s %s %d \n", operation, QueryType, nameDummyVariable, filterByName, filterOn, salaryDummyVariable, filterBy, salaryFilter);

            printf("%s %s %s %s", operation, QueryType, filterByName, filterOn);

            char *filterBySalary = QueryOperationType();

            char messageToLogger[200];
            snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I created a hybrid query to get names that %s : %s 			and salaries %s : %d..", getpid(), filterByName, filterOn, filterBySalary, salaryFilter);
            printf("%s", messageToLogger);

            hypridQuery(filterByName, filterOn, filterBySalary, salaryFilter, DBData);
        }
        fclose(QueryFile);
        sendReleaseToQueryLogger(queryLoggerPid);
    }
}

char *QueryOperationType()
{
    if (strstr(line, "+") != NULL)
    {
        return "_add";
    }
    else if (strstr(line, "-") != NULL)
    {
        return "_subtract";
    }
    else if (strstr(line, ">=") != NULL)
    {
        return "_greaterOrEqual";
    }
    else if (strstr(line, "<=") != NULL)
    {
        return "_lessOrEqual";
    }
    else if (strstr(line, ">") != NULL)
    {
        return "_greater";
    }
    else if (strstr(line, "<") != NULL)
    {
        return "_less";
    }
    else if (strstr(line, "=") != NULL)
    {
        return "_equal";
    }
}

void hypridQuery(char *filterByName, char *filterOn, char *filterBySalary, int salaryFilter, struct recordStruct *DBData)
{
    if (strcmp(filterByName, "startsWith") == 0)
    {
        if (strcmp(filterBySalary, "_greaterOrEqual") == 0)
            nameStartsWithAndSalaryGreaterOrEqual(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_lessOrEqual") == 0)
            nameStartsWithAndSalaryLessOrEqual(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_greater") == 0)
            nameStartsWithAndSalaryGreater(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_less") == 0)
            nameStartsWithAndSalaryLess(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_equal") == 0)
            nameStartsWithAndSalaryEquals(DBData, filterOn, salaryFilter);
    }
    else
    {
        if (strcmp(filterBySalary, "_greaterOrEqual") == 0)
            nameEqualsAndSalaryGreaterOrEqual(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_lessOrEqual") == 0)
            nameEqualsAndSalaryLessOrEqual(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_greater") == 0)
            nameEqualsAndSalaryGreater(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_less") == 0)
            nameEqualsAndSalaryLess(DBData, filterOn, salaryFilter);
        else if (strcmp(filterBySalary, "_equal") == 0)
            nameEqualsAndSalaryEquals(DBData, filterOn, salaryFilter);
    }
}

////////////////////////////////////////////////////////////////Client Requests////////////////////////////////////////////////////////////////////////////

void sendAddRequestToManager( char *name, int salary)
{
    int send_val;
    struct msgStruct addMessage;

    addMessage.mtype = getppid();
    addMessage.Record.key = -1;
    strcpy(addMessage.Record.name, name);
    addMessage.Record.salary = salary;
    addMessage.senderPid = getpid();
    send_val = msgsnd(msgqid, &addMessage, sizeof(addMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Errror in send");
    else
    {
        printf("Sent add message\n");
        printf("Add message sent (Client) = %d, %s, %d\n", addMessage.Record.key, addMessage.Record.name, addMessage.Record.salary);
    }

    if (msgrcv(msgqid, &addMessage, sizeof(addMessage) - sizeof(long), getpid(), !IPC_NOWAIT) == -1)
    {
        perror("Error in receiving response to add request");
        return;
    }
    else
    {
        //printf("Received Add response  %s\n", addMessage.operation);
        if (addMessage.Record.key == -1)
        {
            printf(" Add request Rejected by manager\n");
        }
        else
        {
            printf("\n\n\n Add request confirmed by manager and record was added to key number %d\n", addMessage.Record.key);
        }
    }
}

void sendAcquireRequestToManager(int key)
{
    for (int i = 0; i < 2; i++)
    {
        int send_val;
        struct msgStruct acquireMessage;

        acquireMessage.mtype = getppid();
        acquireMessage.Record.key = key;
        strcpy(acquireMessage.Record.name, "_acquire");
        acquireMessage.Record.salary = getpid();

        send_val = msgsnd(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), !IPC_NOWAIT);
        if (send_val == -1)
            perror("Error in send");
        else
        {
            printf("Sent acquire message\n");
            printf("Acquire message sent (Client) = %d, %s, %d\n", acquireMessage.Record.key, acquireMessage.Record.name, acquireMessage.Record.salary);
        }

        if (msgrcv(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), getpid(), !IPC_NOWAIT) == -1)
        {
            perror("Error in receiving response to acquire request");
            return;
        }
        else
        {
            int responseToAcquireRequest = acquireMessage.Record.key;
            printf("Received acquire response key= %d, %s, %d\n", acquireMessage.Record.key, acquireMessage.Record.name, acquireMessage.Record.salary);
            if (responseToAcquireRequest == 0)
            {
                printf("I will sleep now\n");
                raise(SIGSTOP);
                printf("I woke up now\n");
            }
            else
                return;
        }
    }
}

void sendModifyRequestToManager( int key, char *name, int salary)
{
    int send_val;
    struct msgStruct modifyMessage;

    modifyMessage.mtype = getppid();
    modifyMessage.senderPid = getpid();
    modifyMessage.Record.key = key;
    strcpy(modifyMessage.Record.name, name);
    modifyMessage.Record.salary = salary;

    send_val = msgsnd(msgqid, &modifyMessage, sizeof(modifyMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("Sent modify message\n");
     //   printf("Modify message sent (Client) = %d, %s, %d\n", modifyMessage.Record.key, modifyMessage.Record.name, modifyMessage.Record.salary);
    }
}

void sendReleaseRequestToManager( int key)
{
    int send_val;
    struct msgStruct releaseMessage;

    releaseMessage.mtype = getppid();
    releaseMessage.Record.key = key;
    strcpy(releaseMessage.Record.name, "_release");

    send_val = msgsnd(msgqid, &releaseMessage, sizeof(releaseMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("Sent release message\n");
        printf("Release message sent (Client) = %d, %s, %d\n", releaseMessage.Record.key, releaseMessage.Record.name, releaseMessage.Record.salary);
    }
}

void sendAcquireToQueryLogger(int queryLoggerPid)
{
    for (int i = 0; i < 2; i++)
    {
        int send_val;
        struct QueryLoggerAndLoggerMsg acquireMessage;
        acquireMessage.mtype = queryLoggerPid;
        acquireMessage.senderPid = getpid();
        strcpy(acquireMessage.operation, "_acquire");

        send_val = msgsnd(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), !IPC_NOWAIT);
        if (send_val == -1)
            perror("Error in send");
        else
        {
            printf("Sent acquire message to query logger whose pid is  %d \n", queryLoggerPid);
            printf("Acquire message sent (Client) = %ld, %s mypid= %d \n", acquireMessage.senderPid, acquireMessage.operation, getpid());
        }

        if (msgrcv(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), getpid(), !IPC_NOWAIT) == -1)
        {
            perror("Error in receiving response to acquire request");
            return;
        }
        else
        {

            printf("Received acquire response  %s\n", acquireMessage.operation);
            if (strcmp(acquireMessage.operation, "denied") == 0)
            {
                printf("I will sleep now\n");
                raise(SIGSTOP);
                printf("I woke up now\n");
            }
            else
                return;
        }
    }
}

void sendReleaseToQueryLogger(int queryLoggerPid)
{
    int send_val;
    struct QueryLoggerAndLoggerMsg releaseMessage;

    releaseMessage.mtype = queryLoggerPid;
    releaseMessage.senderPid = getpid();
    strcpy(releaseMessage.operation, "_release");

    send_val = msgsnd(msgqid, &releaseMessage, sizeof(releaseMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("Sent release message to Querry Logger\n");
        printf("Release message sent (Client) = %ld, %s \n", releaseMessage.senderPid, releaseMessage.operation);
    }
}

////////////////////////////////////////////////////////////DBManager Requests///////////////////////////////////////////////////////////////////

void receiveAnyMessageFromClients(struct msgStruct receivedMessage, struct recordStruct *DBData, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid)
//void receiveAnyMessageFromClients(struct msgStruct receivedMessage, struct recordStruct *DBData, int *nextKey, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid)
{
    int recordNum = receivedMessage.Record.key;
    if (receivedMessage.Record.key == -1)
        receiveAddMessageFromClient(receivedMessage, DBData);
    //receiveAddMessageFromClient(receivedMessage, DBData, nextKey);
    if (strcmp(receivedMessage.Record.name, "_acquire") == 0)
    {
        int clientId = receivedMessage.Record.salary;
        receiveAcquireMessageFromClient(recordNum, clientId, semaphoreLocks, queuesForEachRecord);
    }
    if (strcmp(receivedMessage.Record.name, "_release") == 0)
    {
        receiveReleaseMessageFromClient(recordNum, semaphoreLocks, queuesForEachRecord);
    }
    if (strcmp(receivedMessage.Record.name, "_add") == 0 || strcmp(receivedMessage.Record.name, "_subtract") == 0)
        receiveModifyMessageFromClient(receivedMessage, DBData);
}

// void receiveAddMessageFromClient(struct msgStruct addMessage, struct recordStruct *DBData, int *nextKey)
void receiveAddMessageFromClient(struct msgStruct addMessage, struct recordStruct *DBData)
{
    //if (*nextKey==1000)
    if (nextKey > numberOfRecords)
    {
        //This means memory is full
        addMessage.Record.key = -1;

        char timeNow[30];
        getTime(timeNow);
        printf("\n %s\n", timeNow);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an add request from %d .Rejected as memory is full ", getpid(), addMessage.senderPid);
        printf("\n %s", messageToLogger);
    }
    else
    {
        //    addMessage.Record.key = *nextKey;
        // DBData[*nextKey] = addMessage.Record;
        // (*nextKey)++;

        addMessage.Record.key = nextKey;
        DBData[nextKey] = addMessage.Record;
        (nextKey)++;

        char timeNow[30];
        getTime(timeNow);
        printf("\n %s\n", timeNow);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an add request from %d .Approved as : key %d , name  %s n salary %d", getpid(), addMessage.senderPid, addMessage.Record.key, addMessage.Record.name, addMessage.Record.salary);
        printf("\n %s", messageToLogger);
    }
    addMessage.mtype = addMessage.senderPid;
    addMessage.senderPid = getppid();
    int send_val = msgsnd(msgqid, &addMessage, sizeof(addMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("\n Sent Add response to client \n");
        printf("\n Add response message sent (Manager)= %d, %s \n", addMessage.Record.key, addMessage.Record.name);
    }
}

void receiveAcquireMessageFromClient(int recordNum, int clientId, int *semaphoreLocks, int *queuesForEachRecord)
{
    int send_val;
    struct msgStruct responseToAcquireMessage;
    responseToAcquireMessage.mtype = clientId;
    printf("Inside acguire Semaphore[0]=%d\n", semaphoreLocks[0]);
    printf("Semaphore[1]=%d\n", semaphoreLocks[1]);
    printf("Semaphore[2]=%d\n", semaphoreLocks[2]);

    if (semaphoreLocks[recordNum] == 1)
    {
        if (recordNum < nextKey)
        {
            semaphoreLocks[recordNum] = 0;
            responseToAcquireMessage.Record.key = 1; // access granted

            char timeNow[30];
            getTime(timeNow);
            printf("\n %s\n", timeNow);

            char messageToLogger[200];
            snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an  acquire request from %d to acquire key %d , and request approved", getpid(), clientId, recordNum);
            printf(" \n %s", messageToLogger);
        }
        else
        {
            responseToAcquireMessage.Record.key = -1; //just a signal that access is not granted

            char timeNow[30];
            getTime(timeNow);
            printf("\n %s\n", timeNow);

            char messageToLogger[200];
            snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an  acquire request from %d to acquire key %d ,Request denied this key is not available", getpid(), clientId, recordNum);
            printf(" \n %s", messageToLogger);
        }
    }
    else
    {
        int emptySlotInQueue = findFirstEmptyPlaceInQueue(recordNum, queuesForEachRecord);
        queuesForEachRecord[recordNum * numberOfDBClients + emptySlotInQueue] = clientId;
        responseToAcquireMessage.Record.key = 0; // access denied

        char timeNow[30];
        getTime(timeNow);
        printf("\n %s\n", timeNow);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an acquire request from %d to key %d , and request is on queue", getpid(), clientId, recordNum);
        printf(" \n  %s \n", messageToLogger);
    }

    send_val = msgsnd(msgqid, &responseToAcquireMessage, sizeof(responseToAcquireMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("Sent response to acquire message\n");
        printf("Acquire response message sent (Manager)= %d, %s, %d\n", responseToAcquireMessage.Record.key, responseToAcquireMessage.Record.name, responseToAcquireMessage.Record.salary);
    }
}

void receiveModifyMessageFromClient(struct msgStruct modifyMessage, struct recordStruct *DBData)
{
    if (modifyMessage.Record.key < nextKey)
    {
            if (strcmp(modifyMessage.Record.name, "_add") == 0)
            {
                DBData[modifyMessage.Record.key].salary += modifyMessage.Record.salary;

                char timeNow[30];
                getTime(timeNow);
                printf("%s\n", timeNow);

                char messageToLogger[200];
                snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a modify request from %d to add key %d by %d ", getpid(), modifyMessage.senderPid, modifyMessage.Record.key, modifyMessage.Record.salary);
                printf("%s \n", messageToLogger);
            }
            else
            {
                DBData[modifyMessage.Record.key].salary -= modifyMessage.Record.salary;
                char timeNow[30];
                getTime(timeNow);
                printf("%s\n", timeNow);

                char messageToLogger[200];
                snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a modify request from %d to minus key %d by %d ", getpid(), modifyMessage.senderPid, modifyMessage.Record.key, modifyMessage.Record.salary);
                printf("%s \n", messageToLogger);
            }
        }
}

void receiveReleaseMessageFromClient(int recordNum, int *semaphoreLocks, int *queuesForEachRecord)
{
    if (recordNum < nextKey)
    {
        if (semaphoreLocks[recordNum] == 0)
        {
            semaphoreLocks[recordNum] = 1;
            int clientToBeReleased = queuesForEachRecord[recordNum * numberOfDBClients];
            if (clientToBeReleased != 0)
            {
                shiftArrayOfQueuesOneStep(recordNum, queuesForEachRecord);
                kill(clientToBeReleased, SIGCONT);

                char timeNow[30];
                getTime(timeNow);
                printf("%s \n", timeNow);

                char messageToLogger[200];
                snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a release request from %d to key %d , and request is on queue", getpid(), clientToBeReleased, recordNum);
                printf("%s \n", messageToLogger);
            }
            char timeNow[30];
            getTime(timeNow);
            printf("%s\n", timeNow);

            char messageToLogger[200];
            snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a release request from %d to key %d.Acquire request is still on queue", getpid(), clientToBeReleased, recordNum);
            printf("%s \n", messageToLogger);

            printf("Inside release Semaphore[0]=%d\n", semaphoreLocks[0]);
            printf("Semaphore[1]=%d\n", semaphoreLocks[1]);
            printf("Semaphore[2]=%d\n", semaphoreLocks[2]);
        }
    }
}

void shiftArrayOfQueuesOneStep(int row, int *queuesForEachRecord)
{
    for (int step = 0; step < numberOfDBClients; step++)
    {
        queuesForEachRecord[row * numberOfDBClients + step] = queuesForEachRecord[row * numberOfDBClients + step + 1];
    }
    queuesForEachRecord[row * numberOfDBClients + numberOfDBClients - 1] = 0;
}

int findFirstEmptyPlaceInQueue(int row, int *queuesForEachRecord)
{
    for (int step = 0; step < numberOfDBClients; step++)
    {
        if (queuesForEachRecord[row * numberOfDBClients + step] == 0)
            return step;
    }
}

/////////////////////////////////////////////////////////////Query Requests//////////////////////////////////////////////////////////////////////////

///****************************SALARY QUEREISSSS ***********************************//
void greaterThanOrEqual(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (DBData[record].salary >= filter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void lessThanOrEqual(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (DBData[record].salary <= filter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void greaterThan(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (DBData[record].salary > filter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void lessThan(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (DBData[record].salary < filter && DBData[record].key != 0)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void equal(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (DBData[record].salary == filter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

////// ******************************************* NAME QUERIES **************************////////////////
void nameEquals(struct recordStruct *DBData, char filter[])
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strcmp(DBData[record].name, filter) == 0)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void startsWith(struct recordStruct *DBData, char filter[])
{
    printf("here\n"); //
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strncmp(DBData[record].name, filter, strlen(filter)) == 0)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

////////////********************************* HYBRID QUERIESSS   *******************************************////////////////
////////////********************************* Name Equals and salary query *********************************///////////////
void nameEqualsAndSalaryEquals(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strcmp(DBData[record].name, filter) == 0 && DBData[record].salary == salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}
void nameEqualsAndSalaryLess(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strcmp(DBData[record].name, filter) == 0 && DBData[record].salary < salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void nameEqualsAndSalaryLessOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strcmp(DBData[record].name, filter) == 0 && DBData[record].salary <= salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void nameEqualsAndSalaryGreater(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strcmp(DBData[record].name, filter) == 0 && DBData[record].salary > salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void nameEqualsAndSalaryGreaterOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strcmp(DBData[record].name, filter) == 0 && DBData[record].salary >= salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

//************************   		Name Starts With and salaries queries 		*************************///

void nameStartsWithAndSalaryEquals(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strncmp(DBData[record].name, filter, strlen(filter)) == 0 && DBData[record].salary == salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}
void nameStartsWithAndSalaryLess(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strncmp(DBData[record].name, filter, strlen(filter)) == 0 && DBData[record].salary < salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void nameStartsWithAndSalaryLessOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strncmp(DBData[record].name, filter, strlen(filter)) == 0 && DBData[record].salary <= salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void nameStartsWithAndSalaryGreater(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strncmp(DBData[record].name, filter, strlen(filter)) == 0 && DBData[record].salary > salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

void nameStartsWithAndSalaryGreaterOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < numberOfRecords; record++)
    {
        if (strncmp(DBData[record].name, filter, strlen(filter)) == 0 && DBData[record].salary >= salaryFilter)
            fprintf(QueryFile, "Key: %d  Name: %s Salary: %d \n", DBData[record].key, DBData[record].name, DBData[record].salary);
    }
}

//////////////////////////////////////////////////////////////Query Logger Requests/////////////////////////////////////////////////////////////////////

void receiveQueryAcquire(int *queuesForFile, int *semaphoreLockOfFile, int senderPid)
{
   // printf("Inside Querry logger Acquire\n");
    struct QueryLoggerAndLoggerMsg responseToAcquireMessage;
    responseToAcquireMessage.mtype = senderPid;

    if ((*semaphoreLockOfFile) == 1)
    {
        (*semaphoreLockOfFile) = 0;
        strcpy(responseToAcquireMessage.operation, "granted");
    }
    else
    {
        int emptySlotInQueue = findFirstEmptyPlaceInQueryQueue(queuesForFile);
        queuesForFile[emptySlotInQueue] = senderPid;
    //    printf("queuesForFile[emptySlotInQueue]: %d, emptySlotInQueue %d\n", emptySlotInQueue, queuesForFile[0]);

        strcpy(responseToAcquireMessage.operation, "denied");
    }

    int send_val = msgsnd(msgqid, &responseToAcquireMessage, sizeof(responseToAcquireMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Errror in send");
    else
    {
    //    printf("Sent response to querry acquire message to pid %d \n", senderPid);
        printf("Acquire response message sent (Querry Logger)= %s, %ld, semaphoreLockOfFile: %d\n", responseToAcquireMessage.operation, responseToAcquireMessage.mtype, (*semaphoreLockOfFile));
    }
    printf("In Acquire semaphoreLockOfFile: %d \n", (*semaphoreLockOfFile));
    // printf("queuesForFile[emptySlotInQueue]: %d \n",queuesForFile[0]);
}

void receiveQueryRelease(int *queuesForFile, int *semaphoreLockOfFile)
{
    (*semaphoreLockOfFile) = 1;
    int clientToBeReleased = queuesForFile[0];
    if (clientToBeReleased != 0)
    {
        shiftArrayOfQueryQueuesOneStep(queuesForFile);
        kill(clientToBeReleased, SIGCONT);
    }
    printf("In release semaphoreLockOfFile: %d \n", (*semaphoreLockOfFile));
}

void shiftArrayOfQueryQueuesOneStep(int *queuesForFile)
{
    for (int step = 0; step < numberOfDBClients; step++)
    {
        queuesForFile[step] = queuesForFile[step + 1];
    }
    queuesForFile[numberOfDBClients - 1] = 0;
}

int findFirstEmptyPlaceInQueryQueue(int *queuesForFile)
{
    for (int step = 0; step < numberOfDBClients; step++)
    {
        if (queuesForFile[step] == 0)
            return step;
    }
}
