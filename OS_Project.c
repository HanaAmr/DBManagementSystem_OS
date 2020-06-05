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
    char message[200];
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
char *fileName ="parent.txt";
key_t msgqid;
FILE * QueryFile;
FILE *LoggerFile;
int numOfDead;
int shmid2;
int loggerKillYourself=0;
int wakingUpBit=0;



int numberOfDBClients;
void shiftArrayOfQueuesOneStep(int row, int *queuesForEachRecord);
void sendAddRequestToManager(key_t msgqid, char *name, int salary);
void receiveAnyMessageFromClients(struct msgStruct receivedMessage, struct recordStruct *DBData, int *nextKey, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid,struct loggerStruct * LoggerSharedMemory, int loggerPid);
void receiveAddMessageFromClient(struct msgStruct addMessage, struct recordStruct *DBData, int *nextKey,struct loggerStruct * LoggerSharedMemory, int loggerPid);
int findFirstEmptyPlaceInQueue(int row, int *queuesForEachRecord);
void sendAcquireRequestToManager(key_t msgqid, int key);
void receiveAcquireMessageFromClient(int recordNum, int clientId, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid,struct loggerStruct * LoggerSharedMemory, int loggerPid);
void receiveModifyMessageFromClient(struct msgStruct modifyMessage, struct recordStruct *DBData,struct loggerStruct * LoggerSharedMemory, int loggerPid);
void sendModifyRequestToManager(key_t msgqid, int key, char *name, int salary);
void sendReleaseRequestToManager(key_t msgqid, int key);
void receiveReleaseMessageFromClient(int recordNum, int *semaphoreLocks, int *queuesForEachRecord,struct loggerStruct * LoggerSharedMemory, int loggerPid);
char *QueryOperationType();
void whatOperationIsThis();
void hypridQuery(char* filterByName,char* filterOn,char* filterBySalary,int salaryFilter,struct recordStruct *DBData);
int RecieveInitialMessage(key_t msgqid, char RecievedJobTitle[] , int* loggerPid);
void SendInitialMessage(key_t msgqid, char jobTitle[], int idofReciever, int queryLoggerPid , int loggerPid);

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

void receiveLoggerAcquire(int senderPid ,int* empty, int* mutexLock , int* queuesForIndex , int indexProducer);
void receiveLoggerRelease(int* full, int* mutexLock , int* queuesForIndex , int* indexProducer);
void sendReleaseToLogger(int loggerPid);
int sendAcquireToLogger(int loggerPid);

void getTime(char* timeNow);
void writeInLoggerSharedMemory(struct loggerStruct *LoggerSharedMemory,char* messageToLogger, int loggerPid);

void DBManager(int* pidsOfForkedArray, int shmid);
void Client(int shmid,char* RecievedJobTitle,int queryLoggerPid, int loggerPid);
void QuerryLogger(int shmid);
void Logger(int shmid);

void handler(int signum);
void handler2(int signum);
void handler3(int signum);

void shiftArrayOfQueryQueuesOneStep(int *queuesForFile);
int findFirstEmptyPlaceInQueryQueue(int *queuesForFile);
void receiveQueryAcquire(int * queuesForFile,int* semaphoreLockOfFile,int senderPid);
void receiveQueryRelease(int * queuesForFile,int* semaphoreLockOfFile);

void sendAcquireToQueryLogger(int queryLoggerPid);
void sendReleaseToQueryLogger(int queryLoggerPid);






int main()
{
    struct recordStruct Record[1000] = {{0, "", 0}};

    struct loggerStruct LoggerSharedMemory[100]={{"",""}};

    msgqid = msgget(IPC_PRIVATE, 0644);
    if (msgqid == -1)
    {
        perror("Error in creating message box");
        exit(-1);
    }

    key_t key = ftok("shmfile", 65);
    int shmid = shmget(key, sizeof(Record), 0666 | IPC_CREAT);

    key_t key2 = key+1;
    shmid2 = shmget(key2, sizeof(LoggerSharedMemory), 0666 | IPC_CREAT);
    
    printf("\nshmid %d and shmid2 = %d\n\n", shmid, shmid2);
    
    LoggerFile = fopen("LoggerFile.txt", "w");
    fclose(LoggerFile);
    QueryFile = fopen("QueryFile.txt", "w");
    fclose(QueryFile);


    int pid;

    printf("\nI am the parent, my pid = %d and my parent's pid = %d\n\n", getpid(), getppid());
    int parentPid=getpid(); 
    filePointer = fopen(fileName, "r");

    if (filePointer == NULL)
    {
        printf("Could not open file %s", fileName);
    }
    else
    {
        fscanf(filePointer, "%d", &numberOfDBClients);
    }


    numberForked = numberOfDBClients + 2;       // Where 2 is the count of Logger and querry logger

    int pidsOfForkedArray[numberForked];
    char RecievedJobTitle[15];
    


    for (int forkingChild = 1; forkingChild <= (numberForked); forkingChild++)
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
        DBManager(pidsOfForkedArray,shmid);
    }
    else
    {
        int loggerPid;
        int queryLoggerPid = RecieveInitialMessage(msgqid,RecievedJobTitle,&loggerPid);
        if (strstr (RecievedJobTitle,"client")!=NULL)
            Client(shmid,RecievedJobTitle,queryLoggerPid, loggerPid);
        else if (strcmp(RecievedJobTitle,"querylogger")==0)
            QuerryLogger(shmid);
        else if (strcmp(RecievedJobTitle,"logger")==0)
            Logger(shmid);
    }

    
}


void DBManager(int* pidsOfForkedArray, int shmid)
{
    signal (SIGUSR1, handler);
    signal (SIGUSR2, handler3);
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

        SendInitialMessage(msgqid, jobTitle,pidsOfForkedArray[child - 1], pidsOfForkedArray[numberForked - 2],pidsOfForkedArray[numberForked - 1]);
    }
    
    struct recordStruct * DBData = (struct recordStruct *)shmat(shmid, (void *)0, 0);
    struct loggerStruct * LoggerSharedMemory = (struct loggerStruct *)shmat(shmid2, (void *)0, 0);
 
    printf("Received data (Manager): %d,%s,%d\n", DBData[0].key, DBData[0].name, DBData[0].salary);
    printf("Received data (Manager): %d,%s,%d\n", DBData[1].key, DBData[1].name, DBData[1].salary);
    printf("Received data (Manager): %d,%s,%d\n", DBData[2].key, DBData[2].name, DBData[2].salary);
       
    int nextKey = 0;
    int queuesForEachRecord[1000 * numberOfDBClients];
    int semaphoreLocks[1000] = {[0 ... 999] = 1};

    struct msgStruct receivedMessage;
    int iShouldKillMyself=0;

    while(1)
    {
        while (msgrcv(msgqid, &receivedMessage, sizeof(receivedMessage) - sizeof(long), getpid(), IPC_NOWAIT) == -1)
        {
            if(numOfDead==numberOfDBClients)
            {
                iShouldKillMyself=1;
                break;
            }    
            if (errno != ENOMSG)
                printf("Error receiving message: %s\n", strerror(errno));
        }

        if(iShouldKillMyself==1)
            break;
        
        printf("Received a message\n");
        receiveAnyMessageFromClients(receivedMessage, DBData, &nextKey, semaphoreLocks, queuesForEachRecord, msgqid,LoggerSharedMemory,pidsOfForkedArray[numberForked - 1]);

        if(numOfDead==numberOfDBClients)
            break;
    }

    sleep(5);

    kill(pidsOfForkedArray[numberForked - 2], SIGINT);
    kill(pidsOfForkedArray[numberForked - 1],SIGUSR2);

    printf("Received data (Manager): %d,%s,%d\n", DBData[0].key, DBData[0].name, DBData[0].salary);
    printf("Received data (Manager): %d,%s,%d\n", DBData[1].key, DBData[1].name, DBData[1].salary);
    printf("Received data (Manager): %d,%s,%d\n", DBData[2].key, DBData[2].name, DBData[2].salary);


    shmdt(DBData); 
    shmdt(LoggerSharedMemory); 
    shmctl(shmid,IPC_RMID,NULL);
    shmctl(shmid2,IPC_RMID,NULL);      

}
void handler(int signum)
{
    numOfDead++;
    printf("Number of dead =%d\n",numOfDead); 
     
}

void SendInitialMessage(key_t msgqid, char jobTitle[], int idofReciever, int queryLoggerPid , int loggerPid)
{
    int send_val;
    struct initialMsg message;

    message.mtype = idofReciever; 
    strcpy(message.jobTitle, jobTitle);
    message.queryLoggerPid=queryLoggerPid;
    message.loggerPid=loggerPid;

    send_val = msgsnd(msgqid, &message, sizeof(message) - sizeof(long), !IPC_NOWAIT);

    if (send_val == -1)
        perror("Errror in send");
    else
        printf("\n My id is %d Message sent that contains jobtitle: %s , queryPid= %d , loggerPid= %d\n", getpid(), message.jobTitle, message.queryLoggerPid, message.loggerPid);
}

int RecieveInitialMessage(key_t msgqid, char RecievedJobTitle[] , int* loggerPid)
{
    int rec_val;
    struct initialMsg message;

    rec_val = msgrcv(msgqid, &message, sizeof(message) - sizeof(long), getpid(), !IPC_NOWAIT);

    if (rec_val == -1)
        perror("Error in receive");
    else
	{
       printf("\n My id is %d Message received  contains jobtitle: %s , queryPid= %d \n", getpid(), message.jobTitle, message.queryLoggerPid);
       strcpy(RecievedJobTitle,message.jobTitle);
       (*loggerPid)=message.loggerPid;
       return message.queryLoggerPid;
	}

}

void QuerryLogger(int shmid)
{
    printf("Querry Logger alive %d\n", getpid());

    int queuesForFile[numberOfDBClients+1];
    int semaphoreLockOfFile= 1;
    for(int i=0 ; i <(numberOfDBClients+1) ; i++)
    {
        queuesForFile[i]=0;
    }

    struct QueryLoggerAndLoggerMsg receivedMessage;

    while(1)
    {
        while (msgrcv(msgqid, &receivedMessage, sizeof(receivedMessage) - sizeof(long), getpid(), IPC_NOWAIT) == -1)
        {
            if (errno != ENOMSG)
                printf("Error receiving message: %s\n", strerror(errno));
        }

        printf("Querry Logger received a message\n");
        if(strcmp(receivedMessage.operation,"_acquire")==0)
            receiveQueryAcquire(queuesForFile,&semaphoreLockOfFile,receivedMessage.senderPid);
        else
            receiveQueryRelease(queuesForFile,&semaphoreLockOfFile);
        sleep(1);    
    }

}

void Logger(int shmid)
{
    signal(SIGUSR2, handler2);
    struct loggerStruct *LoggerSharedMemory = (struct loggerStruct *)shmat(shmid2, (void *)0, 0);
    
    LoggerFile = fopen("LoggerFile.txt", "a");
    int full=0;
    int empty=100;
    int mutexLock=1;
    int queuesForIndex[numberOfDBClients+1];
    for(int i=0 ; i <(numberOfDBClients+1) ; i++)
    {
        queuesForIndex[i]=0;
    }

    int indexProducer=0;
    int indexConsumer=0;

    

    struct QueryLoggerAndLoggerMsg receivedMessage;

    while(1) //for (int i=0 ; i<1000000 ; i++)
    {
        
        if (msgrcv(msgqid, &receivedMessage, sizeof(receivedMessage) - sizeof(long), getpid(), IPC_NOWAIT) != -1)
        {
            sleep(1);
            // printf("Logger received a message\n");
            if(strcmp(receivedMessage.operation,"_acquire")==0)
                receiveLoggerAcquire(receivedMessage.senderPid, &empty, &mutexLock ,queuesForIndex, indexProducer);
            else
                receiveLoggerRelease( &full, &mutexLock ,queuesForIndex , &indexProducer);
        }

        if(loggerKillYourself==1)
            break;

        if(full!=0)
        {
            // printf("  Should consume here ,indexConsumer = %d\n",indexConsumer);
            printf("\n\n %s %s \n\n",LoggerSharedMemory[indexConsumer].time,LoggerSharedMemory[indexConsumer].message);
        
            fprintf(LoggerFile, " IndexConsumer: %d At %s  %s \n\n",indexConsumer, LoggerSharedMemory[indexConsumer].time,LoggerSharedMemory[indexConsumer].message);

            indexConsumer++;
            if(indexConsumer>=100)
                indexConsumer=0;
            full--;
            empty++;

        } 
        
    }

    fclose(LoggerFile);   
    shmdt(LoggerSharedMemory);
    printf("Logger will die\n");
    exit(0);
        

}

void handler2(int signum)
{
    loggerKillYourself=1;
}

void receiveLoggerAcquire(int senderPid ,int* empty, int* mutexLock , int* queuesForIndex , int indexProducer)
{
    printf("Inside logger Acquire\n");
    struct QueryLoggerAndLoggerMsg responseToAcquireMessage;
    responseToAcquireMessage.mtype = senderPid;

    if((*mutexLock)==0 | (*empty)==0)
    {
        int emptySlotInQueue = findFirstEmptyPlaceInQueryQueue(queuesForIndex);
        queuesForIndex[emptySlotInQueue] = senderPid;
        strcpy(responseToAcquireMessage.operation, "denied");
        printf("queuesForIndex[emptySlotInQueue]: %d, emptySlotInQueue %d\n",emptySlotInQueue,queuesForIndex[0]);
   
        
    } else 
    {
        (*empty)--;
        (*mutexLock)=0;
        strcpy(responseToAcquireMessage.operation, "granted");
        responseToAcquireMessage.senderPid= indexProducer;
    }

    int send_val = msgsnd(msgqid, &responseToAcquireMessage, sizeof(responseToAcquireMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Errror in send");
    else
    {
        printf("Sent response to logger acquire message to pid %d \n",senderPid);
        printf("^^^^^^^^^^^^^^^^^^ \n Acquire response message sent (Logger)= %s, %ld ,%ld\n", responseToAcquireMessage.operation, responseToAcquireMessage.mtype,responseToAcquireMessage.senderPid);
    }
    printf("In Acquire mutexLock: %d \n",(*mutexLock));
      
    
}

void receiveLoggerRelease(int* full, int* mutexLock , int* queuesForIndex , int* indexProducer)
{
    printf("Inside logger Release\n");
    
    (*mutexLock)= 1;
    (*full)++;
    (*indexProducer)++;
    if((*indexProducer)>=100)
        (*indexProducer)=0;
    printf("######### \n queuesForIndex[0]: %d   queuesForIndex[1]: %d\n",queuesForIndex[0],queuesForIndex[1]);
            
    int clientToBeReleased = queuesForIndex[0];
    if (clientToBeReleased != 0)
    {
        shiftArrayOfQueryQueuesOneStep(queuesForIndex);
        
        kill(clientToBeReleased, SIGCONT);
        kill(clientToBeReleased, SIGUSR2);
        printf("!!!!!!!!! \n I released %d \n",clientToBeReleased);
    }
    printf("In release mutexLock: %d \n",(*mutexLock));
   
}

int sendAcquireToLogger(int loggerPid)
{
   
    while(1)//for(int i=0 ; i<2 ; i++)
    {
        int skip=1;
        wakingUpBit=0;
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
            // printf("Sent acquire message to logger my pid is  %d \n",getpid());
            printf("Acquire message sent (Client) = %ld, %s mypid= %d \n", acquireMessage.senderPid, acquireMessage.operation,getpid());
        }
        // kill(loggerPid,SIGCONT);
        
        if (msgrcv(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), getpid(), !IPC_NOWAIT) == -1)
        {
            perror("Error in receiving response to acquire request from logger");
            return -1;
        }
        else
        {    
            printf("Received acquire response from logger  %s\n", acquireMessage.operation);
            if (strcmp(acquireMessage.operation,"denied")==0 && wakingUpBit==0)
            {
                printf("I will sleep now due to logger my pid: %d \n",getpid());
                raise(SIGSTOP);
                printf("I woke up now due to logger my pid: %d \n",getpid());
                skip=0;
            }
            // else
            {
                if(skip==1)
                {
                    printf("IndexProducer receied(CLient)  %ld \n",acquireMessage.senderPid);
                    int indexProducer=acquireMessage.senderPid;
                    return indexProducer;
                } 
            }
                
        }
    }   
}

void handler3(int signum)
{
    wakingUpBit=1;
    printf("////////////////////////////////wakingUpBit, my pid: %d \n",getpid());
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
        printf("Sent release message to Logger\n");
        printf("Release message sent (Client) = %ld, %s \n", releaseMessage.senderPid, releaseMessage.operation);
    }
    kill(loggerPid,SIGCONT);
}


void Client(int shmid,char* RecievedJobTitle,int queryLoggerPid, int loggerPid)
{
    signal (SIGUSR2, handler3);
    struct recordStruct *DBData = (struct recordStruct *)shmat(shmid, (void *)0, 0);
    struct loggerStruct *LoggerSharedMemory = (struct loggerStruct *)shmat(shmid2, (void *)0, 0);
 
    FILE *filePointer;
	char MyFile[20];
	snprintf(MyFile, sizeof MyFile, "%s%s",RecievedJobTitle,".txt");
	char *fileName =MyFile;
	filePointer = fopen(fileName, "r");
		if (filePointer == NULL)
		{
		    printf("Could not open file %s", fileName);
		}
		else
		{            
		 while (fgets(line, sizeof(line), filePointer) != NULL)
			{
			    sscanf(line, "%s", operation);
			    whatOperationIsThis(DBData,queryLoggerPid , loggerPid, LoggerSharedMemory);
                sleep(2);
			}
		}
	    
	fclose(filePointer);
    shmdt(DBData);
    shmdt(LoggerSharedMemory);
    printf("pid  %d will die", getpid());
    sleep(4);
    kill(getppid(),SIGUSR1);
    exit(0);
}

void getTime(char* timeNow)
{    
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    strftime(timeNow, 30, "%c", tm);
}

void writeInLoggerSharedMemory(struct loggerStruct *LoggerSharedMemory,char* messageToLogger, int loggerPid)
{
    int indexProducer=sendAcquireToLogger(loggerPid);
    if(indexProducer!=-1)
    {
        char timeNow[30];
        getTime(timeNow);

        strcpy(LoggerSharedMemory[indexProducer].message,messageToLogger);
        strcpy(LoggerSharedMemory[indexProducer].time,timeNow);

        printf("\n\n %s %s \n\n",LoggerSharedMemory[indexProducer].time,LoggerSharedMemory[indexProducer].message);
             
    }
    sendReleaseToLogger(loggerPid);
    
}

void whatOperationIsThis(struct recordStruct *DBData,int queryLoggerPid, int loggerPid, struct loggerStruct *LoggerSharedMemory)
{
    if (strcmp(operation, "add") == 0)
    {
        char name[15];
        int salary;
        sscanf(line, "%s%s%d", operation, name, &salary);
        sendAddRequestToManager(msgqid, name, salary);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request an add request to add name: %s  and salary: %d to database......", getpid(), name, salary);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);

    }
    else if (strcmp(operation, "modify") == 0)
    {
        char operationType[10];
        char sign[10]; // sign is just a dummy variable that is only used for correct Formatting
        int key;
        int modificationValue;
        sscanf(line, "%s%d%s%d", operation, &key, sign, &modificationValue);
        char *Mode = QueryOperationType();
        sendModifyRequestToManager(msgqid, key, Mode, modificationValue);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request a modify request to %s key number: %d by %d ......", getpid(), Mode, key, modificationValue);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);

    }
    else if (strcmp(operation, "release") == 0)
    {
        int key;
        sscanf(line, "%s%d", operation, &key);
        sendReleaseRequestToManager(msgqid, key);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request a release request to key number: %d......", getpid(), key);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);

    }
    else if (strcmp(operation, "acquire") == 0)
    {
        int key;
        sscanf(line, "%s%d", operation, &key);
        sendAcquireRequestToManager(msgqid, key);

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request an acquire request to key number: %d  ......", getpid(), key);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
   
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

        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am client with pid %d and I request a query request that I printed in the query file  ......",getpid());
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
   
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


void hypridQuery(char* filterByName,char* filterOn,char* filterBySalary,int salaryFilter,struct recordStruct *DBData)
{
    if(strcmp(filterByName, "startsWith") == 0)
    {
        if(strcmp(filterBySalary, "_greaterOrEqual") == 0)
            nameStartsWithAndSalaryGreaterOrEqual(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_lessOrEqual") == 0)
            nameStartsWithAndSalaryLessOrEqual(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_greater") == 0)
            nameStartsWithAndSalaryGreater(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_less") == 0)
            nameStartsWithAndSalaryLess(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_equal") == 0)
            nameStartsWithAndSalaryEquals(DBData,filterOn,salaryFilter);
    } 
    else
    {
        if(strcmp(filterBySalary, "_greaterOrEqual") == 0)
            nameEqualsAndSalaryGreaterOrEqual(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_lessOrEqual") == 0)
            nameEqualsAndSalaryLessOrEqual(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_greater") == 0)
            nameEqualsAndSalaryGreater(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_less") == 0)
            nameEqualsAndSalaryLess(DBData,filterOn,salaryFilter);
        else if(strcmp(filterBySalary, "_equal") == 0)
            nameEqualsAndSalaryEquals(DBData,filterOn,salaryFilter);
    }   
}


////////////////////////////////////////////////////////////////Client Requests////////////////////////////////////////////////////////////////////////////

void sendAddRequestToManager(key_t msgqid, char *name, int salary)
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
        if (addMessage.Record.key== -1)
        {
            printf(" Add request Rejected by manager\n");
        }
        else
        {
            printf("\n\n\n Add request confirmed by manager and record was added to key number %d\n",addMessage.Record.key);
        }
    }
}

void sendAcquireRequestToManager(key_t msgqid, int key)
{
    for(int i=0 ; i<2 ; i++)
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
            return ;
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

void sendModifyRequestToManager(key_t msgqid, int key, char *name, int salary)
{
    int send_val;
    struct msgStruct modifyMessage;

    modifyMessage.mtype = getppid();
    modifyMessage.Record.key = key;
    strcpy(modifyMessage.Record.name, name);
    modifyMessage.Record.salary = salary;

    send_val = msgsnd(msgqid, &modifyMessage, sizeof(modifyMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("Sent modify message\n");
        printf("Modify message sent (Client) = %d, %s, %d\n", modifyMessage.Record.key, modifyMessage.Record.name, modifyMessage.Record.salary);
    }
}

void sendReleaseRequestToManager(key_t msgqid, int key)
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
    for(int i=0 ; i<2 ; i++)
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
            printf("Sent acquire message to query logger whose pid is  %d \n",queryLoggerPid);
            printf("Acquire message sent (Client) = %ld, %s mypid= %d \n", acquireMessage.senderPid, acquireMessage.operation,getpid());
        }

        if (msgrcv(msgqid, &acquireMessage, sizeof(acquireMessage) - sizeof(long), getpid(), !IPC_NOWAIT) == -1)
        {
            perror("Error in receiving response to acquire request");
            return ;
        }
        else
        {
    
            printf("Received acquire response  %s\n", acquireMessage.operation);
            if (strcmp(acquireMessage.operation,"denied")==0)
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



void receiveAnyMessageFromClients(struct msgStruct receivedMessage, struct recordStruct *DBData, int *nextKey, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid , struct loggerStruct * LoggerSharedMemory, int loggerPid)
{
    int recordNum = receivedMessage.Record.key;
    if (receivedMessage.Record.key == -1)
        receiveAddMessageFromClient(receivedMessage, DBData, nextKey,LoggerSharedMemory,loggerPid);
    if (strcmp(receivedMessage.Record.name, "_acquire") == 0)
    {
        int clientId = receivedMessage.Record.salary;
        receiveAcquireMessageFromClient(recordNum, clientId, semaphoreLocks, queuesForEachRecord, msgqid,LoggerSharedMemory,loggerPid);
    }
    if (strcmp(receivedMessage.Record.name, "_release") == 0)
    {
        receiveReleaseMessageFromClient(recordNum, semaphoreLocks, queuesForEachRecord,LoggerSharedMemory,loggerPid);
    }
    if (strcmp(receivedMessage.Record.name, "_add") == 0 || strcmp(receivedMessage.Record.name, "_subtract") == 0)
        receiveModifyMessageFromClient(receivedMessage, DBData,LoggerSharedMemory,loggerPid);
}

void receiveAddMessageFromClient(struct msgStruct addMessage, struct recordStruct *DBData, int *nextKey, struct loggerStruct * LoggerSharedMemory, int loggerPid)
{
    if (*nextKey==1000)
    {
    //This means memory is full 
     addMessage.Record.key=-1;
     
     char timeNow[30];
    getTime(timeNow);
    printf("\n %s\n", timeNow);

    char messageToLogger[200];
    snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an add request from %d .Rejected as memory is full ", getpid(), addMessage.senderPid);
    printf("\n %s", messageToLogger);

    }
    else
    {
        
        addMessage.Record.key = *nextKey;
        DBData[*nextKey] = addMessage.Record;
        (*nextKey)++;
        
        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an add request from %d .Approved as : key %d , name  %s n salary %d", getpid(), addMessage.senderPid, addMessage.Record.key, addMessage.Record.name, addMessage.Record.salary);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
        
    }

    addMessage.mtype=addMessage.senderPid;
    addMessage.senderPid=getppid();
    int send_val = msgsnd(msgqid, &addMessage, sizeof(addMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Error in send");
    else
    {
        printf("\n Sent Add response to client \n");
        printf("\n Add response message sent (Manager)= %d, %s \n", addMessage.Record.key, addMessage.Record.name); 
    }

    
        

}

    

void receiveAcquireMessageFromClient(int recordNum, int clientId, int *semaphoreLocks, int *queuesForEachRecord, key_t msgqid,struct loggerStruct * LoggerSharedMemory, int loggerPid)
{
    int send_val;
    struct msgStruct responseToAcquireMessage;
    responseToAcquireMessage.mtype = clientId;
    printf("Inside acguire Semaphore[0]=%d\n", semaphoreLocks[0]);
    printf("Semaphore[1]=%d\n", semaphoreLocks[1]);
    printf("Semaphore[2]=%d\n", semaphoreLocks[2]);

    if (semaphoreLocks[recordNum] == 1)
    {
        semaphoreLocks[recordNum] = 0;
        responseToAcquireMessage.Record.key = 1; // access granted
 
        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an acquire request from %d to acquire key %d , and request approved", getpid(), clientId, recordNum);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
        
    }
    else
    {
        int emptySlotInQueue = findFirstEmptyPlaceInQueue(recordNum, queuesForEachRecord);
        queuesForEachRecord[recordNum * numberOfDBClients + emptySlotInQueue] = clientId;
        responseToAcquireMessage.Record.key = 0; // access denied
      
        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved an acquire request from %d to key %d , and request is on queue", getpid(), clientId, recordNum);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
        
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

void receiveModifyMessageFromClient(struct msgStruct modifyMessage, struct recordStruct *DBData,struct loggerStruct * LoggerSharedMemory, int loggerPid)
{
    if (strcmp(modifyMessage.Record.name, "_add") == 0)
    {
        DBData[modifyMessage.Record.key].salary += modifyMessage.Record.salary;
        
        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a modify request from %d to add key %d by %d ", getpid(), modifyMessage.senderPid, modifyMessage.Record.key,modifyMessage.Record.salary);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid); 
    }
    else
    {
        DBData[modifyMessage.Record.key].salary -= modifyMessage.Record.salary;
        
        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a modify request from %d to minus key %d by %d ", getpid(), modifyMessage.senderPid, modifyMessage.Record.key,modifyMessage.Record.salary);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
    }
}

void receiveReleaseMessageFromClient(int recordNum, int *semaphoreLocks, int *queuesForEachRecord,struct loggerStruct * LoggerSharedMemory, int loggerPid)
{
    semaphoreLocks[recordNum] = 1;
    int clientToBeReleased = queuesForEachRecord[recordNum * numberOfDBClients];
    if (clientToBeReleased != 0)
    {
        shiftArrayOfQueuesOneStep(recordNum, queuesForEachRecord);
        kill(clientToBeReleased, SIGCONT);
    
        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a release request from %d to key %d , and process on queue s released", getpid(), clientToBeReleased, recordNum);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
    
    } else
    {
        char messageToLogger[200];
        snprintf(messageToLogger, sizeof messageToLogger, "I am DBManager with pid %d and I recieved a release request from %d to key %d .No processes in queue to be released", getpid(), clientToBeReleased, recordNum);
        writeInLoggerSharedMemory(LoggerSharedMemory,messageToLogger,loggerPid);
    
    }
    
    printf("Inside release Semaphore[0]=%d\n", semaphoreLocks[0]);
    printf("Semaphore[1]=%d\n", semaphoreLocks[1]);
    printf("Semaphore[2]=%d\n", semaphoreLocks[2]);
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
    for (int record = 0; record < 1000; record++)
    {
        if (DBData[record].salary >= filter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    }
}

void lessThanOrEqual(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (DBData[record].salary <= filter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    }
}

void greaterThan(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (DBData[record].salary > filter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    }
}

void lessThan(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (DBData[record].salary < filter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    }
}

void equal(struct recordStruct *DBData, int filter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (DBData[record].salary == filter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    }
}

////// ******************************************* NAME QUERIES **************************////////////////
void nameEquals(struct recordStruct *DBData, char filter[])
{
    for (int record = 0; record < 1000; record++)
    {
        if (strcmp(DBData[record].name,filter) == 0)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    }
}


void startsWith(struct recordStruct *DBData, char filter[])
{
    printf("here\n");// 
    for (int record = 0; record < 1000; record++)
    {
        if (strncmp(DBData[record].name,filter , strlen(filter)) == 0)
             fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    }
}

////////////********************************* HYBRID QUERIESSS   *******************************************////////////////
////////////********************************* Name Equals and salary query *********************************///////////////
void nameEqualsAndSalaryEquals(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strcmp(DBData[record].name,filter) == 0 && DBData[record].salary == salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}
void nameEqualsAndSalaryLess(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strcmp(DBData[record].name,filter) == 0 && DBData[record].salary < salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

void nameEqualsAndSalaryLessOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strcmp(DBData[record].name,filter) == 0 && DBData[record].salary <= salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

void nameEqualsAndSalaryGreater(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strcmp(DBData[record].name,filter) == 0 && DBData[record].salary > salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

void nameEqualsAndSalaryGreaterOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strcmp(DBData[record].name,filter) == 0 && DBData[record].salary >= salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

//************************   		Name Starts With and salaries queries 		*************************///

void nameStartsWithAndSalaryEquals(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strncmp(DBData[record].name,filter , strlen(filter)) == 0 && DBData[record].salary == salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}
void nameStartsWithAndSalaryLess(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strncmp(DBData[record].name,filter , strlen(filter)) == 0&& DBData[record].salary < salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

void nameStartsWithAndSalaryLessOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strncmp(DBData[record].name,filter , strlen(filter))==0 && DBData[record].salary <= salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

void nameStartsWithAndSalaryGreater(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strncmp(DBData[record].name,filter , strlen(filter)) == 0 && DBData[record].salary > salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

void nameStartsWithAndSalaryGreaterOrEqual(struct recordStruct *DBData, char filter[], int salaryFilter)
{
    for (int record = 0; record < 1000; record++)
    {
        if (strncmp(DBData[record].name,filter , strlen(filter)) == 0 && DBData[record].salary >= salaryFilter)
            fprintf (QueryFile, "Key: %d  Name: %s Salary: %d \n",DBData[record].key,DBData[record].name,DBData[record].salary);
    } 
}

//////////////////////////////////////////////////////////////Query Logger Requests/////////////////////////////////////////////////////////////////////

void receiveQueryAcquire(int * queuesForFile,int* semaphoreLockOfFile,int senderPid)
{
    printf("Inside Querry logger Acquire\n");
    struct QueryLoggerAndLoggerMsg responseToAcquireMessage;
    responseToAcquireMessage.mtype = senderPid;

    if ((*semaphoreLockOfFile)== 1)
    {
        (*semaphoreLockOfFile)= 0;
        strcpy(responseToAcquireMessage.operation, "granted");
    }
    else
    {
        int emptySlotInQueue = findFirstEmptyPlaceInQueryQueue(queuesForFile);
        queuesForFile[emptySlotInQueue] = senderPid;
        printf("queuesForFile[emptySlotInQueue]: %d, emptySlotInQueue %d\n",emptySlotInQueue,queuesForFile[0]);
   
        strcpy(responseToAcquireMessage.operation, "denied");
    }

    int send_val = msgsnd(msgqid, &responseToAcquireMessage, sizeof(responseToAcquireMessage) - sizeof(long), !IPC_NOWAIT);
    if (send_val == -1)
        perror("Errror in send");
    else
    {
        printf("Sent response to querry acquire message to pid %d \n",senderPid);
        printf("Acquire response message sent (Querry Logger)= %s, %ld, semaphoreLockOfFile: %d\n", responseToAcquireMessage.operation, responseToAcquireMessage.mtype,(*semaphoreLockOfFile));
    }
    printf("In Acquire semaphoreLockOfFile: %d \n",(*semaphoreLockOfFile));
    // printf("queuesForFile[emptySlotInQueue]: %d \n",queuesForFile[0]);
   
       
}

void receiveQueryRelease(int * queuesForFile,int* semaphoreLockOfFile)
{
    (*semaphoreLockOfFile)= 1;
    int clientToBeReleased = queuesForFile[0];
    if (clientToBeReleased != 0)
    {
        shiftArrayOfQueryQueuesOneStep(queuesForFile);
        kill(clientToBeReleased, SIGCONT);
    }
    printf("In release semaphoreLockOfFile: %d \n",(*semaphoreLockOfFile));
        
}

void shiftArrayOfQueryQueuesOneStep(int *queuesForFile)
{
    for (int step = 0; step < (numberOfDBClients+1); step++)
    {
        queuesForFile[step] = queuesForFile[step + 1];
    }
    queuesForFile[ numberOfDBClients ] = 0;
}

int findFirstEmptyPlaceInQueryQueue(int *queuesForFile)
{
    for (int step = 0; step < (numberOfDBClients+1); step++)
    {
        if (queuesForFile[step] == 0)
            return step;
    }
}


