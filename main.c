#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>

static const char* OK_REPLY="OK";
static const char* NO_EXPECTED_REPLY="";
static const uint  REDIS_MAX_RETRY_COUNT = 2;

typedef struct redis_info 
{
    char* id;
    char* name; 
    char* address;
    uint16_t port;
	bool initialized;
} redis_info;

typedef struct sentinel_info 
{
	char* hostname;
	uint16_t port;
} sentinel_info;

static pthread_mutex_t redis_mtx = PTHREAD_MUTEX_INITIALIZER;

void parseInfo(char text[], redis_info* master)
{
    char* token = strtok(text, "\n");
    char* last_line;
    while( token != NULL ) {
            last_line = token;
            token = strtok(NULL, "\n");
    }
    //printf("Parsing the text: %s \n", last_line);
    
    master->id = strtok(last_line, ":");
    char*array[5];
    for (int i = 0; i < 5; i++)
    {
        array[i] = strtok(NULL, ",");
    }
        
    strtok(array[0], "=");
	// redis master name
	master->name = strtok(NULL, "=");

	
    // redis master address & port
    strtok(array[2], "=");
    master->address = strtok(NULL, ":");
    master->port = atoi(strtok(NULL, ":"));
}

bool checkResult(const redisContext *context, const redisReply* reply, const char* expectedReply)
{
    if(!context || context->err != REDIS_OK || !reply)
    {
        printf("nope \n");
        return false;
    }
    
    if(strcmp(expectedReply, NO_EXPECTED_REPLY) == 0)
    {
        return true;
    }
    else if(strcmp(expectedReply, reply->str) == 0)
    {
        return true;
    }

    return false;
}

bool initRedisData(redisContext *sc, redis_info** master)
{
	*master = calloc(1, sizeof(struct redis_info));
	redis_info* my_master = *master;
    my_master->initialized = false;
	redisReply *reply = redisCommand(sc, "INFO Sentinel");	
	if(!checkResult(sc, reply, NO_EXPECTED_REPLY))
	{
		printf("Error %s \n", reply->str);
        if(reply)
        {
            freeReplyObject(reply);
        }
		return false;
	}	
	//printf("INFO Sentinel: \n %s \n", reply->str);		
	parseInfo(reply->str, my_master);
	my_master->initialized = true;	
    freeReplyObject(reply);
    printf("New redis master - %s: address %s port %i \n (information from sentinel %s:%i) \n", my_master->name, my_master->address, my_master->port, sc->tcp.source_addr, sc->tcp.port);
    return true;
}

void enterEmergencyMode()
{
    printf("Entering emergency mode.\n");
};

int selectRedisDb(redisContext *rc, int redis_index)
{
    pthread_mutex_lock(&redis_mtx);
    redisReply *reply = redisCommand(rc, "SELECT %i", redis_index);        
    if(!checkResult(rc, reply, OK_REPLY))
    {
        if(reply)
        {
            printf("Reply text %s\n", reply->str); 
            freeReplyObject(reply);
        }
        pthread_mutex_unlock(&redis_mtx);         
        return 0;
    }
    printf("Selected redis database index: %s \n", reply->str);
    freeReplyObject(reply);
    pthread_mutex_unlock(&redis_mtx);  
    return 1;
}

redisContext* connectToRedisServer(const redis_info* master_data, struct timeval timeout)
{
    redisContext* rc = NULL;
    char tmp[20];
    strcpy(tmp,master_data->address);
    printf("Connecting to redis on %s:%i\n", tmp, master_data->port); 
    rc = redisConnectWithTimeout(tmp, master_data->port, timeout);
    if(rc == NULL || rc->err)
    {
            printf("Cannot connect to redis %s:%i\n", tmp, master_data->port);
            if(rc)
            {
                printf("Connection error: %s\n", rc->errstr);
                redisFree(rc);   
                rc=NULL;             
            }
            else
            {
                printf("Connection error: can't allocate redis context\n");
            }
            return rc;
    }
    
    printf("Connected to redis %s:%i\n",tmp, master_data->port);

    redisReply* reply = redisCommand(rc, "AUTH huehue1");
    if(!checkResult(rc, reply, OK_REPLY))
    {
        printf("Error while authorizing\n");
        if(reply)
        {
            freeReplyObject(reply);
        }
        return rc;
    }
    printf("Authorized: %s \n", reply->str);
    freeReplyObject(reply);
    return rc;
}


redisContext* connectToSentinels(const sentinel_info* sentinel, const struct timeval mTimeout)
{	
    printf("Connecting to sentinel on %s:%i\n", sentinel->hostname, sentinel->port);
    redisContext* sentContext = redisConnectWithTimeout(sentinel->hostname, sentinel->port, mTimeout);
    if(sentContext == NULL || sentContext->err)
    {
        printf("Cannot connect to sentinel %s:%i\n", sentinel->hostname, sentinel->port);
        if(sentContext)
        {
            redisFree(sentContext);
            sentContext = NULL;
        }
    }
    else
    {
        printf("Connected to sentinel on %s:%i\n", sentinel->hostname, sentinel->port);
    }
    return sentContext;
}


int main(int argc, char **argv) {
	
	// Read input arguments
	if(argc < 4)
	{
		printf("Not enough arguments! \n");
        return -1;
	}	
	double timeout =  atof(argv[argc-2]);
	struct timeval mTimeout = {(int)(floor(timeout)), timeout - floor(timeout)};
	int redis_index =  atoi(argv[argc-1]);
	int num_sent = argc-3;

    //Setup sentinels  data
    sentinel_info* my_sentinels[num_sent];    
    for (int i =0; i<num_sent; i++)
    {
        my_sentinels[i] = calloc(1, sizeof(struct sentinel_info));
        char* hostname = strtok(argv[i+1], ":");
        my_sentinels[i]->hostname = calloc(1, (strlen(hostname) + 1) * sizeof(char));
        strcpy(my_sentinels[i]->hostname, hostname);
        my_sentinels[i]->port = atoi(strtok(NULL, ":"));
        printf("New sentinel - address %s port %i \n", my_sentinels[i]->hostname, my_sentinels[i]->port);
    }
    
    redisContext *sentContext = NULL;
    while (true)
    {       
        if(sentContext == NULL)
        {
            // Try to connect of one of sentinels
            for (int j=0; j< num_sent; j++)
            {
                printf("try new sentinel\n");
                sentContext = connectToSentinels(my_sentinels[j], mTimeout);
                if(sentContext != NULL && sentContext->err != 0)
                {
                    break;
                }
            }
        }

        if(sentContext != NULL)
        {
            redis_info* master_data = NULL;
            printf("Acquiring information about master \n");
            // Get information about redis server from sentinel && connecting 
            redisContext* rc;         
            if(initRedisData(sentContext, &master_data))	
            {	                
                redisContext* rc = connectToRedisServer(master_data, mTimeout);
                bool connected = (rc != NULL);
                while(connected)
                {
                    int result = selectRedisDb(rc, redis_index);   
                    if(result == 0)
                    {
                        printf("Disonnected with redis %s:%i\n", master_data->address, master_data->port);
                        connected = false;
                    }  
                    sleep(3);
                }    
            }
            else
            {
                sentContext=NULL;
            }
        }
        sleep(5);
    }

	return 0;        
}
