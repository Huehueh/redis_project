#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>

char* OK_REPLY="OK";

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
    char * token = strtok(text, "\n");
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

bool isConnectionError(const redisContext *redis_ctx, const redisReply *reply)
{
    if (redis_ctx->err == REDIS_ERR_IO) 
	{
        return true;
    }
    return redis_ctx->err == REDIS_ERR_EOF || redis_ctx->err == REDIS_ERR_PROTOCOL;
}

bool isRedisConnected(const redisContext *redisContext, const redisReply *reply)
{
    return (redisContext && redisContext->err == REDIS_OK && reply && strcmp(reply->str,OK_REPLY)==0);
}

void initRedisData(redisContext *sc, redis_info** master)
{
	*master = calloc(1, sizeof(struct redis_info));
	redis_info* my_master = *master;
    my_master->initialized = false;
	redisReply *reply = redisCommand(sc, "INFO Sentinel");	
	if(isConnectionError(sc, reply))
	{
		freeReplyObject(reply);
		return;
	}	
	printf("INFO Sentinel: \n %s \n", reply->str);		
	parseInfo(reply->str, my_master);
	my_master->initialized = true;	
    freeReplyObject(reply);
}

void enterEmergencyMode(){};

void connectToRedisServer(const redis_info* master_data, struct timeval timeout, int redis_index)
{
	printf("Connecting to redis on %s:%i\n", master_data->address, master_data->port);
    char tmp[20];
    strcpy(tmp,master_data->address);
	redisContext *rc = redisConnectWithTimeout(tmp, master_data->port, timeout);
	if(rc == NULL || rc->err)
	{
			printf("Cannot connect to redis %s:%i\n", tmp, master_data->port);
			if(rc->err)
			{
				redisFree(rc);
                return;
			}
	}
    printf("Connected to redis %s:%i\n", tmp, master_data->port);

	redisReply* reply = redisCommand(rc, "AUTH huehue1");
	printf("Authorized: %s \n", reply->str);
	freeReplyObject(reply);

    while(true)
    {
        pthread_mutex_lock(&redis_mtx);
        redisReply *reply = redisCommand(rc, "SELECT %i", redis_index);        
        if(!isRedisConnected(rc, reply))
        {
            freeReplyObject(reply);
            pthread_mutex_unlock(&redis_mtx);  
            printf("Disonnected with redis %s:%i\n", tmp, master_data->port);
            break;
        }
        char*text = reply->str;
        printf("Index: %s \n", text);
        freeReplyObject(reply);
        pthread_mutex_unlock(&redis_mtx);       
        
        sleep(3);
    }
}

int main(int argc, char **argv) {
	
	// Read input arguments
	if(argc < 4)
	{
		printf("Not enough arguments!");
	}	
	double timeout =  atof(argv[argc-2]);
	struct timeval mTimeout = {(int)(floor(timeout)), timeout - floor(timeout)};
	int redis_index =  atoi(argv[argc-1]);
	int num_sent = argc-3;

    // Setup sentinels  data
    struct sentinel_info* my_sentinels[num_sent];    
    for (int i =0; i<num_sent; i++)
    {
        my_sentinels[i] = calloc(1, sizeof(struct sentinel_info));
        char* hostname = strtok(argv[i+1], ":");
        my_sentinels[i]->hostname = calloc(1, (strlen(hostname) + 1) * sizeof(char));
        strcpy(my_sentinels[i]->hostname, hostname);
        my_sentinels[i]->port = atoi(strtok(NULL, ":"));
        printf("SENTINEL FOUND Address %s port %i \n", my_sentinels[i]->hostname, my_sentinels[i]->port);
    }

    redisContext *sentContext = NULL;

    // Connect with at least one of sentinels  
    for (int j=0; j< num_sent; j++)
    {	
        printf("Connecting to sentinel on %s:%i\n", my_sentinels[j]->hostname, my_sentinels[j]->port);
        sentContext = redisConnectWithTimeout(my_sentinels[j]->hostname, my_sentinels[j]->port, mTimeout);
        if(sentContext == NULL || sentContext->err)
        {
            printf("Cannot connect to sentinel %s:%i\n", my_sentinels[j]->hostname, my_sentinels[j]->port);
            if(sentContext)
            {
                redisFree(sentContext);
                sleep(3);
            }
        }
        else
        {
            printf("Connected to sentinel on %s:%i\n", my_sentinels[j]->hostname, my_sentinels[j]->port);
            break;
        }
    }
    
    // if(sentContext == NULL)
    // {
    // 	enterEmergencyMode();
    // }
    
    // /*redisReply *reply;
    // reply = redisCommand(sentContext, "PING");
    // printf("PING: %s \n", reply->str);
    // freeReplyObject(reply);*/
    
    // Get information about redis server
    struct redis_info* master_data;
    initRedisData(sentContext, &master_data);	
    
    // CONNECTING TO REDIS	
    connectToRedisServer(master_data, mTimeout, redis_index);


	return 0;        
}
