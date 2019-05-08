/* Redis benchmark utility.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>

#include <sds.h> /* Use hiredis sds. */
#include "ae.h"
#include "hiredis.h"
#include "adlist.h"
#include "zmalloc.h"

#define UNUSED(V) ((void) V)
#define RANDPTR_INITIAL_SIZE 8

enum {
    DATA_BUF_SIZE = 10240,   // 10K
    DATA_STEP_SIZE = 10,
};

enum {
    CMD_TYPE_PING_INLINE = 0,
    CMD_TYPE_SET,
    CMD_TYPE_GET,
    CMD_TYPE_INCR,
    CMD_TYPE_LPUSH,
    CMD_TYPE_RPUSH,       // 5
    CMD_TYPE_LPOP,
    CMD_TYPE_RPOP,
    CMD_TYPE_SADD,
    CMD_TYPE_SPOP,
    CMD_TYPE_LRANGE_100,  //10
    CMD_TYPE_LRANGE_300,
    CMD_TYPE_LRANGE_500,
    CMD_TYPE_LRANGE_600,
    CMD_TYPE_MSET,
    CMD_TYPE_MGET,        // 15
    CMD_TYPE_HSET,
    CMD_TYPE_HGET,
    CMD_TYPE_HMSET,
    CMD_TYPE_HMGET,
    CMD_TYPE_ZADD,        // 20
    CMD_TYPE_ZRANGE_100,
    CMD_TYPE_ZRANGE_300,
    CMD_TYPE_ZRANGE_500,
    CMD_TYPE_NR,
    CMD_TYPE_NONE,        // 25
};

enum {
    KEY_TYPE_STRING,
    KEY_TYPE_LIST,
    KEY_TYPE_HASH,
    KEY_TYPE_SET,
    KEY_TYPE_ZSET,
    KEY_TYPE_NR,
};

static struct config {
    aeEventLoop *el;
    const char *hostip;
    int hostport;
    const char *hostsocket;
    int numclients;
    int liveclients;
    int requests;
    int requests_issued;
    int requests_finished;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keepalive;
    int pipeline;
    int showerrors;
    long long start;
    long long totlatency;
    long long *latency;
    const char *title;
    list *clients;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    int dbnum;
    sds dbnumstr;
    char *tests;
    char *auth;
    char *keybuf;
    char *fieldbuf;
    int mopnr;
    char *data;
    int dlen;
    int keyidx;
    int keyidxlen;
    int fieldidx;
    int display_realtime;
} config;

typedef struct _client {
    redisContext *context;
    sds obuf;
    char **randptr;         /* Pointers to :rand: strings inside the command buf */
    size_t randlen;         /* Number of pointers in client->randptr */
    size_t randfree;        /* Number of unused pointers in client->randptr */
    size_t written;         /* Bytes of 'obuf' already written */
    long long start;        /* Start time of a request */
    long long latency;      /* Request latency */
    int pending;            /* Number of pending requests (replies to consume) */
    int prefix_pending;     /* If non-zero, number of pending prefix commands. Commands
                               such as auth and select are prefixed to the pipeline of
                               benchmark commands and discarded after the first send. */
    int prefixlen;          /* Size in bytes of the pending prefix commands */
    int lastpending;
    unsigned int cmdtype;
    sds cmdext;
    
} *client;

typedef struct key_type_desc {
    char *key_type_pre;
    char *key_type_def;
} key_type_desc_t;

typedef struct benchmark_op {
    char *cmd_title;
    char *cmd_name;    // command name, such as set, get, .etc
    int need_key;      // command(cmd_name) need key or not
    unsigned char cmd_with_data;  // command need data or not
    unsigned char key_type;              // KEY_TYPE_XXX       
    unsigned char need_prepare_data;     // command need to prepare data or not
    unsigned char prepare_data_cmd_type; // CMD_TYPE_XXX    
    unsigned char key_has_multilevel;    // hash and zset has more than 1 level    
    unsigned char has_multi_key;         // command to proccess multiple keys once
    unsigned char second_level_need_field; // zset, hash need field for second 
                                           // level, but list and set need 
                                           // no field for second level
    char *cmdext;    // extend description for command, for example:
                     //     lrange testlist 0 -1   "0 -1" should be 'cmdext'
} benchmark_op_t;

typedef void (*gen_cmdbuf)(int *, int *, int *, int *, char *);
typedef client (*create_client)(sds , int, int);
typedef void (*create_missing_client)(sds , int, int);

enum {
    NEED_PREPARE_DATA = 1,
    NOT_NEED_PREPARE_DATA = 0,

    CMD_NEED_KEY = 1,
    CMD_NOT_NEED_KEY = 0,

    NEED_SHOW_LATENCY = 1,
    NOT_NEED_SHOW_LATENCY = 0,

    NEED_FREE_ALL_CLIENTS = 1,
    NOT_NEED_FREE_ALL_CLIENTS = 0,

    GEN_SAME_KEY = 1,
    GEN_DIFF_KEY = 0,

    KEY_HAS_MULTILEVEL = 1,
    KEY_HAS_NO_MULTILEVEL = 0,
    
    HAS_MULTIKEY = 1,
    HAS_NO_MULTIKEY = 0,

    SECOND_LEVEL_NEED_FIELD = 1,
    SECOND_LEVEL_NOT_NEED_FIELD = 0,

    CMD_WITH_DATA = 1,
    CMD_WITH_NO_DATA = 0,
};

key_type_desc_t keytype_pre[KEY_TYPE_NR] = {
    [KEY_TYPE_STRING] = {"s_", "DEF_s"},
    [KEY_TYPE_LIST] = {"L_", "DEF_L"},
    [KEY_TYPE_HASH] = {"H_", "DEF_H"},
    [KEY_TYPE_SET] = {"S_", "DEF_S"},
    [KEY_TYPE_ZSET] = {"Z_", "DEF_Z"},
};

benchmark_op_t benchmark_set[CMD_TYPE_NR] = {
    [CMD_TYPE_PING_INLINE] = {"PING",
                             "PING",
                             CMD_NOT_NEED_KEY,
                             CMD_WITH_NO_DATA,
                             KEY_TYPE_STRING,
                             NOT_NEED_PREPARE_DATA,
                             CMD_TYPE_NONE,
                             KEY_HAS_NO_MULTILEVEL,
                             HAS_NO_MULTIKEY,
                             SECOND_LEVEL_NOT_NEED_FIELD,
                             NULL},
    [CMD_TYPE_SET] = {"SET",
                      "SET",
                      CMD_NEED_KEY,
                      CMD_WITH_DATA,
                      KEY_TYPE_STRING,
                      NOT_NEED_PREPARE_DATA,
                      CMD_TYPE_NONE,
                      KEY_HAS_NO_MULTILEVEL,
                      HAS_NO_MULTIKEY,
                      SECOND_LEVEL_NOT_NEED_FIELD,
                      NULL},
    [CMD_TYPE_GET] = {"GET",
                      "GET",
                      CMD_NEED_KEY,
                      CMD_WITH_NO_DATA,
                      KEY_TYPE_STRING,
                      NEED_PREPARE_DATA,
                      CMD_TYPE_SET,
                      KEY_HAS_NO_MULTILEVEL,
                      HAS_NO_MULTIKEY,
                      SECOND_LEVEL_NOT_NEED_FIELD,
                      NULL},                      
    [CMD_TYPE_INCR] = {"INCR",
                       "INCR",
                       CMD_NEED_KEY,
                       CMD_WITH_NO_DATA,
                       KEY_TYPE_STRING,
                       NOT_NEED_PREPARE_DATA,
                       CMD_TYPE_NONE,
                       KEY_HAS_NO_MULTILEVEL,
                       HAS_NO_MULTIKEY,
                       SECOND_LEVEL_NOT_NEED_FIELD,
                       NULL},
    [CMD_TYPE_LPUSH] = {"LPUSH_SINGLE_KEY",
                        "LPUSH",
                        CMD_NEED_KEY,
                        CMD_WITH_DATA,
                        KEY_TYPE_LIST,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_NO_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL},
    [CMD_TYPE_RPUSH] = {"RPUSH_MULTI_KEY",
                        "RPUSH",
                        CMD_NEED_KEY,
                        CMD_WITH_DATA,
                        KEY_TYPE_LIST,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_NO_MULTILEVEL,
                        HAS_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL},
    [CMD_TYPE_LPOP] =  {"LPOP",
                        "LPOP",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_LIST,
                        NEED_PREPARE_DATA,
                        CMD_TYPE_LPUSH,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL},
    [CMD_TYPE_RPOP] =  {"RPOP",
                        "RPOP",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_LIST,
                        NEED_PREPARE_DATA,
                        CMD_TYPE_LPUSH,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL},
    [CMD_TYPE_SADD] =  {"SADD_MULTI_KEY",
                        "SADD",
                        CMD_NEED_KEY,
                        CMD_WITH_DATA,
                        KEY_TYPE_SET,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_MULTILEVEL,
                        HAS_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL},
    [CMD_TYPE_SPOP] =  {"SPOP",
                        "SPOP",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_SET,
                        NEED_PREPARE_DATA,
                        CMD_TYPE_SADD,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL},
    [CMD_TYPE_LRANGE_100] =  {"LRANGE_100",
                              "LRANGE",
                              CMD_NEED_KEY,
                              CMD_WITH_NO_DATA,
                              KEY_TYPE_LIST,
                              NOT_NEED_PREPARE_DATA,
                              CMD_TYPE_NONE,
                              KEY_HAS_MULTILEVEL,
                              HAS_NO_MULTIKEY,
                              SECOND_LEVEL_NOT_NEED_FIELD,
                              "0 100"},
    [CMD_TYPE_LRANGE_300] =  {"LRANGE_300",
                              "LRANGE",
                              CMD_NEED_KEY,
                              CMD_WITH_NO_DATA,
                              KEY_TYPE_LIST,
                              NOT_NEED_PREPARE_DATA,
                              CMD_TYPE_NONE,
                              KEY_HAS_MULTILEVEL,
                              HAS_NO_MULTIKEY,
                              SECOND_LEVEL_NOT_NEED_FIELD,
                              "0 300"},
    [CMD_TYPE_LRANGE_500] =  {"LRANGE_500",
                              "LRANGE",
                              CMD_NEED_KEY,
                              CMD_WITH_NO_DATA,
                              KEY_TYPE_LIST,
                              NOT_NEED_PREPARE_DATA,
                              CMD_TYPE_NONE,
                              KEY_HAS_MULTILEVEL,
                              HAS_NO_MULTIKEY,
                              SECOND_LEVEL_NOT_NEED_FIELD,
                              "0 500"},
    [CMD_TYPE_LRANGE_600] =  {"LRANGE_600",
                              "LRANGE",
                              CMD_NEED_KEY,
                              CMD_WITH_NO_DATA,
                              KEY_TYPE_LIST,
                              NOT_NEED_PREPARE_DATA,
                              CMD_TYPE_NONE,
                              KEY_HAS_MULTILEVEL,
                              HAS_NO_MULTIKEY,
                              SECOND_LEVEL_NOT_NEED_FIELD,
                              "0 600"},
    [CMD_TYPE_MSET] =  {"MSET",
                        "MSET",
                        CMD_NEED_KEY,
                        CMD_WITH_DATA,
                        KEY_TYPE_STRING,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_NO_MULTILEVEL,
                        HAS_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL}, 
    [CMD_TYPE_MGET] =  {"MGET",
                        "MGET",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_STRING,
                        NEED_PREPARE_DATA,
                        CMD_TYPE_MSET,
                        KEY_HAS_NO_MULTILEVEL,
                        HAS_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        NULL}, 
    [CMD_TYPE_HSET] =  {"HSET",
                        "HSET",
                        CMD_NEED_KEY,
                        CMD_WITH_DATA,
                        KEY_TYPE_HASH,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NEED_FIELD,
                        NULL}, 
    [CMD_TYPE_HGET] =  {"HGET",
                        "HGET",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_HASH,
                        NEED_PREPARE_DATA,
                        CMD_TYPE_HSET,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NEED_FIELD,
                        NULL},  
    [CMD_TYPE_HMSET] =  {"HMSET",
                        "HMSET",
                        CMD_NEED_KEY,
                        CMD_WITH_DATA,
                        KEY_TYPE_HASH,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_MULTILEVEL,
                        HAS_MULTIKEY,
                        SECOND_LEVEL_NEED_FIELD,
                        NULL}, 
    [CMD_TYPE_HMGET] =  {"HMGET",
                        "HMGET",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_HASH,
                        NEED_PREPARE_DATA,
                        CMD_TYPE_HMSET,
                        KEY_HAS_MULTILEVEL,
                        HAS_MULTIKEY,
                        SECOND_LEVEL_NEED_FIELD,
                        NULL},
    [CMD_TYPE_ZADD] =  {"ZADDMULTI_KEY",
                        "ZADD",
                        CMD_NEED_KEY,
                        CMD_WITH_DATA,
                        KEY_TYPE_ZSET,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_MULTILEVEL,
                        HAS_MULTIKEY,
                        SECOND_LEVEL_NEED_FIELD,
                        NULL}, 
    [CMD_TYPE_ZRANGE_100] =  {
                        "ZRANGE_100",
                        "ZRANGE",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_ZSET,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        "0 100"}, 
    [CMD_TYPE_ZRANGE_300] =  {
                        "ZRANGE_300",
                        "ZRANGE",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_ZSET,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        "0 300"},   
    [CMD_TYPE_ZRANGE_500] =  {
                        "ZRANGE_500",
                        "ZRANGE",
                        CMD_NEED_KEY,
                        CMD_WITH_NO_DATA,
                        KEY_TYPE_ZSET,
                        NOT_NEED_PREPARE_DATA,
                        CMD_TYPE_NONE,
                        KEY_HAS_MULTILEVEL,
                        HAS_NO_MULTIKEY,
                        SECOND_LEVEL_NOT_NEED_FIELD,
                        "0 500"},                         
};

/* Prototypes */
void sendCmdHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void readAckHandler(aeEventLoop *el, int fd, void *privdata, int mask);
client createClientWithCmd(unsigned int cmdtype, sds cmdext);
void createMissingClientsWithCmd(unsigned int cmdtype, sds cmdext);
void gen_prefix_cmd(sds *cmdbuf, int *prefixpend, int *prefixlen);
void gen_client_cmdbuf(client c, unsigned int cmdtype, sds cmdext);

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static int compareLatency(const void *a, const void *b) {
    return (*(long long*)a)-(*(long long*)b);
}

static void showLatencyReport(void) {
    int i = 0;
    int curlat = 0;
    float perc = 0.0;
    float reqpersec = 0.0;

    reqpersec = (float)config.totlatency/1000;
    if (reqpersec < 1.0) {
        reqpersec = 1.0;
    }
    reqpersec = (float)config.requests_finished / reqpersec;
    if (!config.quiet && !config.csv) {
        printf("====== %s ======\n", config.title);
        printf("  %d requests completed in %.2f seconds\n", config.requests_finished,
            (float)config.totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        printf("\n");

        qsort(config.latency,config.requests,sizeof(long long),compareLatency);
        for (i = 0; i < config.requests; i++) {
            if (config.latency[i]/1000 != curlat || i == (config.requests-1)) {
                curlat = config.latency[i]/1000;
                perc = ((float)(i+1)*100)/config.requests;
                printf("%.2f%% <= %d milliseconds\n", perc, curlat);
            }
        }
        printf("%.2f requests per second\n\n", reqpersec);
    } else if (config.csv) {
        printf("\"%s\",\"%.2f\"\n", config.title, reqpersec);
    } else {
        printf("%s: %.2f requests per second\n", config.title, reqpersec);
    }
}

/* Returns number of consumed options. */
int parseOptions(int argc, const char **argv) {
    int i = 0;
    int lastarg = 0;
    int exit_status = 1;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-c")) {
            if (lastarg) goto invalid;
            config.numclients = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-n")) {
            if (lastarg) goto invalid;
            config.requests = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-k")) {
            if (lastarg) goto invalid;
            config.keepalive = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-h")) {
            if (lastarg) goto invalid;
            config.hostip = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-p")) {
            if (lastarg) goto invalid;
            config.hostport = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-s")) {
            if (lastarg) goto invalid;
            config.hostsocket = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-a") ) {
            if (lastarg) goto invalid;
            config.auth = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-d")) {
            if (lastarg) goto invalid;
            config.datasize = atoi(argv[++i]);
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024*1024) config.datasize = 1024*1024*1024;
        } else if (!strcmp(argv[i],"-K")) {
            if (lastarg) goto invalid;
            config.keysize = atoi(argv[++i]);
            if (config.keysize < 6) {
                config.keysize=6;
            }
            if (config.keysize > 1024*1024) {
                config.keysize = 1024*1024;
            }
        } else if (!strcmp(argv[i],"-M")) {
            if (lastarg) goto invalid;
            config.mopnr = atoi(argv[++i]);
            if (config.mopnr <= 0) {
                config.mopnr=1;
            }
        } else if (!strcmp(argv[i],"-P")) {
            if (lastarg) goto invalid;
            config.pipeline = atoi(argv[++i]);
            if (config.pipeline <= 0) config.pipeline=1;
        } else if (!strcmp(argv[i],"-r")) {
            if (lastarg) goto invalid;
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[++i]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 10000;
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"--csv")) {
            config.csv = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idlemode = 1;
        } else if (!strcmp(argv[i],"-e")) {
            config.showerrors = 1;
        } else if (!strcmp(argv[i], "-D")) {
            config.display_realtime = 0;
        } else if (!strcmp(argv[i],"-t")) {
            if (lastarg) goto invalid;
            /* We get the list of tests to run as a string in the form
             * get,set,lrange,...,test_N. Then we add a comma before and
             * after the string in order to make sure that searching
             * for ",testname," will always get a match if the test is
             * enabled. */
            config.tests = sdsnew(",");
            config.tests = sdscat(config.tests,(char*)argv[++i]);
            config.tests = sdscat(config.tests,",");
            sdstolower(config.tests);
        } else if (!strcmp(argv[i],"--dbnum")) {
            if (lastarg) goto invalid;
            config.dbnum = atoi(argv[++i]);
            config.dbnumstr = sdsfromlonglong(config.dbnum);
        } else if (!strcmp(argv[i],"--help")) {
            exit_status = 0;
            goto usage;
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);

usage:
    printf(
"Usage: redis-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests]> [-k <boolean>]\n\n"
" -h <hostname>      Server hostname (default 127.0.0.1)\n"
" -p <port>          Server port (default 6379)\n"
" -s <socket>        Server socket (overrides host and port)\n"
" -a <password>      Password for Redis Auth\n"
" -c <clients>       Number of parallel connections (default 50)\n"
" -n <requests>      Total number of requests (default 100000)\n"
" -K <size>          Key size in bytes(default 10)\n"
" -M <numkey>        number of keys for operation such as mset,mget .etc\n" 
" -d <size>          Data size of SET/GET value in bytes (default 2)\n"
" -dbnum <db>        SELECT the specified db number (default 0)\n"
" -k <boolean>       1=keep alive 0=reconnect (default 1)\n"
" -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD\n"
"  Using this option the benchmark will expand the string __rand_int__\n"
"  inside an argument with a 12 digits number in the specified range\n"
"  from 0 to keyspacelen-1. The substitution changes every time a command\n"
"  is executed. Default tests use this to hit random keys in the\n"
"  specified range.\n"
" -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).\n"
" -e                 If server replies with errors, show them on stdout.\n"
"                    (no more than 1 error per second is displayed)\n"
" -q                 Quiet. Just show query/sec values\n"
" --csv              Output in CSV format\n"
" -l                 Loop. Run the tests forever\n"
" -t <tests>         Only run the comma separated list of tests. The test\n"
"                    names are the same as the ones produced as output.\n"
"                    test names include: ping,get,set,incr,lpush,rpush,lpop,\n"
"                    rpop,lrange,lrange_100,lrange_300,lrange_500,lrange_600,\n"
"                    mget,mset,hset,hget,hmset,hmget,sadd,spop,zadd,\n"
"                    zrange_100,zrange_300,zrange_500.\n"
" -I                 Idle mode. Just open N idle connections and wait.\n\n"
"Examples:\n\n"
" Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
"   $ redis-benchmark\n\n"
" Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:\n"
"   $ redis-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20\n\n"
" Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:\n"
"   $ redis-benchmark -t set -n 1000000 -r 100000000\n\n"
" Benchmark 127.0.0.1:6379 for a few commands producing CSV output:\n"
"   $ redis-benchmark -t ping,set,get -n 100000 --csv\n\n"
" Benchmark a specific command line:\n"
"   $ redis-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0\n\n"
" Fill a list with 10000 random elements:\n"
"   $ redis-benchmark -r 10000 -n 10000 lpush mylist __rand_int__\n\n"
" On user specified command lines __rand_int__ is replaced with a random integer\n"
" with a range of values selected by the -r option.\n"
    );
    exit(exit_status);
}

int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    if (!config.display_realtime) {
        return 250;
    }

    if (config.liveclients == 0) {
        fprintf(stderr,"All clients disconnected... aborting.\n");
        exit(1);
    }
    if (config.csv) return 250;
    if (config.idlemode == 1) {
        printf("clients: %d\r", config.liveclients);
        fflush(stdout);
	return 250;
    }
    float dt = (float)(mstime()-config.start)/1000.0;
    float rps = (float)config.requests_finished/dt;
    printf("%s: %.2f\r", config.title, rps);
    fflush(stdout);
    return 250; /* every 250ms */
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf+1,name,l);
    buf[l+1] = ',';
    buf[l+2] = '\0';
    return strstr(config.tests,buf) != NULL;
}

static void freeRedisClient(client c) {
    listNode *ln = NULL;
    aeDeleteFileEvent(config.el, c->context->fd, AE_WRITABLE);
    aeDeleteFileEvent(config.el, c->context->fd, AE_READABLE);
    redisFree(c->context);
    sdsfree(c->obuf);
    c->obuf = NULL;
    zfree(c);
    config.liveclients--;
    ln = listSearchKey(config.clients, c);
    assert(ln != NULL);
    listDelNode(config.clients, ln);
}

static void freeAllRedisClients(void) {
    listNode *ln = config.clients->head;
    listNode *next = NULL;

    while(ln) {
        next = ln->next;
        freeRedisClient(ln->value);
        ln = next;
    }
}

static void resetRedisClient(client c) {
    aeDeleteFileEvent(config.el, c->context->fd, AE_WRITABLE);
    aeDeleteFileEvent(config.el, c->context->fd, AE_READABLE);
    
    gen_client_cmdbuf(c, c->cmdtype, c->cmdext);
    aeCreateFileEvent(config.el, c->context->fd, AE_WRITABLE,
                      sendCmdHandler, c);
    c->written = 0;
    c->pending = c->lastpending;
}

static void redisClientDone(client c) {
    if (config.requests_finished == config.requests) {
        freeRedisClient(c);
        aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetRedisClient(c);
    } else {
        config.liveclients--;
        createMissingClientsWithCmd(c->cmdtype, c->cmdext);
        config.liveclients++;
        freeRedisClient(c);
    }
}

void readAckHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) {
        c->latency = ustime() - (c->start);
    }

    if (redisBufferRead(c->context) != REDIS_OK) {
        fprintf(stderr,"Error: %s\n",c->context->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (redisGetReply(c->context,&reply) != REDIS_OK) {
                fprintf(stderr, "Error: %s\n", c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                if (reply == (void*)REDIS_REPLY_ERROR) {
                    fprintf(stderr, "Unexpected error reply, exiting...\n");
                    exit(1);
                }

                if (config.showerrors) {
                    static time_t lasterr_time = 0;
                    time_t now = time(NULL);
                    redisReply *r = reply;
                    if (r->type == REDIS_REPLY_ERROR && lasterr_time != now) {
                        lasterr_time = now;
                        printf("Error from server: %s\n", r->str);
                    }
                }

                freeReplyObject(reply);
                
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        c->prefixlen = 0;
                    }
                    continue;
                }

                if (config.requests_finished < config.requests) {
                    config.latency[config.requests_finished++] = c->latency;
                }
                
                c->pending--;
                if (c->pending == 0) {
                    redisClientDone(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

void sendCmdHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Enforce upper bound to number of requests. */
        if (config.requests_issued++ >= config.requests) {
            freeRedisClient(c);
            return;
        }

        /* Really initialize: randomize keys and set start time. */
        c->start = ustime();
        c->latency = -1;
    }

    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf + c->written;
        ssize_t nwritten = write(c->context->fd,
                                 ptr, sdslen(c->obuf)-c->written);
        if (nwritten == -1) {
            if (errno != EPIPE) {
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            }    
            freeRedisClient(c);
            return;
        }
        
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(config.el, c->context->fd, AE_WRITABLE);
            aeCreateFileEvent(config.el, c->context->fd, AE_READABLE,
                              readAckHandler, c);
        }
    }
}

void analyze_ext_arg(char *cmdext, 
                     int *pargc, 
                     sds *argv, 
                     size_t *argvlen,
                     int usesdsnew,
                     unsigned char *needfreeargv)
{
    char *start = cmdext;
    char *p = NULL;
    int argc = *pargc;

    while((p = strsep(&start, " ")) != NULL) {        
        if (usesdsnew) {
            argv[argc] = sdsnew(p);
            needfreeargv[argc] = 1;
            argvlen[argc] = sdslen(argv[argc]);
            argc++;
        } else {
            argv[argc] = p;
            argvlen[argc++] = strlen(p);
        }        
    }

    *pargc = argc;
}

sds gen_client_cmdbuf_with_multikey(sds obuf, 
                                     unsigned int cmdtype,
                                     unsigned char keytype)
{
    int j = 0;
    int targnr = config.pipeline + 32;
    int argc = 0;
    sds argv[targnr];
    size_t argvlen[targnr];
    unsigned char needfreeargv[targnr];
    char *cmd = NULL;
    int len = 0;
    char *tmpcmdext = NULL;
    int keyprelen = strlen(keytype_pre[keytype].key_type_pre);
    int keyidxlen = config.keysize - keyprelen;

    argc = 0;
    memset(needfreeargv, 0, sizeof(unsigned char) * targnr);
        
    // command name
    argv[argc] = benchmark_set[cmdtype].cmd_name;
    argvlen[argc++] = strlen(benchmark_set[cmdtype].cmd_name);

    // command key
    if (benchmark_set[cmdtype].key_has_multilevel) {
        snprintf(config.keybuf, config.keysize + 1, 
             "%s%.*d", 
             keytype_pre[keytype].key_type_pre,
             keyidxlen, config.keyidx);                 
        argv[argc] = config.keybuf;
        argvlen[argc++] = strlen(config.keybuf);
    }
   
    for (j = 0; j < config.pipeline; j++) {
        if (!benchmark_set[cmdtype].key_has_multilevel) {
            snprintf(config.keybuf, config.keysize + 1, 
                 "%s%.*d", 
                 keytype_pre[keytype].key_type_pre,
                 keyidxlen, config.keyidx);
            argv[argc] = sdsnew(config.keybuf);
            needfreeargv[argc] = 1;
            argvlen[argc] = sdslen(argv[argc]);  
            argc++;

            if (config.randomkeys) {
                config.keyidx++;
            }
        } else if (benchmark_set[cmdtype].second_level_need_field) {
            // field
            snprintf(config.fieldbuf, config.keysize + 1, 
                 "%.*d", config.keysize, config.fieldidx);
            argv[argc] = sdsnew(config.fieldbuf);  
            needfreeargv[argc] = 1;
            argvlen[argc] = sdslen(argv[argc]);
            argc++;
            config.fieldidx++;
        } 

        // data
        if (!benchmark_set[cmdtype].need_prepare_data && 
            benchmark_set[cmdtype].cmd_with_data) {
            argv[argc] = config.data;  
            argvlen[argc++] = config.dlen;
        }

        if (benchmark_set[cmdtype].cmdext) {
            tmpcmdext = strdup(benchmark_set[cmdtype].cmdext);
            analyze_ext_arg(tmpcmdext, &argc, argv, argvlen, 1, needfreeargv);
            free(tmpcmdext);
        }       
    }

    len = redisFormatCommandArgv(&cmd, argc, (const char **)argv, argvlen);
    obuf = sdscatlen(obuf, cmd, len);
    free(cmd);

    for (j = 0; j < argc; j++) {
        if (needfreeargv[j]) {
            sdsfree(argv[j]);
        }
    }

    if (config.randomkeys) {
        config.keyidx++;
    }

    return obuf;
}

sds gen_client_cmdbuf_with_multicmd(sds obuf, 
                                     unsigned int cmdtype,
                                     unsigned char keytype)
{
    int j = 0;
    int argc = 0;
    sds argv[32];
    size_t argvlen[32];
    char *cmd = NULL;
    int len = 0;
    char *tmpcmdext = NULL;
    int keyprelen = strlen(keytype_pre[keytype].key_type_pre);
    int keyidxlen = config.keysize - keyprelen;
    int cmdnamelen = strlen(benchmark_set[cmdtype].cmd_name);
    
    for (j = 0; j < config.pipeline; j++) {
        argc = 0;
        
        // command name
        argv[argc] = benchmark_set[cmdtype].cmd_name;
        argvlen[argc++] = cmdnamelen;

        // command key
        snprintf(config.keybuf, config.keysize + 1, 
             "%s%.*d", 
             keytype_pre[keytype].key_type_pre,
             keyidxlen, config.keyidx);                 
        argv[argc] = config.keybuf;
        argvlen[argc++] = strlen(config.keybuf);

        // command key field
        if (benchmark_set[cmdtype].key_has_multilevel && 
            benchmark_set[cmdtype].second_level_need_field) {
            // field
            snprintf(config.fieldbuf, config.keysize + 1, 
                 "%.*d", 
                 config.keysize, config.fieldidx);
            argv[argc] = config.fieldbuf;  
            argvlen[argc++] = strlen(config.fieldbuf);
            config.fieldidx++;
        } 

        // data
        if (!benchmark_set[cmdtype].need_prepare_data && 
            benchmark_set[cmdtype].cmd_with_data) {
            argv[argc] = config.data;  
            argvlen[argc++] = config.dlen;
        }

        if (benchmark_set[cmdtype].cmdext) {
            tmpcmdext = strdup(benchmark_set[cmdtype].cmdext);
            analyze_ext_arg(tmpcmdext, &argc, argv, argvlen, 0, NULL);            
        }

        len = redisFormatCommandArgv(&cmd, argc, (const char **)argv, argvlen);
        obuf = sdscatlen(obuf, cmd, len);
        free(cmd);

        if (benchmark_set[cmdtype].cmdext) {
            free(tmpcmdext);
        }

        if (config.randomkeys) {
            config.keyidx++;
        }
    }

    return obuf;
}

void gen_client_cmdbuf(client c, unsigned int cmdtype, sds cmdext)
{
    int j = 0;
    unsigned char keytype = 0;
    sds obuf = sdsempty();

    sdsfree(c->obuf);
    c->obuf = NULL;

    c->cmdtype = cmdtype;
    c->cmdext = cmdext;
        
    gen_prefix_cmd(&obuf, &c->prefix_pending, &c->prefixlen);

    if (!benchmark_set[cmdtype].need_key) {
        /* Append the request itself. */
        for (j = 0; j < config.pipeline; j++) {
            obuf = sdscatfmt(obuf, "%s\r\n", benchmark_set[cmdtype].cmd_name);
        } 

        c->pending += config.pipeline;
        c->obuf = obuf;
        return;
    }

    if (cmdtype == CMD_TYPE_NONE) {
        for (j = 0; j < config.pipeline; j++) {
            obuf = sdscatsds(obuf, cmdext);
        }

        c->pending += config.pipeline;
        c->obuf = obuf;
        return;
    }

    keytype = benchmark_set[cmdtype].key_type;
    if (benchmark_set[cmdtype].has_multi_key) {
        obuf = gen_client_cmdbuf_with_multikey(obuf, cmdtype, keytype);
        c->pending += 1;
    } else {
        obuf = gen_client_cmdbuf_with_multicmd(obuf, cmdtype, keytype);
        c->pending += config.pipeline;
    }

    c->obuf = obuf;

    return;
}

client createClientWithCmd(unsigned int cmdtype, sds cmdext) {
    client c = zmalloc(sizeof(struct _client));

    if (config.hostsocket == NULL) {
        c->context = redisConnectNonBlock(config.hostip,config.hostport);
    } else {
        c->context = redisConnectUnixNonBlock(config.hostsocket);
    }
    if (c->context->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        if (config.hostsocket == NULL)
            fprintf(stderr, "%s:%d: %s\n",
                    config.hostip, config.hostport, c->context->errstr);
        else
            fprintf(stderr, "%s: %s\n", config.hostsocket, c->context->errstr);
        exit(1);
    }
    /* Suppress hiredis cleanup of unused buffers for max speed. */
    c->context->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->pending = 0;
    gen_client_cmdbuf(c, cmdtype, cmdext);
    //c->obuf = cmdbuf;
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    //c->prefix_pending = prefixpend;    
    //c->prefixlen = prefixlen;

    c->written = 0;
    c->pending += c->prefix_pending;
    c->lastpending = c->pending;
    c->randptr = NULL;
    c->randlen = 0;

    if (config.idlemode == 0) {
        aeCreateFileEvent(config.el, c->context->fd, 
                         AE_WRITABLE, sendCmdHandler,c);
    }
    listAddNodeTail(config.clients,c);
    config.liveclients++;
    return c;
}

void createMissingClientsWithCmd(unsigned int cmdtype, sds cmdext) {
    int n = 0;

    while(config.liveclients < config.numclients) {
        createClientWithCmd(cmdtype, cmdext);

        /* Listen backlog is quite limited on most systems */
        if (++n > 64) {
            usleep(50000);
            n = 0;
        }
    }
}

void gen_rand_data(char *data, int size)
{
    int c = 0;
    int i = 0;

    for (i = 0; i < size; i++) {
        c = (int)(rand() % 26);
        data[i] = 'a' + c;
    }
}

void gen_prefix_cmd(sds *cmdbuf, int *prefixpend, int *prefixlen)
{
    char *buf = NULL;
    int len = 0;
    int pend = 0;
    int plen = 0;
    
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    if (config.auth) {
        
        len = redisFormatCommand(&buf, "AUTH %s", config.auth);
        *cmdbuf = sdscatlen(*cmdbuf, buf, len);
        free(buf);
        pend++;
    }

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.dbnum != 0) {
        *cmdbuf = sdscatprintf(*cmdbuf,
                                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                                (int)sdslen(config.dbnumstr),
                                config.dbnumstr);                       
        pend++;
    }

    plen = sdslen(*cmdbuf);

    *prefixpend = pend;
    *prefixlen = plen;
}

void create_start_client_till_done(unsigned int cmdtype, 
                                   sds cmdext,
                                   int needshowlatency,
                                   int needfreeallclients)
{
    int keytype = 0;

    if (cmdtype == CMD_TYPE_NONE) {
        config.title = "NONE";
    } else {
        config.title = benchmark_set[cmdtype].cmd_title;
    }
    config.requests_issued = 0;
    config.requests_finished = 0;    
    config.keyidx = 0;
    config.fieldidx = 0;

    if (cmdtype != CMD_TYPE_NONE) {
        keytype = benchmark_set[cmdtype].key_type; 
        config.keyidxlen = config.keysize - 
                           strlen(keytype_pre[keytype].key_type_pre);
        config.keyidxlen = (config.keyidxlen < 0) ? 0 : config.keyidxlen;
    }

    createClientWithCmd(cmdtype, cmdext);                        
    createMissingClientsWithCmd(cmdtype, cmdext);

    config.start = mstime();
    aeMain(config.el, NULL, 1);
    config.totlatency = mstime() - config.start;
    
    if (needshowlatency) {
        showLatencyReport();
    }

    if (needfreeallclients) {
        freeAllRedisClients(); 
    }
}

void benchmark_by_type(unsigned int cmdtype)
{
    unsigned int prepare_cmdtype = 0;

    config.keyidx = 0;
    config.fieldidx = 0;
    
    if (cmdtype >= CMD_TYPE_NR) {
        printf("invlaid command type(%u) which must less then:%u\n",
                cmdtype, CMD_TYPE_NR);
        exit(1);
    }

    if (benchmark_set[cmdtype].need_prepare_data) {
        prepare_cmdtype = benchmark_set[cmdtype].prepare_data_cmd_type;
        if (prepare_cmdtype >= CMD_TYPE_NR) {
            printf("invlaid command type(%u) which must less then:%u\n",
                    prepare_cmdtype, CMD_TYPE_NR);
            exit(1);
        }

        create_start_client_till_done(prepare_cmdtype, NULL,
                        NOT_NEED_SHOW_LATENCY, NEED_FREE_ALL_CLIENTS); 
    }

    create_start_client_till_done(cmdtype, NULL, 
                        NEED_SHOW_LATENCY, NEED_FREE_ALL_CLIENTS); 
}

void key_buf_init(void)
{   
    config.keybuf = malloc(config.keysize + 1);
    if (!config.keybuf) {
        printf("allocate key buffer failed\n");
        exit(1);
    }  

    config.fieldbuf = malloc(config.keysize + 1);
    if (!config.fieldbuf) {
        printf("allocate field buffer failed\n");
        exit(1);
    } 
}

void config_init(void)
{
    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop(1024*10);
    aeCreateTimeEvent(config.el,1,showThroughput,NULL,NULL);
    config.keepalive = 1;
    config.keysize = 10;
    config.datasize = 3;
    config.pipeline = 1;
    config.showerrors = 0;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 10000;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.clients = listCreate();
    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.dbnum = 0;
    config.auth = NULL;
    config.mopnr = 1;
    config.display_realtime = 1;
}

void bench_loop(char *data)
{
    int oldrandomkeys = config.randomkeys;
    
    gen_rand_data(data, config.datasize);
    data[config.datasize] = '\0';
    config.data = data;
    config.dlen = strlen(config.data);

    if (test_is_selected("ping_inline") || test_is_selected("ping")) {
        benchmark_by_type(CMD_TYPE_PING_INLINE);
    }

    if (test_is_selected("set")) {
        benchmark_by_type(CMD_TYPE_SET);
    }

    if (test_is_selected("get")) {
        benchmark_by_type(CMD_TYPE_GET);            
    }

    if (test_is_selected("incr")) {
        benchmark_by_type(CMD_TYPE_INCR);
    }

    if (test_is_selected("lpush")) {
        benchmark_by_type(CMD_TYPE_LPUSH);
    }

    if (test_is_selected("rpush")) {
        benchmark_by_type(CMD_TYPE_RPUSH);
    }

    if (test_is_selected("lpop")) {
        benchmark_by_type(CMD_TYPE_LPOP);
    }

    if (test_is_selected("rpop")) {
        benchmark_by_type(CMD_TYPE_RPOP);
    }

    if (test_is_selected("sadd")) {
        benchmark_by_type(CMD_TYPE_SADD);
    }

    if (test_is_selected("spop")) {
        benchmark_by_type(CMD_TYPE_SPOP);
    }

    // FOR LIST LRANGE TEST, WE ONLY NEED TO DO IT ON SAME LIST
    if (test_is_selected("lrange") ||
        test_is_selected("lrange_100") ||
        test_is_selected("lrange_300") ||
        test_is_selected("lrange_500") ||
        test_is_selected("lrange_600"))
    {
        config.randomkeys = 0;
        benchmark_by_type(CMD_TYPE_LPUSH);
    }

    if (test_is_selected("lrange") || test_is_selected("lrange_100")) {
        benchmark_by_type(CMD_TYPE_LRANGE_100);
    }

    if (test_is_selected("lrange") || test_is_selected("lrange_300")) {
        benchmark_by_type(CMD_TYPE_LRANGE_300);
    }

    if (test_is_selected("lrange") || test_is_selected("lrange_500")) {
        benchmark_by_type(CMD_TYPE_LRANGE_500);
    }

    if (test_is_selected("lrange") || test_is_selected("lrange_600")) {
        benchmark_by_type(CMD_TYPE_LRANGE_600);
    }
    config.randomkeys = oldrandomkeys;

    if (test_is_selected("mset")) {
        benchmark_by_type(CMD_TYPE_MSET);
    }

    if (test_is_selected("mget")) {
        benchmark_by_type(CMD_TYPE_MGET);
    }

    if (test_is_selected("hset")) {
        benchmark_by_type(CMD_TYPE_HSET);
    }

    if (test_is_selected("hget")) {
        benchmark_by_type(CMD_TYPE_HGET);
    }

    if (test_is_selected("hmset")) {
        benchmark_by_type(CMD_TYPE_HMSET);
    }

    if (test_is_selected("hmget")) {
        benchmark_by_type(CMD_TYPE_HMGET);
    }

    if (test_is_selected("zadd")) {
        benchmark_by_type(CMD_TYPE_ZADD);
    }

    // FOR LIST LRANGE TEST, WE ONLY NEED TO DO IT ON SAME LIST
    if (test_is_selected("zrange") ||
        test_is_selected("zrange_100") ||
        test_is_selected("zrange_300") ||
        test_is_selected("zrange_500"))
    {
        config.randomkeys = 0;
        benchmark_by_type(CMD_TYPE_ZADD);
    }

    if (test_is_selected("zrange") || test_is_selected("zrange_100")) {
        benchmark_by_type(CMD_TYPE_ZRANGE_100);
    }

    if (test_is_selected("zrange") || test_is_selected("zrange_300")) {
        benchmark_by_type(CMD_TYPE_ZRANGE_300);
    }

    if (test_is_selected("zrange") || test_is_selected("zrange_500")) {
        benchmark_by_type(CMD_TYPE_ZRANGE_500);
    }

    if (!config.csv) printf("\n");
}

int main(int argc, const char **argv) {
    int i = 0;
    char *data = NULL;
    char *cmd = NULL;
    int len = 0;

    srandom(time(NULL));
    
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config_init();
    
    i = parseOptions(argc,argv);       
    argc -= i;
    argv += i;
    
    key_buf_init();

    config.latency = zmalloc(sizeof(long long)*config.requests);

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
        sds cmdobuf = sdsempty();
        create_start_client_till_done(CMD_TYPE_NONE, cmdobuf, 
                        NOT_NEED_SHOW_LATENCY, NOT_NEED_FREE_ALL_CLIENTS);        
        /* and will wait for every */
    }

    /* Run benchmark with command in the remainder of the arguments. */
    if (argc) {    
        sds title = sdsnew(argv[0]);
        for (i = 1; i < argc; i++) {
            title = sdscatlen(title, " ", 1);
            title = sdscatlen(title, (char*)argv[i], strlen(argv[i]));
        }

        do {
            len = redisFormatCommandArgv(&cmd,argc,argv,NULL);           
            sds cmdobuf = sdsnewlen(cmd, len);
            free(cmd);

            create_start_client_till_done(CMD_TYPE_NONE, cmdobuf, 
                                     NEED_SHOW_LATENCY, NEED_FREE_ALL_CLIENTS);
            sdsfree(cmdobuf);
        } while(config.loop--);

        sdsfree(title);

        return 0;
    }

    /* Run default benchmark suite. */    
    data = zmalloc(config.datasize+1); 
    do {
        bench_loop(data);
    } while(config.loop--);

    return 0;
}
