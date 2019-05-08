/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_CORE_H_
#define _NC_CORE_H_

#include "config.h"

#ifdef HAVE_DEBUG_LOG
# define NC_DEBUG_LOG 1
#endif

#ifdef HAVE_ASSERT_PANIC
# define NC_ASSERT_PANIC 1
#endif

#ifdef HAVE_ASSERT_LOG
# define NC_ASSERT_LOG 1
#endif

#ifdef HAVE_STATS
# define NC_STATS 1
#else
# define NC_STATS 0
#endif

#ifdef HAVE_LITTLE_ENDIAN
# define NC_LITTLE_ENDIAN 1
#endif

#define NC_OK        0
#define NC_ERROR    -1
#define NC_EAGAIN   -2
#define NC_ENOMEM   -3

typedef int rstatus_t; /* return type */
typedef int err_t;     /* error type */

struct array;
struct string;
struct context;
struct conn;
struct conn_tqh;
struct msg;
struct msg_tqh;
struct server;
struct server_pool;
struct mbuf;
struct mhdr;
struct conf;
struct stats;
struct epoll_event;
struct instance;

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>

#include "nc_array.h"
#include "nc_string.h"
#include "nc_queue.h"
#include "nc_rbtree.h"
#include "nc_log.h"
#include "nc_util.h"
#include "nc_stats.h"
#include "nc_mbuf.h"
#include "nc_message.h"
#include "nc_connection.h"
#include "nc_event.h"
#include "nc_whitelist.h"
#include "nc_server.h"

struct instance;
struct stats;

struct context {
    struct instance    *nci;
    uint32_t           id;                /* unique context id */
    struct conf        *cf;               /* configuration */
    struct stats       *stats;            /* stats */

    struct array       pool;              /* server_pool[] */
    struct server      *sentinel;         /* sentinel server */
    uint64_t           server_reconnect_interval; /* server reconnect interval in msec */

    int                ep;                /* epoll device */
    int                nevent;            /* # epoll event */
    int                max_timeout;       /* epoll wait max timeout in msec */
    int                timeout;           /* epoll wait timeout in msec */
    struct epoll_event *event;            /* epoll event */

    time_t             last_time;         /* Used to detect system clock skew */
    struct time_event  *time_event_head;
    long long          time_event_next_id;

    unsigned           whitelist:1;       /* whitelist switch;1:ON, 0:OFF */
    uint64_t           zero_warn_msg_interval; /* reset the waring msg(forbidden msg, 
                                                  token used up msg and so on) and cnt to 0 */
    uint64_t           zero_slow_cmd_count_interval; /* reset the slow command count to 0 */
    uint64_t           slow_cmd_count; /* slow command count in zero_slow_cmd_count_interval */
};

#define REDIS_CMDS_BLACK        0
#define REDIS_CMDS_WHITE    1

#define REDIS_BCMD_SET_MAXNR 256

typedef struct broadcast_cmdset {
    char bcmdinfo[2048];
    char *bcmdset[REDIS_BCMD_SET_MAXNR];
    int  bcmdnr;
    int  bcmdallallow;
} broadcast_cmdset_t;

struct instance {
    struct context  *ctx;                        /* active context */
    int             log_level;                   /* log level */
    char            *log_filename;               /* log filename */
    char            *conf_filename;              /* configuration filename */
    uint16_t        stats_port;                  /* stats monitoring port */
    int             stats_interval;              /* stats aggregation interval */
    int             slow_query_time_limit;       /* slow query time limit */
    char            *stats_addr;                 /* stats monitoring addr */
    uint16_t        sentinel_port;               /* sentinel server listen port */
    char            *sentinel_addr;              /* sentinel server addr */
    uint64_t        server_reconnect_interval;   /* server reconnect interval */
    char            hostname[NC_MAXHOSTNAMELEN]; /* hostname */
    size_t          mbuf_chunk_size;             /* mbuf chunk size */
    size_t          maxmemory;                   /* max memory*/
    pid_t           pid;                         /* process id */
    char            *pid_filename;               /* pid filename */
    unsigned        pidfile:1;                   /* pid file created? */
    unsigned        whitelist:1;                 /* whitelist switch */
    char            *whitelist_prefix;           /* whitelist prefix */
    char            *graylist;
    unsigned        slow_log_switch:1;           /* slow log switch */
    char            *slow_log_filename;          /* slow log filename */
    uint64_t        zero_warn_msg_interval;      /* reset the waring msg(forbidden msg, 
                                                    token used up msg and so on) and cnt to 0 */
    uint64_t        zero_slow_cmd_count_interval; /* reset the slow command count to 0 */
    int             client_tcp_keepalive_timeout;   
    volatile int    gray_idx;
    int             cmd_gray[2][MSG_REQ_REDIS_NUM];
    broadcast_cmdset_t broadcastcmds;
};

struct context *core_start(struct instance *nci);
void core_stop(struct context *ctx);
rstatus_t core_loop(struct context *ctx);

struct instance *get_nci(void);

extern int mem_exceed_soft_limit;
#define NUT_PIPELINE_LIMIT  100

#endif

