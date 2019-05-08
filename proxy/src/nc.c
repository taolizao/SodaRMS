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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "nc_commit.h"
#include "nc_core.h"
#include "nc_conf.h"
#include "nc_signal.h"
#include "nc_sentinel.h"


#define NC_CONF_PATH        "conf/nutcracker.yml"
#define NC_WHITELIST_PATH   "conf/whitelist"
#define NC_SLOW_LOG_PATH    "log/nutcracker_slow.log"

#define NC_LOG_DEFAULT      LOG_NOTICE
#define NC_LOG_MIN          LOG_EMERG
#define NC_LOG_MAX          LOG_PVERB
#define NC_LOG_PATH         NULL

#define NC_STATS_PORT       STATS_PORT
#define NC_STATS_ADDR       STATS_ADDR
#define NC_STATS_INTERVAL   STATS_INTERVAL

#define NC_SENTINEL_PORT    SENTINEL_PORT
#define NC_SENTINEL_ADDR    SENTINEL_ADDR

#define NC_SERVER_RECONNECT_INTERVAL    SERVER_RECONNECT_INTERVAL
#define NC_SERVER_ZERO_WARN_MSG_INTERVAL SERVER_ZERO_WARN_MSG_INTERVAL
#define NC_SERVER_ZERO_SLOW_CMD_COUNT_INTERVAL SERVER_ZERO_SLOW_CMD_COUNT_INTERVAL
#define NC_SLOW_QUERY_TIME_LIMIT SLOW_QUERY_TIME_LIMIT /* in msec */
#define NC_PID_FILE         NULL

#define NC_MBUF_SIZE        MBUF_SIZE
#define NC_MBUF_MIN_SIZE    MBUF_MIN_SIZE
#define NC_MBUF_MAX_SIZE    MBUF_MAX_SIZE

#define NC_MBUF_MEMUSE_DEFAULT	MBUF_MEMUSE_DEFAULT
#define NC_MBUF_MEMUSE_MAX      MBUF_MEMUSE_MAX
#define NC_MBUF_MEMUSE_MIN      MBUF_MEMUSE_MIN

static int show_help;
static int show_version;
static int test_conf;
static int daemonize;
static int describe_stats;

static struct option long_options[] = {
    { "help",               no_argument,        NULL,   'h' },
    { "version",            no_argument,        NULL,   'V' },
    { "test-conf",          no_argument,        NULL,   't' },
    { "daemonize",          no_argument,        NULL,   'd' },
    { "describe-stats",     no_argument,        NULL,   'D' },
    { "verbose",            required_argument,  NULL,   'v' },
    { "output",             required_argument,  NULL,   'o' },
    { "conf-file",          required_argument,  NULL,   'c' },
    { "whitelist-prefix",   required_argument,  NULL,   'w' },
    { "cmds-graylist",      required_argument,  NULL,   'g' },
    { "stats-port",         required_argument,  NULL,   's' },
    { "stats-interval",     required_argument,  NULL,   'i' },
    { "stats-addr",         required_argument,  NULL,   'a' },
    { "sentinel-port",      required_argument,  NULL,   'S' },
    { "server-interval",    required_argument,  NULL,   'I' },
    { "sentinel-addr",      required_argument,  NULL,   'A' },
    { "pid-file",           required_argument,  NULL,   'p' },
    { "mbuf-size",          required_argument,  NULL,   'm' },
    { "keepalive-timeout",  required_argument,  NULL,   'k' },
    { "slowcmd-time-limit", required_argument,  NULL,   'l' },
    { "slow-log",           required_argument,  NULL,   'L' },
    { "maxmemory",          required_argument,  NULL,   'M' },
    { NULL,                 0,                  NULL,    0  }
};

static char short_options[] = "hVtdDBv:o:c:w:s:i:a:S:I:A:p:m:k:l:L:M:g:b:";

static rstatus_t
nc_daemonize(int dump_core)
{
    rstatus_t status;
    pid_t pid, sid;
    int fd;

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return NC_ERROR;

    case 0:
        break;

    default:
        /* parent terminates */
        _exit(0);
    }

    /* 1st child continues and becomes the session leader */

    sid = setsid();
    if (sid < 0) {
        log_error("setsid() failed: %s", strerror(errno));
        return NC_ERROR;
    }

    if (signal(SIGHUP, SIG_IGN) == SIG_ERR) {
        log_error("signal(SIGHUP, SIG_IGN) failed: %s", strerror(errno));
        return NC_ERROR;
    }

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return NC_ERROR;

    case 0:
        break;

    default:
        /* 1st child terminates */
        _exit(0);
    }

    /* 2nd child continues */

    /* change working directory */
    if (dump_core == 0) {
        status = chdir("/");
        if (status < 0) {
            log_error("chdir(\"/\") failed: %s", strerror(errno));
            return NC_ERROR;
        }
    }

    /* clear file mode creation mask */
    umask(0);

    /* redirect stdin, stdout and stderr to "/dev/null" */

    fd = open("/dev/null", O_RDWR);
    if (fd < 0) {
        log_error("open(\"/dev/null\") failed: %s", strerror(errno));
        return NC_ERROR;
    }

    status = dup2(fd, STDIN_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDIN) failed: %s", fd, strerror(errno));
        close(fd);
        return NC_ERROR;
    }

    status = dup2(fd, STDOUT_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDOUT) failed: %s", fd, strerror(errno));
        close(fd);
        return NC_ERROR;
    }

    status = dup2(fd, STDERR_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDERR) failed: %s", fd, strerror(errno));
        close(fd);
        return NC_ERROR;
    }

    if (fd > STDERR_FILENO) {
        status = close(fd);
        if (status < 0) {
            log_error("close(%d) failed: %s", fd, strerror(errno));
            return NC_ERROR;
        }
    }

    return NC_OK;
}

static void
nc_print_run(struct instance *nci)
{
#ifdef COMMIT
    loga("non-slot-nutcracker-%s started on pid %d(commit:%s)", 
            NC_VERSION_STRING, nci->pid, COMMIT);
#else
    loga("non-slot-nutcracker-%s started on pid %d(commit:unknown)", 
            NC_VERSION_STRING, nci->pid);
#endif

    loga("run, rabbit run / dig that hole, forget the sun / "
         "and when at last the work is done / don't sit down / "
         "it's time to dig another one");
}

static void
nc_print_done(void)
{
    loga("done, rabbit done");
}

static void
nc_show_usage(void)
{
    log_stderr(
        "Usage: non-slot-nutcracker [-?hVdDt] [-v verbosity level] [-o output file]" CRLF
        "                  [-c conf file] [-s stats port] [-a stats addr]" CRLF
        "                  [-i stats interval] [-S sentinel port]" CRLF
        "                  [-I server reconnect interval] [-A sentinel addr]" CRLF
        "                  [-p pid file] [-m mbuf size] [-M maxmemory]" CRLF
        "                  [-w whitelist prefix]" CRLF
        "                  [-g cmds gray list]" CRLF
        "                  [-l slow query time limit]" CRLF
        "                  [-L slow log file]" CRLF
        "                  [-b commands allowed for broadcast command]" CRLF
        "                  [-B commands all allowed for broadcast]" CRLF
        "");
    log_stderr(
        "Options:" CRLF
        "  -h, --help                : this help" CRLF
        "  -V, --version             : show version and exit" CRLF
        "  -t, --test-conf           : test configuration for syntax errors and exit" CRLF
        "  -d, --daemonize           : run as a daemon" CRLF
        "  -D, --describe-stats      : print stats description and exit");
    log_stderr(
        "  -v, --verbosity=N         : set logging level (default: %d, min: %d, max: %d)" CRLF
        "  -o, --output=S            : set logging file (default: %s)" CRLF
        "  -c, --conf-file=S         : set configuration file (default: %s)" CRLF
        "  -s, --stats-port=N        : set stats monitoring port (default: %d)" CRLF
        "  -a, --stats-addr=S        : set stats monitoring ip (default: %s)" CRLF
        "  -i, --stats-interval=N    : set stats aggregation interval in msec (default: %d msec)" CRLF
        "  -S, --sentinel-port=N     : set sentinel server port (default: %d)" CRLF
        "  -A, --sentinel-addr=S     : set sentinel server ip (default: %s)" CRLF
        "  -I, --server-interval=N   : set server reconnect interval in msec (default: %d msec)" CRLF
        "  -p, --pid-file=S          : set pid file (default: %s)" CRLF
        "  -m, --mbuf-size=N         : set size of mbuf chunk in bytes (default: %d bytes)" CRLF
        "  -M, --maxmemory=N <MB>    : set memory limit (default: %ld, min: %ld, max:%ld)"CRLF
        "  -w, --whitelist-prefix=S  : set whitelist prefix (default :%s)" CRLF
        "  -g, --cmds-graylist=S     : set graylist filepath(black or white list)" CRLF
        "  -l, --slowcmd-time-limit=N: set slow query time limit in msec (default :%d msec)" CRLF
        "  -L, --slow-log=S          : set slow log file (default :%s)" CRLF
        "  -b, --broadcast_cmds=S    : set commands for broadcast (default :lpop,rpop)" CRLF
        "  -B, --broadcast_all       : allow all commands to run" CRLF
        "",
        NC_LOG_DEFAULT, NC_LOG_MIN, NC_LOG_MAX,
        NC_LOG_PATH != NULL ? NC_LOG_PATH : "stderr",
        NC_CONF_PATH,
        NC_STATS_PORT, NC_STATS_ADDR, NC_STATS_INTERVAL,
        NC_SENTINEL_PORT, NC_SENTINEL_ADDR, NC_SERVER_RECONNECT_INTERVAL,
        NC_PID_FILE != NULL ? NC_PID_FILE : "off",
        NC_MBUF_SIZE, 
        NC_MBUF_MEMUSE_DEFAULT, NC_MBUF_MEMUSE_MIN, NC_MBUF_MEMUSE_MAX,
        NC_WHITELIST_PATH, NC_SLOW_QUERY_TIME_LIMIT, NC_SLOW_LOG_PATH);
}


static rstatus_t
nc_create_pidfile(struct instance *nci)
{
    char pid[NC_UINTMAX_MAXLEN];
    int fd, pid_len;
    ssize_t n;

    fd = open(nci->pid_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        log_error("opening pid file '%s' failed: %s", nci->pid_filename,
                  strerror(errno));
        return NC_ERROR;
    }
    nci->pidfile = 1;

    pid_len = nc_snprintf(pid, NC_UINTMAX_MAXLEN, "%d", nci->pid);

    n = nc_write(fd, pid, pid_len);
    if (n < 0) {
        log_error("write to pid file '%s' failed: %s", nci->pid_filename,
                  strerror(errno));
        return NC_ERROR;
    }

    close(fd);

    return NC_OK;
}

static void
nc_remove_pidfile(struct instance *nci)
{
    int status;

    status = unlink(nci->pid_filename);
    if (status < 0) {
        log_error("unlink of pid file '%s' failed, ignored: %s",
                  nci->pid_filename, strerror(errno));
    }
}

/*****************************************************************************************
++ Initialize default values for nci instance
******************************************************************************************/
static void
nc_set_default_options(struct instance *nci)
{
    int i, j, status;

    nci->ctx = NULL;

    nci->log_level = NC_LOG_DEFAULT;
    nci->log_filename = NC_LOG_PATH;

    nci->conf_filename = NC_CONF_PATH;
    nci->whitelist_prefix = NC_WHITELIST_PATH;
    nci->slow_log_filename = NC_SLOW_LOG_PATH;

    nci->stats_port = NC_STATS_PORT;
    nci->stats_addr = NC_STATS_ADDR;
    nci->stats_interval = NC_STATS_INTERVAL;
    nci->slow_query_time_limit = NC_SLOW_QUERY_TIME_LIMIT;

    nci->sentinel_port = NC_SENTINEL_PORT;
    nci->sentinel_addr = NC_SENTINEL_ADDR;
    nci->server_reconnect_interval = NC_SERVER_RECONNECT_INTERVAL;
    nci->zero_warn_msg_interval = NC_SERVER_ZERO_WARN_MSG_INTERVAL;
    nci->zero_slow_cmd_count_interval = NC_SERVER_ZERO_SLOW_CMD_COUNT_INTERVAL;

    status = nc_gethostname(nci->hostname, NC_MAXHOSTNAMELEN);
    if (status < 0) {
        log_warn("gethostname failed, ignored: %s", strerror(errno));
        nc_snprintf(nci->hostname, NC_MAXHOSTNAMELEN, "unknown");
    }
    nci->hostname[NC_MAXHOSTNAMELEN - 1] = '\0';

    nci->mbuf_chunk_size = NC_MBUF_SIZE;
    nci->maxmemory = NC_MBUF_MEMUSE_DEFAULT;

    nci->pid = (pid_t)-1;
    nci->pid_filename = NULL;
    nci->pidfile = 0;
    nci->whitelist = 0;
    nci->graylist = NULL;
    nci->slow_log_switch = 0;
    nci->gray_idx = 0;
    for(i = 0; i < 2; i++) {
        for(j = 0; j < MSG_REQ_REDIS_NUM; j++)
        nci->cmd_gray[i][j] = REDIS_CMDS_WHITE;
    }

    memset(&nci->broadcastcmds, 0, sizeof(nci->broadcastcmds));
    snprintf(nci->broadcastcmds.bcmdinfo, sizeof(nci->broadcastcmds.bcmdinfo), 
             "rpop,lpop");
    nci->broadcastcmds.bcmdallallow = 0;
}

#define CLIENT_DEAULT_KEEPALIVE 60

/*****************************************************************************************
++ analyze startup arguments
******************************************************************************************/
static rstatus_t nc_analyze_bcmds(struct instance *nci)
{
    char *tmppos = nci->broadcastcmds.bcmdinfo;
    int i = 0;

    nci->broadcastcmds.bcmdnr = 0;
    for (i = 0; i < REDIS_BCMD_SET_MAXNR; i++) {
        nci->broadcastcmds.bcmdset[i] = strsep(&tmppos, ",");
        if (!nci->broadcastcmds.bcmdset[i]) {
            break;
        }

        nci->broadcastcmds.bcmdnr++;
    }
}

static rstatus_t
nc_get_options(int argc, char **argv, struct instance *nci)
{
    int c, value;

    opterr = 0;

    nci->client_tcp_keepalive_timeout = CLIENT_DEAULT_KEEPALIVE;

    for (;;) {
        c = getopt_long(argc, argv, short_options, long_options, NULL);
        if (c == -1) {
            /* no more options */
            break;
        }

        switch (c) {
        case 'h':
            show_version = 1;
            show_help = 1;
            break;

        case 'V':
            show_version = 1;
            break;

        case 't':
            test_conf = 1;
            break;

        case 'd':
            daemonize = 1;
            break;

        case 'D':
            describe_stats = 1;
            show_version = 1;
            break;

        case 'v':
            value = nc_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("nutcracker: option -v requires a number");
                return NC_ERROR;
            }
            nci->log_level = value;
            break;

        case 'o':
            nci->log_filename = optarg;
            break;

        case 'c':
            nci->conf_filename = optarg;
            break;

        case 'w':
            nci->whitelist = 1;
            nci->whitelist_prefix = optarg;
            break;

        case 'g':
            nci->graylist = optarg;
            break;
            
        case 'L':
            nci->slow_log_switch = 1;
            nci->slow_log_filename= optarg;
            break;

        case 'b':
            snprintf(nci->broadcastcmds.bcmdinfo, 
                     sizeof(nci->broadcastcmds.bcmdinfo), "%s", optarg);                    
            break;

        case 'B':
            nci->broadcastcmds.bcmdallallow = 1;
            break;
            
        case 's':
            value = nc_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("nutcracker: option -s requires a number");
                return NC_ERROR;
            }
            if (!nc_valid_port(value)) {
                log_stderr("nutcracker: option -s value %d is not a valid "
                           "port", value);
                return NC_ERROR;
            }

            nci->stats_port = (uint16_t)value;
            break;

        case 'i':
            value = nc_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("nutcracker: option -i requires a number");
                return NC_ERROR;
            }

            nci->stats_interval = value;
            break;

        case 'l':
            value = nc_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("nutcracker: option -i requires a number");
                return NC_ERROR;
            }

            nci->slow_query_time_limit = value;
            break;

        case 'a':
            nci->stats_addr = optarg;
            break;

        case 'S':
            value = nc_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("nutcracker: option -S requires a number");
                return NC_ERROR;
            }
            if (!nc_valid_port(value)) {
                log_stderr("nutcracker: option -S value %d is not a valid "
                           "port", value);
                return NC_ERROR;
            }

            nci->sentinel_port = (uint16_t)value;
            break;

        case 'I':
            value = nc_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("nutcracker: option -I requires a number");
                return NC_ERROR;
            }

            nci->server_reconnect_interval = value;
            break;

        case 'A':
            nci->sentinel_addr = optarg;
            break;

        case 'p':
            nci->pid_filename = optarg;
            break;

        case 'm':
            value = nc_atoi(optarg, strlen(optarg));
            if (value <= 0) {
                log_stderr("nutcracker: option -m requires a non-zero number");
                return NC_ERROR;
            }

            if (value < NC_MBUF_MIN_SIZE || value > NC_MBUF_MAX_SIZE) {
                log_stderr("nutcracker: mbuf chunk size must be between %zu and"
                           " %zu bytes", NC_MBUF_MIN_SIZE, NC_MBUF_MAX_SIZE);
                return NC_ERROR;
            }

            nci->mbuf_chunk_size = (size_t)value;
            break;

        case 'M':
            value = nc_atoi(optarg, strlen(optarg));
            if(value <= 0) {
                log_stderr("nutcracker: option -M requires a number");
                return NC_ERROR;
            } else if((unsigned long)value < NC_MBUF_MEMUSE_MIN || 
                    (unsigned long)value > NC_MBUF_MEMUSE_MAX) {
                log_stderr("nutcracker: maxmemory must be betwwen %lu and %lu MB", 
                        NC_MBUF_MEMUSE_MIN, NC_MBUF_MEMUSE_MAX);
                return NC_ERROR;
            }
            nci->maxmemory = (size_t)value;
            break;


        case 'k':
             nci->client_tcp_keepalive_timeout = atoi(optarg);
             break;

        case '?':
            switch (optopt) {
            case 'o':
            case 'c':
            case 'w':
            case 'p':
            case 'L':
                log_stderr("nutcracker: option -%c requires a file name",
                           optopt);
                break;

            case 'm':
            case 'v':
            case 's':
            case 'i':
            case 'S':
            case 'I':
            case 'l':
                log_stderr("nutcracker: option -%c requires a number", optopt);
                break;

            case 'a':
            case 'A':
                log_stderr("nutcracker: option -%c requires a string", optopt);
                break;

            default:
                log_stderr("nutcracker: invalid option -- '%c'", optopt);
                break;
            }
            return NC_ERROR;

        default:
            log_stderr("nutcracker: invalid option -- '%c'", optopt);
            return NC_ERROR;

        }
    }

    nc_analyze_bcmds(nci); 

    return NC_OK;
}

/*
 * Returns true if configuration file has a valid syntax, otherwise
 * returns false
 */
static bool
nc_test_conf(struct instance *nci)
{
    struct conf *cf;

    cf = conf_create(nci->conf_filename);
    if (cf == NULL) {
        log_stderr("nutcracker: configuration file '%s' syntax is invalid",
                   nci->conf_filename);
        return false;
    }

    conf_destroy(cf);

    log_stderr("nutcracker: configuration file '%s' syntax is ok",
               nci->conf_filename);
    return true;
}

static rstatus_t
nc_pre_run(struct instance *nci)
{
    rstatus_t status;

    status = log_init(nci->log_level, nci->log_filename, nci);
    if (status != NC_OK) {
        return status;
    }

    if (daemonize) {
        status = nc_daemonize(1);
        if (status != NC_OK) {
            return status;
        }
    }

    nci->pid = getpid();

    status = signal_init();
    if (status != NC_OK) {
        return status;
    }

    if (nci->pid_filename) {
        status = nc_create_pidfile(nci);
        if (status != NC_OK) {
            return status;
        }
    }

    nc_print_run(nci);

    return NC_OK;
}

static void
nc_post_run(struct instance *nci)
{
    if (nci->pidfile) {
        nc_remove_pidfile(nci);
    }

    signal_deinit();

    nc_print_done();

    log_deinit();
}

static int
warn_msg_reset(struct context *ctx, long long id, void *clientData)
{
    struct server_pool *pool = NULL;
    uint32_t i, npool;

    NUT_NOTUSED(id);
    NUT_NOTUSED(clientData);

    /* reset the warning cnt to zero every 2 minutes */
    for (i = 0, npool = array_n(&ctx->pool); i < npool; i++) {
        pool = (struct server_pool *)array_get(&ctx->pool, i);

        pool->forbidden_counter = 0;
        pool->perm_warning = 0;
        pool->connection_is_full = 0;
        pool->quota_is_insufficient = 0;
    }

    return ctx->zero_warn_msg_interval;
}

static int
slow_cmd_count_reset(struct context *ctx, long long id, void *clientData)
{
    NUT_NOTUSED(id);
    NUT_NOTUSED(clientData);

    ctx->slow_cmd_count = 0;

    return ctx->zero_slow_cmd_count_interval;
}

static void
nc_run(struct instance *nci)
{
    rstatus_t status;
    struct context *ctx;

    ctx = core_start(nci);
    if (ctx == NULL) {
        return;
    }

    /* reset the warning msg and cnt at intervals */
    event_add_timer(ctx, (long long)ctx->zero_warn_msg_interval, warn_msg_reset, NULL, NULL);

    /* reset the slow command count to 0 at intervals */
    event_add_timer(ctx, (long long)ctx->zero_slow_cmd_count_interval, \
                    slow_cmd_count_reset, NULL, NULL);

    /* whitelist thread */
    nc_get_whitelist(nci);
    /* run rabbit run */
    for (;;) {
        status = core_loop(ctx);
        if (status != NC_OK) {
            break;
        }
    }

    core_stop(ctx);
}

struct instance nci;

struct instance *get_nci(void)
{
    return &nci;
}

int __attribute__((weak))
main(int argc, char **argv)
{
    rstatus_t status;

    // set default values for nci
    nc_set_default_options(&nci);

    // analyze startup command arguments
    status = nc_get_options(argc, argv, &nci);
    if (status != NC_OK) {
        nc_show_usage();
        exit(1);
    }

    // show help information of usage and stats
    // for '-V' or '-h' or '-D' arguments
    if (show_version) {
#ifdef COMMIT
        log_stderr("This is non-slot-nutcracker-%s commit:%s" CRLF, NC_VERSION_STRING, COMMIT);
#else
        log_stderr("This is non-slot-nutcracker-%s commit:unknown" CRLF, NC_VERSION_STRING);
#endif
        // for '-h' argument
        if (show_help) {
            nc_show_usage();
        }

        // for '-D' argument
        if (describe_stats) {
            stats_describe();
        }

        exit(0);
    }

    // check contents of configuration file
    if (test_conf) {
        if (!nc_test_conf(&nci)) {
            exit(1);
        }
        exit(0);
    }

    status = nc_pre_run(&nci);
    if (status != NC_OK) {
        nc_post_run(&nci);
        exit(1);
    }

    nc_run(&nci);

    nc_post_run(&nci);

    exit(1);
}
