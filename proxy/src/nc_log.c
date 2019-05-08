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

#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "nc_core.h"

#define BUF_MAX_LEN (2 * LOG_MAX_LEN)
#define LOG_HEADER_LEN 100
#define MAX_SLOW_CMD_COUNT_IN_TEN_SEC 100

static struct logger logger;
static char *buf;


int
log_init(int level, char *name, struct instance *nci)
{
    struct logger *l = &logger;

    l->level = MAX(LOG_EMERG, MIN(level, LOG_PVERB));
    l->name = name;
    if (name == NULL || !strlen(name)) {
        l->fd = STDERR_FILENO;
    } else {
        l->fd = open(name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->fd < 0) {
            log_stderr("opening log file '%s' failed: %s", name,
                       strerror(errno));
            return -1;
        }
    }

    l->slow_log_filename = name = nci->slow_log_filename;
    if(nci->slow_log_switch != 0){
        l->slow_log_threshold = nci->slow_query_time_limit;
        if (name == NULL || !strlen(name)) {
            l->slow_log_fd = -1;
        } else {
            l->slow_log_fd = open(name, O_WRONLY | O_APPEND | O_CREAT, 0644);
            if (l->slow_log_fd < 0) {
                log_stderr("opening slow log file '%s' failed: %s", name,
                           strerror(errno));
                return -1;
            }
        }
    } else {
        l->slow_log_fd = -1;
        l->slow_log_threshold = SLOW_QUERY_TIME_LIMIT;
    }

    buf = nc_alloc(BUF_MAX_LEN); /* init slow slog write buffer, so we have no need to
                                    malloc buffer every time */

    return 0;
}

void
log_deinit(void)
{
    struct logger *l = &logger;

    if (l->fd < 0 || l->fd == STDERR_FILENO) {
        return;
    }

    close(l->fd);
}

void
log_reopen(void)
{
    struct logger *l = &logger;

    if (l->fd != STDERR_FILENO) {
        close(l->fd);
        close(l->slow_log_fd);
        l->fd = open(l->name, O_WRONLY | O_APPEND | O_CREAT, 0644);
        l->slow_log_fd = open(l->slow_log_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (l->fd < 0 || l->slow_log_fd < 0) {
            log_stderr_safe("reopening log file '%s' failed, ignored: %s", l->name,
                       strerror(errno));
        }
    }
}

void
log_level_up(void)
{
    struct logger *l = &logger;

    if (l->level < LOG_PVERB) {
        l->level++;
        log_safe("up log level to %d", l->level);
    }
}

void
log_level_down(void)
{
    struct logger *l = &logger;

    if (l->level > LOG_EMERG) {
        l->level--;
        log_safe("down log level to %d", l->level);
    }
}

void
log_level_set(int level)
{
    struct logger *l = &logger;

    l->level = MAX(LOG_EMERG, MIN(level, LOG_PVERB));
    loga("set log level to %d", l->level);
}

int
log_loggable(int level)
{
    struct logger *l = &logger;

    if (level > l->level) {
        return 0;
    }

    return 1;
}

int
log_slow(struct msg *pmsg, uint64_t msg_dealing_time,
         struct context *ctx, struct server *server){
    struct logger *l = &logger;
    int len = 0;
    int errno_save = 0;
    struct tm *local = NULL;
    time_t t = 0;
    char *timestr = NULL;
    ssize_t n = 0;

    errno_save = errno;
    
    NUT_NOTUSED(server);

    if(l->slow_log_fd > 0 && 
       ctx->slow_cmd_count <= MAX_SLOW_CMD_COUNT_IN_TEN_SEC &&
       msg_dealing_time > (uint64_t)(l->slow_log_threshold * 1000)) {
        ctx->slow_cmd_count++;
        size_t buf_len = BUF_MAX_LEN;
        memset(buf, 0, buf_len);
        
        t = time(NULL);
        local = localtime(&t);
        timestr = asctime(local);
        timestr[strlen(timestr) - 1] = '\0';

        len += nc_snprintf(buf + len, buf_len - len, "[%s]", timestr);
        len += nc_snprintf(buf + len, buf_len - len, "[%fms] ",
                           ((double)msg_dealing_time / 1000));

        nc_memcpy(buf + len, pmsg->dump_data, pmsg->dump_len);
        len += pmsg->dump_len;
        buf[len++] = '\n';

        n = nc_write(l->slow_log_fd, buf, len);
        if (n < 0) {
            l->nerror++;
            return -1;
        }
        errno = errno_save;
        //stats_server_incr_by(ctx, server, slow_cmd_count, 1);
    }
    return 0;
}

/*int*/
/*slow_log_hexdump(char *buf, struct msg *pmsg){*/
    /*char *data = (char *)pmsg->narg_start;*/
    /*int datalen = pmsg->pos - pmsg->narg_start;*/
    /*int i = 0;*/
    /*int off = 0;*/
    /*int len = 0;*/
    /*int size = 0;*/

    /*[> slow log hexdump <]*/
    /*off = 0;                  [> data offset <]*/
    /*len = 0;                  [> length of output buffer <]*/
    /*size = BUF_MAX_LEN - LOG_HEADER_LEN;   [> size of output buffer <]*/

    /*while (datalen != 0 && (len < size - 1)) {*/
        /*char *save = NULL;*/
        /*char *str = NULL;*/
        /*unsigned char c = 0;*/
        /*int savelen = 0;*/

        /*len += nc_scnprintf(buf + len, size - len, "%08x  ", off);*/

        /*save = data;*/
        /*savelen = datalen;*/

        /*for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {*/
            /*c = (unsigned char)(*data);*/
            /*str = (i == 7) ? "  " : " ";*/
            /*len += nc_scnprintf(buf + len, size - len, "%02x%s", c, str);*/
        /*}*/
        /*for (; i < 16; i++) {*/
            /*str = (i == 7) ? "  " : " ";*/
            /*len += nc_scnprintf(buf + len, size - len, "  %s", str);*/
        /*}*/

        /*data = save;*/
        /*datalen = savelen;*/

        /*len += nc_scnprintf(buf + len, size - len, "  |");*/

        /*for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {*/
            /*c = (unsigned char)(isprint(*data) ? *data : '.');*/
            /*len += nc_scnprintf(buf + len, size - len, "%c", c);*/
        /*}*/
        /*len += nc_scnprintf(buf + len, size - len, "|\n");*/

        /*off += 16;*/
    /*}*/

    /*buf[len - 1] = '\n';*/
    /*return len;*/
/*}*/

void
_log(const char *file, int line, int panic, const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char buf[LOG_MAX_LEN], *timestr;
    va_list args;
    struct tm *local;
    time_t t;
    ssize_t n;

    if (l->fd < 0) {
        return;
    }

    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    t = time(NULL);
    local = localtime(&t);
    timestr = asctime(local);

    len += nc_scnprintf(buf + len, size - len, "[%.*s] %s:%d ",
                        strlen(timestr) - 1, timestr, file, line);

    va_start(args, fmt);
    len += nc_vscnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(l->fd, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;

    if (panic) {
        abort();
    }
}

void
_log_stderr(const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char buf[8 * LOG_MAX_LEN];
    va_list args;
    ssize_t n;

    errno_save = errno;
    len = 0;                /* length of output buffer */
    size = 8 * LOG_MAX_LEN; /* size of output buffer */

    va_start(args, fmt);
    len += nc_vscnprintf(buf, size, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(STDERR_FILENO, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}

/*
 * Hexadecimal dump in the canonical hex + ascii display
 * See -C option in man hexdump
 */
void
_log_hexdump(const char *file, int line, char *data, int datalen,
             const char *fmt, ...)
{
    struct logger *l = &logger;
    char buf[8 * LOG_MAX_LEN];
    int i, off, len, size, errno_save;
    ssize_t n;

    NUT_NOTUSED(file);
    NUT_NOTUSED(line);
    NUT_NOTUSED(fmt);

    if (l->fd < 0) {
        return;
    }

    /* log hexdump */
    errno_save = errno;
    off = 0;                  /* data offset */
    len = 0;                  /* length of output buffer */
    size = 8 * LOG_MAX_LEN;   /* size of output buffer */

    while (datalen != 0 && (len < size - 1)) {
        char *save, *str;
        unsigned char c;
        int savelen;

        len += nc_scnprintf(buf + len, size - len, "%08x  ", off);

        save = data;
        savelen = datalen;

        for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
            c = (unsigned char)(*data);
            str = (i == 7) ? "  " : " ";
            len += nc_scnprintf(buf + len, size - len, "%02x%s", c, str);
        }
        for ( ; i < 16; i++) {
            str = (i == 7) ? "  " : " ";
            len += nc_scnprintf(buf + len, size - len, "  %s", str);
        }

        data = save;
        datalen = savelen;

        len += nc_scnprintf(buf + len, size - len, "  |");

        for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
            c = (unsigned char)(isprint(*data) ? *data : '.');
            len += nc_scnprintf(buf + len, size - len, "%c", c);
        }
        len += nc_scnprintf(buf + len, size - len, "|\n");

        off += 16;
    }

    n = nc_write(l->fd, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}

void
_log_safe(const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char buf[LOG_MAX_LEN];
    va_list args;
    ssize_t n;

    if (l->fd < 0) {
        return;
    }

    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    len += nc_safe_snprintf(buf + len, size - len, "[.......................] ");

    va_start(args, fmt);
    len += nc_safe_vsnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(l->fd, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}

void
_log_stderr_safe(const char *fmt, ...)
{
    struct logger *l = &logger;
    int len, size, errno_save;
    char buf[LOG_MAX_LEN];
    va_list args;
    ssize_t n;

    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    len += nc_safe_snprintf(buf + len, size - len, "[.......................] ");

    va_start(args, fmt);
    len += nc_safe_vsnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    n = nc_write(STDERR_FILENO, buf, len);
    if (n < 0) {
        l->nerror++;
    }

    errno = errno_save;
}

