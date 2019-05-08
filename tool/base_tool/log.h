/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file log.h
 * @author taolizao33@gmail.com
 *
 */

#ifndef BDRP_TOOL_LOG_H
#define BDRP_TOOL_LOG_H

enum {
    LOG_LEVEL_FATAL = 0,
    LOG_LEVEL_ERR,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_TRACE,
    LOG_LEVEL_NR,
};

enum {
    LOG_BUFFER_SIZE = 1024,
    LOG_MAX_SIZE = (1 << 30),
    LOG_NUM = 2,
};

typedef struct log_desc {
    FILE *log_fd;
    long log_size;
    pthread_mutex_t log_lock;
    long log_lock_inited;
    int loglevel_limit;
    char log_path[1024];
    int log2out;
} log_desc_t;

log_desc_t *get_logdesc(void);
void log_init(void);
int log_init_with_arg(const char *logpath, int loglevel);
void log_finalize(void);
void log_reinit(void);
void log_record(int32_t level, char *filename, int32_t linenum,  char *fmt, ...);
#define LOG_REC(level, format, args...) \
    log_record(level, __FILE__, __LINE__, format, ##args)

#endif  /*BDRP_TOOL_LOG_H*/

