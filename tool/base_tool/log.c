/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 ***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file log.c
 * @author taolizao33@gmail.com
 *
 */
 
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <float.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/syscall.h>
#include <pthread.h>

#include "log.h"

log_desc_t g_logdesc;

log_desc_t *get_logdesc(void)
{
    return &g_logdesc;
}

static inline int log_lock()
{
    int rc = 0;
    
    log_desc_t *pglog = get_logdesc();

    rc = pthread_mutex_lock(&pglog->log_lock);
    if (rc)
    {
        fprintf(stderr, "pthread_mutex_lock failed(errno:%d)\n", errno);
        return -1;
    }

    return 0;
}

static inline int log_unlock()
{
    int rc = 0;
    log_desc_t *pglog = get_logdesc();
    
    rc = pthread_mutex_unlock(&pglog->log_lock);
    if (rc)
    {
        fprintf(stderr, "pthread_mutex_unlock failed(errno:%d)\n", errno);
        return -1;
    }

    return 0;
}

static inline int log_lock_init()
{
    int rc = 0;
    log_desc_t *pglog = get_logdesc();
    
    rc = pthread_mutex_init(&pglog->log_lock, NULL);
    if (rc)
    {
        fprintf(stderr, "pthread_mutex_init failed(errno:%d)\n", errno);
        return -1;
    }
    
    pglog->log_lock_inited = 1;
    return 0;
}

static inline int log_lock_finalize()
{
    int rc = 0;
    log_desc_t *pglog = get_logdesc();

    if (pglog->log_lock_inited)
    {
        rc = pthread_mutex_destroy(&pglog->log_lock);
        if (rc)
        {
            fprintf(stderr, "pthread_mutex_destroy failed(errno:%d)\n", errno);
            return -1;
        }
        pglog->log_lock_inited = 0;
    }

    return 0;
}

static inline long get_current_utime()
{
    struct timeval now;
    gettimeofday(&now, NULL);	
    return now.tv_sec * 1000000 + now.tv_usec;
}

void log_record(int32_t level, char *filename, int32_t linenum, char *fmt, ...)
{
    int buflen = 0;
    va_list argptr;
    time_t now;
    long nowtime = 0;     
    struct tm *timenow = NULL;
    pthread_t mythid;
    pid_t pid;
    char logbuf[LOG_BUFFER_SIZE];
    char confbakpath[1024];
    log_desc_t *pglog = get_logdesc();
    int len = 0;
    int leftlen = LOG_BUFFER_SIZE;
    
    if(level > pglog->loglevel_limit) 
    {
        return;
    }
   
    memset(logbuf, 0, LOG_BUFFER_SIZE);	
    
    time(&now);
    timenow = localtime(&now);
    nowtime = get_current_utime();
    mythid = pthread_self();  
    pid = getpid();
    
    buflen += snprintf(&logbuf[buflen], leftlen, 
                        "[L%d]%s", level, asctime(timenow));
    if (buflen >= leftlen) {
        leftlen = 0;
        buflen = LOG_BUFFER_SIZE - leftlen;
    } else {
        leftlen -= buflen;
    }
                        
    buflen--; /*clear \n*/
    
    buflen += snprintf(&logbuf[buflen], leftlen, 
                " (%ld) PT[%u][%u]: %s:%d ", 
                nowtime, pid, (unsigned)mythid, filename, linenum);
    if (buflen >= leftlen) {
        leftlen = 0;
        buflen = LOG_BUFFER_SIZE - leftlen;
    } else {
        leftlen -= buflen;
    }
    
    va_start(argptr, fmt);
    buflen += vsnprintf(&logbuf[buflen], leftlen, fmt, argptr);
    if (buflen >= leftlen) {
        leftlen = 0;
        buflen = LOG_BUFFER_SIZE - leftlen;
    } else {
        leftlen -= buflen;
    }
    
    va_end(argptr);

    
    if(logbuf[buflen - 1] != '\n') 
    {
        buflen += snprintf(&logbuf[buflen], LOG_BUFFER_SIZE - buflen, "\n");
    }      

    log_lock();    

    if (pglog->log2out)
    {
        printf("%s", logbuf);
    }
    fprintf(pglog->log_fd, "%s", logbuf);
    fflush(pglog->log_fd);
    
    pglog->log_size += buflen;
    if (pglog->log_size > LOG_MAX_SIZE)
    {
        snprintf(confbakpath, sizeof(confbakpath), "%s_bak", pglog->log_path);
        fclose(pglog->log_fd);
        rename(pglog->log_path, confbakpath);

        pglog->log_fd = fopen(pglog->log_path, "a");
        if(pglog->log_fd == NULL) 
        {
            fprintf(stderr, "open log path:%s failed(errno:%d)\n", 
                    pglog->log_path, errno);
            log_unlock(); 
            exit(0);
        }
    }

    log_unlock();         
}

void log_init(void)
{
    struct stat statbuf; 
    log_desc_t *pglog = get_logdesc();

    pglog->log_fd = fopen(pglog->log_path, "a");
    if(pglog->log_fd == NULL) 
    {
        fprintf(stderr, "open log path:%s failed(errno:%d)\n", 
                pglog->log_path, errno);
        exit(0);
    }
    
    stat(pglog->log_path, &statbuf);
    pglog->log_size = statbuf.st_size;
    log_lock_init();
}

int log_init_with_arg(const char *logpath, int loglevel)
{
    struct stat statbuf; 
    log_desc_t *pglog = get_logdesc();

    memset(pglog, 0, sizeof(*pglog));
    snprintf(pglog->log_path, sizeof(pglog->log_path), "%s", logpath);
    pglog->loglevel_limit = loglevel;

    pglog->log_fd = fopen(pglog->log_path, "a");
    if(pglog->log_fd == NULL) 
    {
        fprintf(stderr, "open log path:%s failed(errno:%d)\n", 
                pglog->log_path, errno);
        return -1;
    }
    
    stat(pglog->log_path, &statbuf);
    pglog->log_size = statbuf.st_size;
    log_lock_init();

    return 0;
}

void log_reinit(void)
{
    struct stat statbuf; 
    log_desc_t *pglog = get_logdesc();

    if (pglog->log_fd)
    {
        fclose(pglog->log_fd);
    }

    if (pglog->log_lock_inited)
    {
        log_lock();    
    }
    
    pglog->log_fd = fopen(pglog->log_path, "a");
    if(pglog->log_fd == NULL) 
    {
        fprintf(stderr, "open log path:%s failed(errno:%d)\n", 
                pglog->log_path, errno);
        exit(0);
    }
    
    stat(pglog->log_path, &statbuf);
    pglog->log_size = statbuf.st_size;

    if (pglog->log_lock_inited)
    {
        log_unlock();    
    }
    else
    {
        log_lock_init();
    }
}

void log_finalize(void)
{
    log_desc_t *pglog = get_logdesc();

    if(pglog->log_fd) 
    {
        fclose(pglog->log_fd);
        pglog->log_fd = 0;
    }
    
    log_lock_finalize();
}

