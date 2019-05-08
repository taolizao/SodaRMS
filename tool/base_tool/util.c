/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 ***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file util.c
 * @author taolizao33@gmail.com
 *
 */
 
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <sys/stat.h>
#include <semaphore.h>

#include "log.h"
#include "list.h"
#include "util.h"

void fprintf_stdout(char *fmt, ...)
{
    va_list argptr;
    char logbuf[1024];
    int buflen = 0;
    
    va_start(argptr, fmt);
    buflen = vsnprintf(logbuf, 1024, fmt, argptr);
    va_end(argptr);
    
    if(logbuf[buflen - 1] != '\n')
    {   
        logbuf[buflen - 1] = '\n';
    }
    
    fprintf(stdout, "%s", logbuf);
    fflush(stdout);
}

int conf_item_str2long(char *itemstr, long *itemint)
{
    long tmpint = 0;

    tmpint = atol(itemstr);
    if (!tmpint && strcmp(itemstr, "0"))
    {
        fprintf(stderr, "invalid argument item '%s'\n", itemstr);
        return -1;
    }

    if (tmpint < 0)
    {
        fprintf(stderr, "should not be negative numbers\n");
        return -1;
    }

    *itemint = tmpint;

    return 0;
}

void signal_abort_handler(int sig)
{
    fprintf(stderr, "abort called and exit here!\n");
    exit(0);
}

void *zmalloc(size_t size)
{
    void *tmpptr = NULL;

    tmpptr = malloc(size);
    if (!tmpptr)
    {
        fprintf(stderr, "alloc %d bytes failed\n", (int)size);
    }
    else
    {
        memset(tmpptr, 0, size);
    }

    //fprintf_stdout("MALLOC %p\n", tmpptr);

    return tmpptr;
}

void free_space(char **pptr)
{
    if (pptr && (*pptr))
    {
        //fprintf_stdout("free %p\n", (char *)*pptr);
        free(*pptr);
        *pptr = NULL;
    }
}

void insert_sort(long *array, long *count, long size, long newval)
{
    int i = 0;
    int j = 0;
    long idx = *count;

    av_assert0(idx < size);

    for (i = 0; i < idx; i++)
    {
        if (array[i] < newval)
        {
            continue;
        }

        if (array[i] == newval)
        {
            break;
        }

        for (j = idx; j > i; j--)
        {
            array[j] = array[j-1];
        }
        *count = idx + 1;
    }

    if (i == idx)
    {
        *count = idx + 1;
    }
    array[i] = newval;
}

int popen_cmdexe(char *command, someother_func reslineanalyze)
{
    int rc = 0;
    char tmpline[1024];
    FILE *fd = NULL;
    char *fgetsres = NULL;

    fd = popen(command, "r");
    if (!fd)
    {
        fprintf(stderr, "execute command(%s) failed\n", command);
        return -1;
    }

    while(1)
    {
        fgetsres = fgets(tmpline, sizeof(tmpline), fd);
        if (!fgetsres)
        {
            break;
        }

        rc = reslineanalyze(tmpline);
        if (rc < 0)
        {
            fprintf(stderr, "analyze command result(%s) failed\n", tmpline);
            pclose(fd);
            return -1;
        }
    }

    pclose(fd);

    return 0;
}

void get_time_tv(struct timeval *ptv)
{
    struct timezone tz;
    
    gettimeofday(ptv, &tz);
}   

long get_time_dur_us(struct timeval tstart, struct timeval tend)
{
    long durtime = 0;
    
    av_assert0(tend.tv_sec >= tstart.tv_sec);
    
    durtime = 1000000 * (tend.tv_sec - tstart.tv_sec);
    durtime += (tend.tv_usec - tstart.tv_usec);
    
    av_assert0(durtime >= 0);
    
    return durtime;
}
