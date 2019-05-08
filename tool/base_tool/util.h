/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file util.h
 * @author taolizao33@gmail.com
 *
 */

#ifndef BDRP_TOOL_UTIL_H
#define BDRP_TOOL_UTIL_H

#define AV_STRINGIFY(s) AV_TOSTRING(s)
#define AV_TOSTRING(s) #s

#define av_assert0(cond)  \
do                        \
{                         \
    if (!(cond))          \
    {                     \
        printf("Assertion %s failed at %s:%d\n",     \
            AV_STRINGIFY(cond), __FILE__, __LINE__); \
        abort();                                     \
    }                                                \
} while (0)

typedef int (*someother_func)(void *);

void fprintf_stdout(char *fmt, ...);

int conf_item_str2long(char *itemstr, long *itemint);
void signal_abort_handler(int sig);

void *zmalloc(size_t size);
void free_space(char **pptr);
void insert_sort(long *array, long *count, long size, long newval);
int popen_cmdexe(char *command, someother_func reslineanalyze);
void get_time_tv(struct timeval *ptv);
long get_time_dur_us(struct timeval tstart, struct timeval tend);

#endif  //  end of BDRP_TOOL_UTIL_H

