/***************************************************************************
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

#include "list.h"
#include "pubutil.h"

int conf_item_str2long(char *itemstr, long *itemint)
{
    int tmpint = 0;

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

void *zeromalloc(size_t size)
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

    return tmpptr;
}

void free_space(char **pptr)
{
    if (pptr && (*pptr))
    {
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

int ipv4_format_check(const char *ipv4addr)
{
    int rc = 0;
    struct in_addr in;

    rc = inet_pton(AF_INET, ipv4addr, (void *)&in);
    if (rc < 0)
    {
        fprintf(stderr, "ivnalid ip format %s\n", ipv4addr);
        return -1;
    }

    return 0;
}
    
int statistic_addobj(void *pobj, void *pstat)
{
    int rc = 0;
    int statcountbak = 0;
    struct list_head *pplist = (struct list_head* )pobj;
    statistic_head_t *pstatistic = (statistic_head_t *)pstat;

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_lock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_addobj lock %p failed\n", &pstatistic->stat_mutex);
            return -1;
        }       
    }

    statcountbak = pstatistic->stat_count;
    
    list_add_tail(pplist, &pstatistic->stat_head);
    pstatistic->stat_count++;

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_unlock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_addobj unlock %p failed\n", &pstatistic->stat_mutex);
            //return -1;
        }       
    }

    if (!statcountbak)
    {
        //fprintf(stderr, "add obj into list and sem_post:%p\n", &pstatistic->stat_sem);
        sem_post(&pstatistic->stat_sem);
    }

    return 0;
}

int statistic_spliceobj(void *pstatsrc, void *pdst)
{
    int rc = 0;
    statistic_head_t *pstatisticsrc = (statistic_head_t *)pstatsrc;
    struct list_head *plist = (struct list_head *)pdst;

    if (pstatisticsrc->stat_need_mutex)
    {
        rc = pthread_mutex_lock(&pstatisticsrc->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_spliceobj lock %p failed\n", &pstatisticsrc->stat_mutex);
            return -1;
        }       
    }

    list_splice_init(&pstatisticsrc->stat_head, plist);
    pstatisticsrc->stat_count = 0;

    if (pstatisticsrc->stat_need_mutex)
    {
        rc = pthread_mutex_unlock(&pstatisticsrc->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_spliceobj unlock %p failed\n", &pstatisticsrc->stat_mutex);
            //return -1;
        }       
    }

    return 0;
}

int statistic_getobj(void **ppobj, void *pstat)
{
    int rc = 0;
    int found = -1;
    struct list_head *plist = NULL;
    statistic_head_t *pstatistic = (statistic_head_t *)pstat;

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_lock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_getobj lock %p failed\n", &pstatistic->stat_mutex);
            return -1;
        }       
    }

    if (pstatistic->stat_count)
    {
        rc = list_pop_head(&plist, &pstatistic->stat_head);
        if (rc >= 0)
        {
            found = 0;
            *ppobj = (void *)plist;
            pstatistic->stat_count--;
        }       
    }

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_unlock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_getobj unlock %p failed\n", &pstatistic->stat_mutex);
            //return -1;
        }       
    }

    return found;
}

int statistic_delobj(void *pobj, void *pstat)
{
    int rc = 0;
    struct list_head *plist = (struct list_head *)pobj;
    statistic_head_t *pstatistic = (statistic_head_t *)pstat;

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_lock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_getobj lock %p failed\n", &pstatistic->stat_mutex);
            return -1;
        }       
    }

    list_del_init(plist);
    if (pstatistic->stat_count > 0)
    {
        pstatistic->stat_count--;
    }

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_unlock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_getobj unlock %p failed\n", &pstatistic->stat_mutex);
            //return -1;
        }       
    }

    return 0;
}

int statistic_freeobj_custom(void *pobj, void *pstat, free_func ffree)
{
    int rc = 0;
    struct list_head *pos = NULL;
    struct list_head *n = NULL;
    statistic_head_t *pstatistic = (statistic_head_t *)pstat;

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_lock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_getobj lock %p failed\n", &pstatistic->stat_mutex);
            return -1;
        }       
    }
   
    list_for_each_safe(pos, n, &pstatistic->stat_head)
    {
        list_del_init(pos);
        if (ffree) {
            ffree(pos);
        }
    }
    pstatistic->stat_count = 0;

    if (pstatistic->stat_need_mutex)
    {
        rc = pthread_mutex_unlock(&pstatistic->stat_mutex);
        if (rc)
        {
            fprintf(stderr, "statistic_getobj unlock %p failed\n", &pstatistic->stat_mutex);
            //return -1;
        }       
    }

    return 0;
}


int statistic_freeobj(void *pobj, void *pstat)
{
    return statistic_freeobj_custom(pobj, pstat, free);
}

int statistic_head_init(statistic_head_t *pstat, 
                        long need_mutex, 
                        statistic_ops_t *pstatops)
{
    int rc = 0;

    memset(pstat, 0, sizeof(*pstat));
    
    init_list_head(&pstat->stat_head);
    pstat->stat_need_mutex = need_mutex;

    rc = sem_init(&pstat->stat_sem, 0, 0);
    if (rc != 0)
    {
        fprintf(stderr, "init thread semaphore failed\n");   
        return -1;
    }

    if (need_mutex)
    {
        rc = pthread_mutex_init(&pstat->stat_mutex, NULL);
        if (rc != 0)
        {
            fprintf(stderr, "init thread mutex failed\n"); 
            sem_destroy(&pstat->stat_sem);
            return -1;
        } 
    }

    if (!pstatops)
    {
        pstat->stat_ops.stat_addobj = statistic_addobj;
        pstat->stat_ops.stat_spliceobj = statistic_spliceobj;
        pstat->stat_ops.stat_getobj = statistic_getobj;
        pstat->stat_ops.stat_freeobj = statistic_freeobj;
        pstat->stat_ops.stat_delobj = statistic_delobj;
    } else {
        pstat->stat_ops = *pstatops;
    }

    return 0;
}

int statistic_head_finalize(statistic_head_t *pstat)
{
    int rc = 0;
    int err = 0;

    if (pstat->stat_ops.stat_freeobj)
    {
        pstat->stat_ops.stat_freeobj(NULL, pstat);
    }
    
    err = sem_destroy(&pstat->stat_sem);
    if (err != 0)
    {
        fprintf(stderr, "finalize semaphore failed\n");   
        rc = -1;
    }

    if (pstat->stat_need_mutex)
    {
        err = pthread_mutex_destroy(&pstat->stat_mutex);
        if (err != 0)
        {
            fprintf(stderr, "init mutex failed\n");   
            rc = -1;
        } 
    }

    return rc;
}

int dynamic_buffer_init(dynamic_buffer_t **ppdbuffer, size_t initsize) {
    dynamic_buffer_t *pbuf = NULL;

    pbuf = (dynamic_buffer_t *)zeromalloc(sizeof(*pbuf));
    if (!pbuf) {
        fprintf(stderr, "allocate space for dynamic_buffer_t structure failed\n");
        return -1;
    }

    pbuf->dbuf_cont = (char *)zeromalloc(initsize);
    if (!pbuf->dbuf_cont) {
        fprintf(stderr, "init out buffer with %ld bytes failed\n", initsize);
        free(pbuf);
        return -1;                
    }

    pbuf->dbuf_size = initsize;
    pbuf->dbuf_len = 0;

    *ppdbuffer = pbuf;

    return 0;
}

int dynamic_buffer_resize(dynamic_buffer_t *pdbuffer, size_t newsize, int reinit) {
    size_t new_size = pdbuffer->dbuf_size * 2;
    char *tmpbuf = NULL;

    new_size = (new_size > newsize) ? new_size : newsize;

    tmpbuf = realloc(pdbuffer->dbuf_cont, new_size);
    if (!tmpbuf) {
        fprintf(stderr, "resize out buffer from %ld to %ld bytes failed\n", 
                pdbuffer->dbuf_size, new_size);
        return -1;                
    }

    pdbuffer->dbuf_cont = tmpbuf;
    pdbuffer->dbuf_size = new_size;

    if (reinit) {
        memset(pdbuffer->dbuf_cont, 0, pdbuffer->dbuf_size);
    } else {
        memset(&pdbuffer->dbuf_cont[pdbuffer->dbuf_len], 0, 
                pdbuffer->dbuf_size - pdbuffer->dbuf_len);
    }

    return 0;
}

void dynamic_buffer_finalize(dynamic_buffer_t **pdbuffer) {
    dynamic_buffer_t *pbuf = NULL;
    
    if (!(pdbuffer || *pdbuffer)) {
        return;
    }

    pbuf = *pdbuffer;
    free_space((char **)&pbuf->dbuf_cont);       
    free_space((char **)pdbuffer);
}


