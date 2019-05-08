/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file thread.c
 * @author taolizao33@gmail.com
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <semaphore.h>

#include "list.h"
#include "util.h"
#include "thread.h"

int thread_pool_init(thread_pool_t *pthdpool, 
                     char *poolname, 
                     int thdnr, 
                     void *thd_hdl,
                     void *thd_analyze,
                     void *thd_privrelease,
                     thdpool_thdpriv_gen thdpriv_gen)
{
    int rc = 0;
    int len = 0;
    int i = 0;

    av_assert0(pthdpool != NULL);
    av_assert0(thdnr != 0);

    memset(pthdpool, 0, sizeof(*pthdpool));
       
    len = thdnr * sizeof(*pthdpool->thdpool_thds);
    pthdpool->thdpool_thds = (thread_desc_t *)zmalloc(len);
    if (!pthdpool->thdpool_thds)
    {
        fprintf(stderr, "alloc space for thread entry failed(errno:%d)\n", errno);
        return -1;
    }

    pthdpool->thdpool_size = thdnr;

    for (i = 0; i < thdnr; i++)
    {
        pthdpool->thdpool_thds[i].thread_id = i;
        pthdpool->thdpool_thds[i].thread_belongpool = pthdpool;
        pthdpool->thdpool_thds[i].thread_res = 0;
        pthdpool->thdpool_thds[i].thread_hdl = thd_hdl;
        pthdpool->thdpool_thds[i].thread_analyze = thd_analyze;
        pthdpool->thdpool_thds[i].thread_privrelease = thd_privrelease;
    }
   
    if (poolname)
    {
        snprintf(pthdpool->thdpool_name, sizeof(pthdpool->thdpool_name),
                 "%s", poolname);
    }

    rc = pthread_mutex_init(&pthdpool->thdpool_mutex, NULL);
    if (rc != 0)
    {
        fprintf(stderr, "init threads mutex failed(errno:%d)\n", errno);        
        return -1;
    } 
    pthdpool->thdpool_mutexinited = 1;

    if (thdpriv_gen)
    {
        rc = thdpriv_gen(pthdpool);
        if (rc < 0)
        {
            fprintf(stderr, "generate private data for threads failed(errno:%d)\n", errno);
            return -1;
        }
    }

    return rc;
}

int thread_pool_start(thread_pool_t *pthdpool)
{
    int rc = 0;
    int i = 0;
    thread_desc_t *pthread = NULL;

    av_assert0(pthdpool != NULL);

    for (i = 0; i < pthdpool->thdpool_size; i++)
    {
        pthread = &pthdpool->thdpool_thds[i];
        //fprintf_stdout("start thread %d\n", i);
        rc = pthread_create(&pthread->thread_inst, NULL, 
                        pthread->thread_hdl, pthread->thread_priv);
        if (rc != 0)
        {
            fprintf(stderr, "create thread %d failed(errno:%d)\n", 
                    pthread->thread_id, errno);
            return -1;
        }
    }

    return 0;
}

int thread_pool_wait(thread_pool_t *pthdpool)
{
    int rc = 0;
    int i = 0;
    thread_desc_t *pthread = NULL;

    av_assert0(pthdpool != NULL);

    for (i = 0; i < pthdpool->thdpool_size; i++)
    {
        pthread = &pthdpool->thdpool_thds[i];
        //fprintf_stdout("pthread_join for thread %d\n", i);
        rc = pthread_join(pthread->thread_inst, NULL);
        if (rc != 0)
        {
            fprintf(stderr, "join thread %d failed(errno:%d)\n", 
                    pthread->thread_id, errno);
            return -1;
        }
    }

    //fprintf_stdout("pthread_join all ok\n");
    return 0;
}

void thread_pool_destroy(thread_pool_t *pthdpool)
{
    int i = 0;
    someother_func releasepriv = NULL;
    thread_desc_t *pthread = NULL;

    av_assert0(pthdpool != NULL);

    if (pthdpool->thdpool_thds)
    {
        for (i = 0; i < pthdpool->thdpool_size; i++)
        {
            pthread = &pthdpool->thdpool_thds[i];

            if (pthread->thread_privrelease)
            {
                releasepriv = pthread->thread_privrelease;
                releasepriv(&pthread->thread_priv);
            }
            else
            {
                free_space(&pthread->thread_priv);
            }
        }

        free_space((void **)&pthdpool->thdpool_thds);
        pthdpool->thdpool_size = 0;
    }

    if (pthdpool->thdpool_mutexinited)
    {
        pthread_mutex_destroy(&pthdpool->thdpool_mutex);
        pthdpool->thdpool_mutexinited = 0;
    }
}

