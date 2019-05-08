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
#include "pubutil.h"
#include "thread.h"

int thread_set_cancel_marks(void) 
{
    int rc = 0;

    rc = pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    if (rc != 0) {
        fprintf(stderr, "set PTHREAD_CANCEL_ENABLE mark failed(errno:%d)\n", errno);
        return -1;
    }

    rc = pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    if (rc != 0) {
        fprintf(stderr, "set PTHREAD_CANCEL_ASYNCHRONOUS mark failed(errno:%d)\n", errno);
        return -1;
    }

    return 0;
}

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
    pthdpool->thdpool_thds = (thread_desc_t *)zeromalloc(len);
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
        free_space((char **)&pthdpool->thdpool_thds);
        pthdpool->thdpool_size = 0;
        return -1;
    } 
    pthdpool->thdpool_mutexinited = 1;

    if (thdpriv_gen)
    {
        rc = thdpriv_gen(pthdpool);
        if (rc < 0)
        {
            fprintf(stderr, "generate private data for threads failed(errno:%d)\n", errno);
            pthread_mutex_destroy(&pthdpool->thdpool_mutex);
            free_space((char **)&pthdpool->thdpool_thds);
            pthdpool->thdpool_size = 0;
            return -1;
        }
    }

    return 0;
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
                        pthread->thread_hdl, pthread);
        if (rc != 0) {
            fprintf(stderr, "create thread %d failed(errno:%d)\n", 
                    pthread->thread_id, errno);
            return -1;
        } else {
            pthread->thread_inited = 1;
        }
    }

    return 0;
}

int thread_pool_wait(thread_pool_t *pthdpool)
{
    int rc = 0;
    int i = 0;
    int failed = 0;
    thread_desc_t *pthread = NULL;

    av_assert0(pthdpool != NULL);

    for (i = 0; i < pthdpool->thdpool_size; i++)
    {
        pthread = &pthdpool->thdpool_thds[i];
        //fprintf_stdout("pthread_join for thread %d\n", i);
        if (pthread->thread_inited) {
            rc = pthread_join(pthread->thread_inst, NULL);
            if (rc != 0)
            {
                fprintf(stderr, "join thread %d failed(errno:%d)\n", 
                        pthread->thread_id, errno);
                failed++;
            }

            pthread->thread_inited = 0;
        }
    }

    pthdpool->thdpool_thdsjoined = 1;

    //fprintf_stdout("pthread_join all ok\n");
    return failed ? -1 : 0;
}

int thread_pool_cancel(thread_pool_t *pthdpool)
{
    int rc = 0;
    int i = 0;
    int failed = 0;
    thread_desc_t *pthread = NULL;

    av_assert0(pthdpool != NULL);

    for (i = 0; i < pthdpool->thdpool_size; i++)
    {
        pthread = &pthdpool->thdpool_thds[i];
        //fprintf_stdout("pthread_join for thread %d\n", i);
        if (pthread->thread_inited && (!pthread->thread_canceled)) {
            rc = pthread_cancel(pthread->thread_inst);
            if (rc != 0)
            {
                fprintf(stderr, "cancel thread %d failed(errno:%d)\n", 
                        pthread->thread_id, errno);
                failed++;                        
            }
            pthread->thread_canceled = 1;
        }
    }

    pthdpool->thdpool_canceled = 1;

    //fprintf_stdout("pthread_join all ok\n");
    return failed ? -1 : 0;
}

void thread_pool_destroy(thread_pool_t *pthdpool)
{
    int i = 0;
    someother_func releasepriv = NULL;
    thread_desc_t *pthread = NULL;

    av_assert0(pthdpool != NULL);

    if (!pthdpool->thdpool_canceled) {
        thread_pool_cancel(pthdpool);
    }

    if (!pthdpool->thdpool_thdsjoined) {
        thread_pool_wait(pthdpool);
    }

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
                free_space((char **)&pthread->thread_priv);
            }
        }

        free_space((char **)&pthdpool->thdpool_thds);
        pthdpool->thdpool_size = 0;
    }

    if (pthdpool->thdpool_mutexinited)
    {
        pthread_mutex_destroy(&pthdpool->thdpool_mutex);
        pthdpool->thdpool_mutexinited = 0;
    }
}



