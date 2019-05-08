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
#include <atomic.h>

#include "list.h"
#include "pubutil.h"
#include "thread.h"
#include "multitask.h"

int multitask_pool_init(task_pool_t *ptaskpool, 
                     char *poolname, 
                     int tasknr,
                     void *task_proc,
                     taskpool_taskpriv_gen taskpriv_gen,
                     void *taskpriv_release)
{
    int rc = 0;
    int len = 0;
    int i = 0;

    av_assert0(ptaskpool != NULL);
    av_assert0(tasknr != 0);

    memset(ptaskpool, 0, sizeof(*ptaskpool));
       
    len = tasknr * sizeof(*ptaskpool->taskpool_taskarray);
    ptaskpool->taskpool_taskarray = (task_desc_t *)zeromalloc(len);
    if (!ptaskpool->taskpool_taskarray)
    {
        fprintf(stderr, "alloc space for task entry failed(errno:%d)\n", errno);
        return -1;
    }

    ptaskpool->taskpool_size = tasknr;
    atomic_set(&ptaskpool->taskpool_count, tasknr);
    ptaskpool->taskpool_taskproc = task_proc;
    ptaskpool->taskpool_taskprivgen = taskpriv_gen;
    ptaskpool->taskpool_taskprivrelease = taskpriv_release;

    for (i = 0; i < tasknr; i++)
    {
        ptaskpool->taskpool_taskarray[i].task_id = i;
        ptaskpool->taskpool_taskarray[i].task_ppool = ptaskpool;
    }
   
    if (poolname)
    {
        snprintf(ptaskpool->taskpool_name, sizeof(ptaskpool->taskpool_name), "%s", poolname);
    }

    if (taskpriv_gen)
    {        
        rc = taskpriv_gen(ptaskpool);
        if (rc < 0)
        {
            fprintf(stderr, "generate private data for tasks failed(errno:%d)\n", errno);
            
            return -1;
        }
    }    

    return 0;
}

int multitask_pool_start(task_pool_t *ptaskpool)
{
    int rc = 0;
    int i = 0;
    task_desc_t *ptask = NULL;

    av_assert0(ptaskpool != NULL);

    for (i = 0; i < ptaskpool->taskpool_size; i++)
    {
        ptask = &ptaskpool->taskpool_taskarray[i];
        rc = pthread_create(&ptask->task_thread, NULL, ptaskpool->taskpool_taskproc, ptask);
        if (rc != 0) {
            fprintf(stderr, "create thread of task %d failed(errno:%d)\n", 
                    ptask->task_id, errno);
            return -1;
        } else {
            ptask->task_thd_inited = 1;
        }
    }

    return 0;
}

int multitask_pool_wait(task_pool_t *ptaskpool)
{
    int rc = 0;
    int i = 0;
    int failed = 0;
    task_desc_t *ptask = NULL;

    av_assert0(ptaskpool != NULL);

    for (i = 0; i < ptaskpool->taskpool_size; i++)
    {
        ptask = &ptaskpool->taskpool_taskarray[i];
        if (ptask->task_thd_inited)
        {
            rc = pthread_join(ptask->task_thread, NULL);
            if (rc != 0)
            {
                fprintf(stderr, "join thread of task %d failed(errno:%d)\n", 
                        ptask->task_id, errno);
                failed++;
            }

            ptask->task_thd_inited = 0;
        }
    }

    ptaskpool->taskpool_tasksover = 1;

    return failed ? -1 : 0;
}

void multitask_pool_destroy(task_pool_t *ptaskpool)
{
    av_assert0(ptaskpool != NULL);

    if (!ptaskpool->taskpool_tasksover) {
        multitask_pool_wait(ptaskpool);
    }
  
    free_space((char **)&ptaskpool->taskpool_taskarray);

    if (ptaskpool->taskpool_taskprivrelease) {
        ptaskpool->taskpool_taskprivrelease(ptaskpool);
    } else {
        free_space((char **)&ptaskpool->taskpool_taskprivdata);
    }
}

