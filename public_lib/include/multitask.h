/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file thresd.h
 * @author taolizao33@gmail.com
 *
 */

#ifndef DBA_REDIS_DBCACHECONV_MULTITASK_H
#define DBA_REDIS_DBCACHECONV_MULTITASK_H

enum {
    TASKPOOL_NAME_LEN = 128,
    TASKPOOL_TASKNR_MAX_DEF = 1024,
};

struct task_pool;

typedef int (*taskpool_taskpriv_gen)(struct task_pool *);
typedef int (*taskpool_taskpriv_release)(struct task_pool *);

typedef struct task_desc {
    pthread_t task_thread;
    int task_thd_inited;
    int task_id;
    struct task_pool *task_ppool;
} task_desc_t;

typedef struct task_pool {
    int taskpool_size;
    atomic_t taskpool_count;
    int taskpool_hasfailed;
    int taskpool_tasksover;
    char taskpool_name[TASKPOOL_NAME_LEN];
    task_desc_t *taskpool_taskarray;
    void *taskpool_taskproc;
    taskpool_taskpriv_gen taskpool_taskprivgen;
    taskpool_taskpriv_release taskpool_taskprivrelease;
    void *taskpool_taskprivdata;
} task_pool_t;

int multitask_pool_init(task_pool_t *ptaskpool, 
                     char *poolname, 
                     int tasknr,
                     void *task_proc,
                     taskpool_taskpriv_gen taskpriv_gen,
                     void *taskpriv_release);
int multitask_pool_start(task_pool_t *ptaskpool);
int multitask_pool_wait(task_pool_t *ptaskpool);
void multitask_pool_destroy(task_pool_t *ptaskpool);

#endif  /* DBA_REDIS_DBCACHECONV_MULTITASK_H */

