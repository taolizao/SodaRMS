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

#ifndef DBA_REDIS_DBCACHECONV_THREAD_H
#define DBA_REDIS_DBCACHECONV_THREAD_H

enum {
    THREAD_POOLNAME_LEN = 128,
};

enum {
    THREAD_STATUS_INACTIVE = 0,
    THREAD_STATUS_ACTIVE,
};

struct thread_pool;

typedef struct thread_desc {
    pthread_t thread_inst;
    int thread_id;
    int thread_canceled;
    int thread_inited;
    void *thread_priv;
    void *thread_hdl;
    void *thread_analyze;
    void *thread_privrelease;
    int thread_res;
    int thread_down;
    struct thread_pool *thread_belongpool;
} thread_desc_t;

typedef struct thread_pool {
    int thdpool_size;
    int thdpool_hasfailed;
    int thdpool_canceled;
    int thdpool_thdsjoined;
    int thdpool_mutexinited;
    pthread_mutex_t thdpool_mutex; 
    unsigned int thdpool_selthdidx;
    char thdpool_name[THREAD_POOLNAME_LEN];
    thread_desc_t *thdpool_thds;
} thread_pool_t;

typedef int (*thdpool_thdpriv_gen)(thread_pool_t *);

int thread_pool_init(thread_pool_t *pthdpool, 
                     char *poolname, 
                     int thdnr, 
                     void *thd_hdl,
                     void *thd_analyze,
                     void *thd_privrelease,
                     thdpool_thdpriv_gen thdpriv_gen);
int thread_pool_start(thread_pool_t *pthdpool);
int thread_pool_wait(thread_pool_t *pthdpool);
void thread_pool_destroy(thread_pool_t *pthdpool);

#endif  /* DBA_REDIS_DBCACHECONV_THREAD_H */

