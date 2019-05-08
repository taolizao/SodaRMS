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

#ifndef DBA_REDIS_DBCACHECONV_PUBUTIL_H
#define DBA_REDIS_DBCACHECONV_PUBUTIL_H

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
typedef void (*free_func)(void *);
typedef int (* stat_op_func)(void *, void *);
typedef int (* stat_get_func)(void **, void *);
typedef struct statistic_ops {
    stat_op_func stat_addobj;    // function to add object to stat_head
    stat_op_func stat_spliceobj; // function to scratch all objects from stat_head
    stat_get_func stat_getobj;    // function to get and remove an object from stat_head
    stat_op_func stat_freeobj;   // function to free all objects in stat_head
    stat_op_func stat_delobj;  // funciton to delete object from stat head
} statistic_ops_t;

/* statistic unit */
typedef struct statistic_head {
    struct list_head stat_head;    // list head of statistic objects 
    long stat_count;               // number of object
    long stat_need_mutex;          // need
    sem_t stat_sem;                // semaphore to wait new object
    pthread_mutex_t stat_mutex;    // mutex used when add of delete object from head list    
    statistic_ops_t stat_ops;      // operation func 
} statistic_head_t;

typedef struct dynamic_buffer {
    size_t dbuf_len;    // length of content in dbuf_cont
    size_t dbuf_size;   // sizeof dbuf_cont
    char *dbuf_cont;    
} dynamic_buffer_t;

int dynamic_buffer_init(dynamic_buffer_t **ppdbuffer, size_t initsize);
int dynamic_buffer_resize(dynamic_buffer_t *pdbuffer, size_t newsize, int reinit);
void dynamic_buffer_finalize(dynamic_buffer_t **pdbuffer);

int conf_item_str2long(char *itemstr, long *itemint);
void signal_abort_handler(int sig);

void *zeromalloc(size_t size);
void free_space(char **pptr);
void insert_sort(long *array, long *count, long size, long newval);
int popen_cmdexe(char *command, someother_func reslineanalyze);

void get_time_tv(struct timeval *ptv);
long get_time_dur_us(struct timeval tstart, struct timeval tend);

int ipv4_format_check(const char *ipv4addr);

int statistic_head_init(statistic_head_t *pstat, long need_mutex, statistic_ops_t *pstatops);
int statistic_head_finalize(statistic_head_t *pstat);

int statistic_addobj(void *pobj, void *pstat);
int statistic_spliceobj(void *pstatsrc, void *pdst);
int statistic_getobj(void **ppobj, void *pstat);
int statistic_freeobj(void *pobj, void *pstat);
int statistic_freeobj_custom(void *pobj, void *pstat, free_func ffree);
int statistic_delobj(void *pobj, void *pstat);

#endif  /*DBA_REDIS_DBCACHECONV_PUBUTIL_H*/

