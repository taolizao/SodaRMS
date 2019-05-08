/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file operations.h
 * @author taolizao33@gmail.com
 *
 */

#ifndef BDRP_TOOL_OPERATIONS_H
#define BDRP_TOOL_OPERATIONS_H

typedef struct opration_privdata {
    long start_no;
    long end_no;
    redisContext **redisconns;  /*all redis connctions*/
    long connnr;                /*connection number */
    long nextconn;
    long ttimeus;
    thread_desc_t *thread_desc;
} opration_privdata_t;

int op_run(void);

#endif  /*BDRP_TOOL_OPERATIONS_H*/

