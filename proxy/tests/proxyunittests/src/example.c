/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file testexample.c
 * @author taolizao(taolizao@gmail.com)
 *
 */
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include "example.h"

int branch_func(void)
{
    printf("This is branch_func\n");
    return 0;
}

int test_func(void)
{
    int rc = 0;

    rc = branch_func();
    if (rc < 0)
    {
        printf("call branch_func failed(errno:%d)\n", rc);
        return rc;
    }

    return rc;
}

int test_service_init_step1(void)
{
    int rc = 0;
 
    printf("This is test_service_step1\n");

    return rc;
}

int test_service_init(void)
{
    int rc = 0;

    //printf("This is test_service_init\n");

    rc = test_service_init_step1();
    if (rc < 0)
    {
        printf("test_service_init_step1 failed\n");
    }

    return rc;
}

int test_service_finalize(void)
{
    int rc = 0;

    //printf("This is test_service_finalize\n");

    return rc;
}


