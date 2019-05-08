/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file test_stub.c
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

#include <Basic.h>
#include <Console.h>
#include <CUnit.h>
#include <TestDB.h>

int branch_func_stub(void)
{
    int rc = 0;

    printf("This is branch_func_stub to return -1\n");
    rc = -1;

    return rc;
}

int test_service_init_step1_stub(void)
{
    int rc = 0;

    //printf("This is test_service_init_step1_stub\n");

    return rc;
}

