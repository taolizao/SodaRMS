/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file test_init.c
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
#include <Automated.h>
#include <CUError.h>
#include <MyMem.h>
#include <TestRun.h>
#include <Util.h>

#include "test_stub.h"
#include "test_init.h"
#include "example.h"
#include "rtstub.h"

extern int test_service_init_step1(void);

int service_prestub_start(void)
{
    int rc = 0;

    //TODO stub functions before init service
    rc = rtstub_create(test_service_init_step1, 
                       test_service_init_step1_stub);
    if (rc < 0)
    {
        printf("stub test_service_init_step1 to "
            "test_service_init_step1_stub failed\n");
    }

    return rc;
}

int service_prestub_end(void)
{
    int rc = 0;

    //TODO unstub all the functions here
    rtstub_destroy(test_service_init_step1);

    return rc;
}

int service_init(void)
{
    int rc = 0;

    rc = service_prestub_start();
    if (rc < 0)
    {   
        printf("service_prestub_start failed(errno:%d)\n", rc);
        return rc;
    }
    
    // init
    //TODO add init services
    rc = test_service_init();
    if (rc < 0)
    {
        printf("test_service_init failed(errno:%d)\n", rc);
        return rc;
    }

    return rc;
}

int service_finalize(void)
{
    int rc = 0;

    //TODO add services finalize procedure
    rc = test_service_finalize();
    if (rc < 0)
    {
        printf("test_service_finalize failed(errno:%d)\n", rc);
    }

    service_prestub_end();

    return rc;
}

int suite_success_init(void)
{
    int rc = 0;

    rc = rtstub_init();
    if (rc < 0)
    {
        printf("rtstub_init failed(errno:%d)\n", rc);
        return rc;
    }

    rc = service_init();
    if (rc < 0)
    {
        printf("service_init failed(errno:%d)\n", rc);
        return rc;
    }

    return rc;
}

int suite_success_clean(void)
{
    int rc = 0;

    service_finalize();

    rtstub_finalize();

    return rc;
}

