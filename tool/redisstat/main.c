/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file main.c
 * @author taolizao33@gmail.com
 *
 */
 
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#include "const.h"
#include "log.h"
#include "atomic.h"
#include "init.h"

int main(int argc, char *argv[])
{
    int rc = 0;
    
    rc = server_init(argc, argv);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "server_init failed\n");
        return RETURN_ERR;
    }

    rc = start_run();
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "start_run failed\n");
        //return RETURN_ERR;
    }

    //LOG_REC(LOG_LEVEL_WARNING, "exit here\n");

    server_finalize();
    
    return rc;
}

