/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file init.c
 * @author taolizao33@gmail.com
 *
 */
 
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>

#include "const.h"
#include "log.h"
#include "atomic.h"
#include "init.h"
#include "util.h"
#include "thread.h"
#include "hiredis.h"
#include "operations.h"

extern const char *DEF_DELIM;
extern const char *no_bns;
extern const char *no_tgt;
extern const char *no_port;
extern const char *no_latency;

stat_server_t g_statserver;

stat_server_t *get_statserver(void)
{
    return &g_statserver;
}

int conf_retrive_latency(stat_config_t *pconfig, stat_server_t *pstatserver)
{
    int rc = 0;
    char *tmppos = NULL;
    char *tmpitem = NULL;
    long tmplogval = 0;

    tmppos = pconfig->conf_latencyclass;
    while(1)
    {
        tmpitem = strsep(&tmppos, DEF_DELIM);
        if (!tmpitem)
        {
            break;
        }

        rc = conf_item_str2long(tmpitem, &tmplogval);
        if (rc < 0)
        {
            LOG_REC(LOG_LEVEL_ERR, "invalid latency class value:%s\n", tmpitem);
            return RETURN_ERR;
        }

        insert_sort(pstatserver->laytencyclass, 
                    &pstatserver->laytencyclassnr, 
                    LATENCY_CLASS_MAX, 
                    tmplogval);
        if (pstatserver->laytencyclassnr > LATENCY_CLASS_MAX)
        {
            LOG_REC(LOG_LEVEL_WARNING, 
                    "more than %ld latency class given\n", LATENCY_CLASS_MAX);
            break;
        }
    }

    return RETURN_OK;
}

int servcoststat_init(stat_config_t*pconfig, stat_server_t *pstatserver)
{
    long len = 0;
    long maxlatencyms = 0;

    pstatserver->optotalnr = 
            pconfig->conf_thdnr * pconfig->conf_thdopnr * pconfig->conf_oploopnr;
    conf_retrive_latency(pconfig, pstatserver);

    maxlatencyms = pstatserver->laytencyclass[pstatserver->laytencyclassnr - 1];
    if (maxlatencyms > pconfig->conf_maxlatencyms)
    {
        pconfig->conf_maxlatencyms = maxlatencyms;
    }

    pstatserver->optimecostsize = pconfig->conf_maxlatencyms + 1 + 2;
    len = pstatserver->optimecostsize * sizeof(atomic64_t);
    pstatserver->optimecoststat = (atomic64_t *)zmalloc(len);
    if (!pstatserver->optimecoststat)
    {
        LOG_REC(LOG_LEVEL_ERR, "allocate %ld bytes failed\n", len);
        return RETURN_ERR;
    }

    return RETURN_OK;
}

int sernode_getnode(void *bnsnodestr)
{
    int len = 0;
    char *pnodename = NULL;
    int nodenamelen = 0;
    char *nodehost = NULL;
    stat_server_t *pstatserver = get_statserver();

    av_assert0(bnsnodestr != NULL);
    
    if (pstatserver->nodenr >= NODENR_MAX)
    {
        LOG_REC(LOG_LEVEL_ERR, "too many nodes, limited :%d\n", NODENR_MAX);
        return RETURN_ERR;
    }

    pnodename = (char *)bnsnodestr;
    nodenamelen = strlen(pnodename);
    if (pnodename[nodenamelen - 1] == '\n')
    {
        pnodename[nodenamelen - 1] = 0;
    }
    
    len = strlen(pnodename) + 1;
    nodehost = zmalloc(len);
    if (!nodehost)
    {
        LOG_REC(LOG_LEVEL_ERR, "sernodebns_getnode allocate %ld bytes failed\n", len);
        return RETURN_ERR;
    }

    snprintf(nodehost, len, "%s", pnodename);
    
    pstatserver->nodes[pstatserver->nodenr] = nodehost;
    pstatserver->nodenr++;    

    return RETURN_OK;
}

int servnodebns_init(stat_config_t*pconfig, stat_server_t *pstatserver)
{
    int rc = 0;
    char command[SHELLCMD_MAXLEN];
    int strcmpres = 0;

    strcmpres = strcmp(pconfig->conf_tgtbns, no_bns);
    if (!strcmpres)
    {
        LOG_REC(LOG_LEVEL_DEBUG, "no bns name give\n");
        return RETURN_OK;
    }

    snprintf(command, sizeof(command), 
             "get_instance_by_service -i %s | awk '{print $2}'", 
             pconfig->conf_tgtbns);
    rc = popen_cmdexe(command, sernode_getnode);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_DEBUG, "get bns nodes failed\n");
        return RETURN_ERR;
    }

    return RETURN_OK;
}

int servnodehost_init(stat_config_t*pconfig, stat_server_t *pstatserver)
{
    int rc = 0;
    char *tmppos = NULL;
    char *tmpitem = NULL;

    tmppos = pconfig->conf_tgtnodes;
    while(1)
    {
        tmpitem = strsep(&tmppos, DEF_DELIM);
        if (!tmpitem)
        {
            break;
        }

        rc = sernode_getnode(tmpitem);
        if (rc < 0)
        {
            LOG_REC(LOG_LEVEL_ERR, "servnodehost_init get node failed\n");
            return RETURN_ERR;
        }
    }

    return RETURN_OK;    
}

int servnodeport_init(stat_config_t*pconfig, stat_server_t *pstatserver)
{
    int rc = 0;
    char *tmppos = NULL;
    char *tmpitem = NULL;

    tmppos = pconfig->conf_tgtports;
    while(1)
    {
        if (pstatserver->portnr >= PORTNR_MAX)
        {
            LOG_REC(LOG_LEVEL_ERR, "too many ports, limited :%d\n", PORTNR_MAX);
            return RETURN_ERR;
        }
    
        tmpitem = strsep(&tmppos, DEF_DELIM);
        if (!tmpitem)
        {
            break;
        }

        rc = conf_item_str2long(tmpitem, &pstatserver->ports[pstatserver->portnr]);
        if (rc < 0)
        {
            LOG_REC(LOG_LEVEL_ERR, "conf_item_str2long failed\n");
            return RETURN_ERR;
        }

        pstatserver->portnr++;
    }

    return RETURN_OK;    
}

int servdata_init(stat_config_t*pconfig, stat_server_t *pstatserver)
{
    int len = pconfig->conf_opdatalen + 1;
    char *tmpbuf = NULL;

    tmpbuf = zmalloc(len);
    if (!tmpbuf)
    {
        LOG_REC(LOG_LEVEL_ERR, "servdata_init allocate %ld bytes failed\n", len);
        return RETURN_ERR;
    }

    memset(tmpbuf, '1', pconfig->conf_opdatalen);
    pstatserver->valuedata = tmpbuf;

    return RETURN_OK;
}

int servtgtinfo_init(stat_config_t*pconfig, stat_server_t *pstatserver)
{
    int rc = 0;

    rc = servnodebns_init(pconfig, pstatserver);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "servnodebns_init failed\n");
        return RETURN_ERR;
    }

    rc = servnodehost_init(pconfig, pstatserver);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "servnodebns_init failed\n");
        return RETURN_ERR;
    }

    rc = servnodeport_init(pconfig, pstatserver);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "servnodeport_init failed\n");
        return RETURN_ERR;
    } 

    if (pstatserver->nodenr < 1 || pstatserver->portnr < 1)
    {
        LOG_REC(LOG_LEVEL_ERR, "no NODE or no PORT given\n");
        return RETURN_ERR;
    }

    return RETURN_OK;
}

void servdata_display(void)
{
    long tmpval = 0;
    long i = 0;
    stat_server_t *pstatserver = get_statserver();

    printf("STATISTIC_SERVER_DATA:\n"
        "  BASIC INFO:\n"
        "    optotalnr:%ld\n"
        "    opfailednr:%ld\n"
        "    latencynr:%ld\n"
        "    nodenr:%d\n"
        "    portnr:%d\n"
        "    defvalue:%s\n",
        pstatserver->optotalnr,
        atomic64_read(&pstatserver->opfailnr),
        pstatserver->laytencyclassnr,
        pstatserver->nodenr,
        pstatserver->portnr,
        pstatserver->valuedata);
    printf("  TIMECONST INFO:\n");
    for (i = 0; i < pstatserver->optimecostsize; i++)
    {
        tmpval = atomic64_read(&pstatserver->optimecoststat[i]);
        if (tmpval)
        {
            printf("    %ldms COST NR: %ld\n", i, tmpval);
        }
    }

    printf("  LATENCY CLASS INFO:\n");
    for (i = 0; i < pstatserver->laytencyclassnr; i++)
    {
        printf("    %ldth LATENCY: %ldms\n", i, pstatserver->laytencyclass[i]);
    }

    printf("  NODES INFO:\n");
    for (i = 0; i < pstatserver->nodenr; i++)
    {
        printf("    %ldth NODE: %s\n", i, pstatserver->nodes[i]);
    }

    printf("  PORTS INFO:\n");
    for (i = 0; i < pstatserver->portnr; i++)
    {
        printf("    %ldth PORT: %ld\n", i, pstatserver->ports[i]);
    }
}

int serverstat_init(void)
{
    int rc = 0;
    stat_server_t *pstatserver = get_statserver();
    stat_config_t *pconfig = get_statconfig();

    /* 1. clear space first */
    memset(pstatserver, 0, sizeof(*pstatserver));

    /* 2. init related signal */
    rc = servcoststat_init(pconfig, pstatserver);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "servcoststat_init failed\n");
        return RETURN_ERR;
    }

    /* 3. init target nodes and ports information */
    rc = servtgtinfo_init(pconfig, pstatserver);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "servtgtinfo_init failed\n");
        return RETURN_ERR;
    }

    /* 4. init default value for redis command */
    rc = servdata_init(pconfig, pstatserver);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "servdata_init failed\n");
        return RETURN_ERR;
    }

    //servdata_display();

    return RETURN_OK;
}

void signal_init(void)
{
    signal(SIGABRT, signal_abort_handler);
}

void serv_lock(void)
{
    stat_server_t *pstatserver = get_statserver();

    pthread_mutex_lock(&pstatserver->serv_mutex);
}

void serv_unlock(void)
{
    stat_server_t *pstatserver = get_statserver();
    
    pthread_mutex_unlock(&pstatserver->serv_mutex);
}

void serv_lock_init(void)
{
    stat_server_t *pstatserver = get_statserver();
    
    pthread_mutex_init(&pstatserver->serv_mutex, NULL);
    pstatserver->mutex_inited = 1;
}

void serv_lock_finalize(void)
{
    stat_server_t *pstatserver = get_statserver();

    if (pstatserver->mutex_inited)
    {
        pthread_mutex_destroy(&pstatserver->serv_mutex);
        pstatserver->mutex_inited = 0;
    }
}

int server_init(int argc, char *argv[])
{
    int rc = 0;

    conf_definit();
    log_init();

    /* 1. do configurations */
    rc = conf_init(argc, argv);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "conf_init failed\n");
        return RETURN_ERR;
    }

    log_reinit();

    /* 2. init servstat */
    rc = serverstat_init();
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "serverstat_init failed\n");
        return RETURN_ERR;
    }

    /* 3. init related signal */
    signal_init();

    /* 4. init service lock */
    serv_lock_init();
        
    return RETURN_OK;
}

void server_finalize(void)
{
    int i = 0;
    stat_server_t *pstatserver = get_statserver();

    free_space((void **)&pstatserver->optimecoststat);
    for (i = 0; i < pstatserver->nodenr; i++)
    {
        free_space((void **)&pstatserver->nodes[i]);
    }

    serv_lock_finalize();  
    free_space((void **)&pstatserver->valuedata);

    log_finalize();    
}

int start_run(void)
{
    int rc = 0;
    stat_config_t*pconfig = get_statconfig();

    rc = op_run();
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_WARNING, 
                "operation type:%s failed\n", pconfig->conf_opname);
        return RETURN_ERR;
    }
    
    return RETURN_OK;
}

