/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file oprations.c
 * @author taolizao33@gmail.com
 *
 */
 
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>
#include <pthread.h>

#include "const.h"
#include "log.h"
#include "atomic.h"
#include "init.h"
#include "util.h"
#include "thread.h"

#include "hiredis.h"
#include "operations.h"

redisContext *get_next_conn(opration_privdata_t *privdata)
{        
    redisContext *rcontxt = NULL;
    
    if (privdata->nextconn >= privdata->connnr)
    {        
        privdata->nextconn = 0;
        rcontxt = privdata->redisconns[0];
    }
    else
    {
        rcontxt = privdata->redisconns[privdata->nextconn];
        privdata->nextconn++;
    }

    //fprintf_stdout("get redisconn:%p connnr:%ld nextconnidx:%ld\n", 
    //                rcontxt, privdata->connnr, privdata->nextconn);
    
    return rcontxt;
}

int redisconn_init(stat_config_t*pconfig, 
                   stat_server_t *pstatserver, 
                   opration_privdata_t *pprivdata)
{
    long count = 0;
    long len = 0;
    long portidx = 0;
    long nodeidx = 0;
    struct timeval tv;
    redisContext **tmpconns = NULL;
    redisContext *redisconn = NULL;

    tv.tv_sec = pconfig->conf_optimeoutms / 1000;
    tv.tv_usec = (pconfig->conf_optimeoutms - tv.tv_sec * 1000) * 1000;

    count = pstatserver->nodenr * pstatserver->portnr;
    av_assert0(count != 0);

    len = count * sizeof(redisContext *);
    tmpconns = (redisContext **)zmalloc(len);
    if (!tmpconns)
    {
        LOG_REC(LOG_LEVEL_ERR, "alloc %ld bytes for connections failed\n", len);
        return RETURN_ERR;
    }

    pprivdata->redisconns = tmpconns;

    pprivdata->connnr = 0;
    for (portidx = 0; portidx < pstatserver->portnr; portidx++)
    {
        for (nodeidx = 0; nodeidx < pstatserver->nodenr; nodeidx++)
        {                               
            redisconn = redisConnectWithTimeout(pstatserver->nodes[nodeidx], 
                                     pstatserver->ports[portidx],
                                     tv);                                        
            if (redisconn->err)  
            {
                redisFree(redisconn);
                LOG_REC(LOG_LEVEL_ERR, 
                    "create redis connect(idx:%ld) to %s:%ld failed\n", 
                    pprivdata->connnr, 
                    pstatserver->nodes[nodeidx],
                    pstatserver->ports[portidx]);
                return RETURN_ERR;
            }

            
            redisSetTimeout(redisconn, tv);
            
            pprivdata->redisconns[pprivdata->connnr] = redisconn;  
            pprivdata->connnr++;                
        }
    }        

    av_assert0(pprivdata->connnr == count);

    return RETURN_OK;
}

int op_gen_privdata(thread_pool_t *pthdpool)
{
    int rc = 0;
    long real_start = 0;    
    int thdidx = 0;
    int len = sizeof(opration_privdata_t);
    thread_desc_t *pthddesc = NULL;
    opration_privdata_t *tmpopprivdata = NULL;
    stat_config_t *pconfig = get_statconfig();
    stat_server_t *pstatserver = get_statserver();

    real_start = pconfig->conf_keystartno;
    for (thdidx = 0; thdidx < pthdpool->thdpool_size; thdidx++)
    {
        pthddesc = &pthdpool->thdpool_thds[thdidx];
        tmpopprivdata = (opration_privdata_t *)zmalloc(len);
        if (!tmpopprivdata)
        {
            pthddesc->thread_res = RETURN_ERR;
            LOG_REC(LOG_LEVEL_ERR, "gen_set_privdata for thread:%d failed\n", thdidx);
            return RETURN_ERR;
        }

        pthddesc->thread_priv = tmpopprivdata;
        
        tmpopprivdata->start_no = real_start;
        real_start += pconfig->conf_thdopnr;
        tmpopprivdata->end_no = real_start;
        tmpopprivdata->thread_desc = pthddesc;

        rc = redisconn_init(pconfig, pstatserver, tmpopprivdata);
        if (rc < 0)
        {
            LOG_REC(LOG_LEVEL_ERR, "redisconn_init for thread:%d failed\n", thdidx);
            return RETURN_ERR;
        }       
    }
    
    return RETURN_OK;
}

int op_release_thdprivdata(void **priv)
{ 
    long connidx = 0; 
    opration_privdata_t *thdprivdata = (opration_privdata_t *)(*priv);

    if (!thdprivdata)
    {
        return RETURN_OK;
    }
    
    for (connidx = 0; connidx < thdprivdata->connnr; connidx++)
    {
        if (thdprivdata->redisconns[connidx])
        {
            redisFree(thdprivdata->redisconns[connidx]);
            thdprivdata->redisconns[connidx] = NULL;
        }
    } 

    free_space((void **)&thdprivdata->redisconns);
    thdprivdata->connnr = 0;

    free_space(priv);
    
    return RETURN_OK;
}

void op_analyze(void)
{
    long timecostidx = 0;
    long totalcnt = 0;
    long totalsuccessnr = 0;
    long curlaytencyclass = 0;
    int laytencyidx = 0;
    long tmpcount = 0;
    long tmpnr = 0;
    long opfailednr = 0;
    float percent = 0.0;
    long ttimems = 0;
    stat_config_t *pconfig = get_statconfig();
    stat_server_t *pstatserver = get_statserver();

    //printf("analyze_result\n");

    if (!pstatserver->laytencyclassnr)
    {
        curlaytencyclass = -1;
    }
    else
    {
        curlaytencyclass = pstatserver->laytencyclass[laytencyidx];
    }

    fprintf_stdout("======================================================\n");
    fprintf_stdout("===============STATISTIC INFORMATION==================\n");
    fprintf_stdout("TOTAL_OPERATION_NR: %ld\n", pstatserver->optotalnr);
    
    for (timecostidx = 0; timecostidx <= pconfig->conf_maxlatencyms; timecostidx++)
    {        
        tmpnr = atomic64_read(&pstatserver->optimecoststat[timecostidx]);       
        if (tmpnr > 0)
        {
            totalcnt += tmpnr;
        }
        //fprintf_stdout("OP TIMECOST(%ld) NR:%ld, SUCCNR:%ld\n", 
        //            timecostidx, tmpnr, totalcnt);

        if (timecostidx == curlaytencyclass)
        {
            //percent = ((float)tmpcount)/((float)pstatserver->optotalnr);
            percent = ((float)totalcnt) / ((float)pstatserver->optotalnr);
            percent *= 100.0;
            fprintf_stdout("OPERATIONNR_TIMECOST(<=%ldms):%ld  PERCENT:%.2f%\n", 
                    timecostidx, totalcnt, percent);           
            laytencyidx++;
            if (laytencyidx < pstatserver->laytencyclassnr)
            {
                curlaytencyclass = pstatserver->laytencyclass[laytencyidx];
            }
            else
            {
                curlaytencyclass = -1;
            }
        }
    }

    opfailednr = atomic64_read(&pstatserver->opfailnr);
    tmpcount = atomic64_read(&pstatserver->optimecoststat[timecostidx]);
    //fprintf_stdout("OP TIMECOST(%ld) NR:%ld\n", timecostidx, tmpcount);
    //maxlatencyuse = atomic64_read(&pstatserver->optimecoststat[timecostidx + 1]);
    

    //fprintf_stdout("total:%ld success:%ld failed:%ld\n",
    //            pstatserver->optotalnr, totalcnt, opfailednr);
               
    totalsuccessnr = pstatserver->optotalnr - opfailednr;
    av_assert0((totalcnt + tmpcount) ==  totalsuccessnr); 

    percent = ((float)totalcnt) / ((float)pstatserver->optotalnr);
    percent *= 100.0;
    fprintf_stdout("OPERATIONNR_TIMECOST(<=%ldms):%ld  PERCENT:%.2f%\n", 
                    pconfig->conf_maxlatencyms, totalcnt, percent);

    percent = ((float)tmpcount) / ((float)pstatserver->optotalnr);
    percent *= 100.0;
    fprintf_stdout("OPERATIONNR_TIMECOST(>%ldms):%ld  PERCENT:%.2f%\n", 
                    pconfig->conf_maxlatencyms, tmpcount, percent);

    percent = ((float)totalsuccessnr) / ((float)pstatserver->optotalnr);
    percent *= 100.0;
    fprintf_stdout("OPRATIONNR_SUCCESS:%ld  PERCENT:%.2f%\n", 
                    totalsuccessnr, percent);

    percent = ((float)opfailednr) / ((float)pstatserver->optotalnr);
    percent *= 100.0;
    fprintf_stdout("OPRATIONNR_FAILED:%ld   PERCENT:%.2f%\n", 
                    opfailednr, percent);

    ttimems = pstatserver->totalus / 1000;
    ttimems = ttimems ? ttimems : 1;
    //av_assert0(ttimems > 0);
    fprintf_stdout("OPRATION_TOTALTIME:%ldms  AVGTIME:%.2fms  QPS:%ld\n",
                   ttimems, 
                   ((float)ttimems) / ((float)pstatserver->optotalnr),
                   (pstatserver->optotalnr * 1000) / ttimems);               
    

    fprintf_stdout("======================================================\n");           
}

void op_res_display(char *precont, char *cmdbuf, redisReply *reply)
{
    size_t nr = 0;
    char tmp[32];
    redisReply *subreply = NULL;
    
    if (reply->type == REDIS_REPLY_ERROR) {
        fprintf_stdout("%s[%s] [ERROR] [errstr:%s]\n", 
                        precont, cmdbuf, reply->str);
    } else if (reply->type == REDIS_REPLY_STRING) {
        fprintf_stdout("%s[%s] [STRING] [%s]\n", precont, cmdbuf, reply->str);
    } else if (reply->type == REDIS_REPLY_INTEGER) {
        fprintf_stdout("%s[%s] [INTEGER] [%ld]\n", precont, cmdbuf, reply->integer);
    } else if (reply->type == REDIS_REPLY_NIL) {
        fprintf_stdout("%s[%s] [NIL]\n", precont, cmdbuf);
    } else if (reply->type == REDIS_REPLY_STATUS) {
        fprintf_stdout("%s[%s] [STATUS] [%s]\n", precont, cmdbuf, reply->str);
    } else if (reply->type == REDIS_REPLY_ARRAY) {
        fprintf_stdout("%s[%s] [ARRAY]\n", precont, cmdbuf);
        for (nr = 0; nr < reply->elements; nr++) {
            subreply = reply->element[nr];
            snprintf(tmp, sizeof(tmp), "%ld", nr);
            op_res_display("  ", tmp, subreply);
        }
    }
}

int op_core_task(opration_privdata_t *privdata, 
                     stat_config_t *pconfig,
                     stat_server_t *pstatserver,
                     char *cmdbuf,
                     long cmdbuflen)
{   
    long idx = 0;  
    long toolongidx = 0;
    redisContext *contxt = NULL;
    redisReply *reply = NULL;
    struct timeval timestart;
    struct timeval timeend;
    long timedurms = 0;
    long timedurus = 0;

    //fprintf_stdout("set startno:%ld endno:%ld\n", 
    //                privdata->start_no, privdata->end_no);
    for (idx = privdata->start_no; idx < privdata->end_no; idx++)
    {
        if (pconfig->conf_keysame) {
            if (pconfig->conf_fieldsame) {
                snprintf(cmdbuf, cmdbuflen, "%s %s %s %s%s %s", 
                     pconfig->conf_opname, 
                     pconfig->conf_keyprefix,
                     pconfig->conf_fieldprefix, 
                     pconfig->conf_valueprefix, pstatserver->valuedata,
                     pconfig->conf_cmdaddtion);
            } else {
                snprintf(cmdbuf, cmdbuflen, "%s %s %s%ld %s%s %s", 
                     pconfig->conf_opname, 
                     pconfig->conf_keyprefix,
                     pconfig->conf_fieldprefix, idx, 
                     pconfig->conf_valueprefix, pstatserver->valuedata,
                     pconfig->conf_cmdaddtion);
            }
        } else {    
            if (pconfig->conf_fieldsame) {
                snprintf(cmdbuf, cmdbuflen, "%s %s%ld %s %s%s %s", 
                     pconfig->conf_opname, 
                     pconfig->conf_keyprefix, idx,
                     pconfig->conf_fieldprefix, 
                     pconfig->conf_valueprefix, pstatserver->valuedata,
                     pconfig->conf_cmdaddtion);
            } else {
                snprintf(cmdbuf, cmdbuflen, "%s %s%ld %s%ld %s%s %s", 
                     pconfig->conf_opname, 
                     pconfig->conf_keyprefix, idx,
                     pconfig->conf_fieldprefix, idx,
                     pconfig->conf_valueprefix, pstatserver->valuedata,
                     pconfig->conf_cmdaddtion);
            }
        }

        //fprintf_stdout("\nredis command(%s)\n", cmdbuf);
               
        contxt = get_next_conn(privdata);
        
        get_time_tv(&timestart);
        
        reply = (redisReply *)redisCommand(contxt, cmdbuf);
        
        get_time_tv(&timeend);
        timedurus = get_time_dur_us(timestart, timeend);
        timedurms = timedurus / 1000;
        privdata->ttimeus += timedurus;

        LOG_REC(LOG_LEVEL_WARNING, "COMMAND TIME COST %ld us\n", timedurus);
        //fprintf_stdout("SET OPERATION USING TIME:%ldms AND REPLY:%p\n",
        //                timedurms, reply);
            
        if (reply == NULL)
        {
            LOG_REC(LOG_LEVEL_WARNING, "op command(%s) failed(%d:%s)\n", 
                    cmdbuf, contxt->err, contxt->errstr);
            serv_lock();
            atomic64_inc(&pstatserver->opfailnr);
            serv_unlock();
        }
        else
        {
            if (pconfig->conf_display_cmdreply) {
                op_res_display("", cmdbuf, reply);
            }
            
            if ((reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK"))
                || (reply->type == REDIS_REPLY_ERROR)
                || (reply->type == REDIS_REPLY_STRING && strstr(reply->str, "ERR"))) {
                LOG_REC(LOG_LEVEL_WARNING, "op command(%s) failed(%s)\n", cmdbuf, reply->str);
                serv_lock();
                atomic64_inc(&pstatserver->opfailnr);
                serv_unlock();
                freeReplyObject(reply);
                continue;
            }
            
            //fprintf_stdout("set op starttime:%ld endtime:%ld durtime:%ld "
            //                "maxlay:%ld\n", timestart, timeend, timedur, 
            //                pconfig->conf_maxlatencyms);
            if (timedurms > pconfig->conf_maxlatencyms)
            {
                toolongidx = pconfig->conf_maxlatencyms + 1;
                //fprintf_stdout("op command(%s) use more than %ld ms record in %ldth timeconstunit\n", 
                //        cmdbuf, pconfig->conf_maxlatencyms, toolongidx);
                serv_lock();
                atomic64_inc(&pstatserver->optimecoststat[toolongidx]);
                atomic64_add(timedurms, &pstatserver->optimecoststat[toolongidx + 1]);
                serv_unlock();
            }
            else
            {
                //fprintf_stdout("op command(%s) use %ld ms\n", 
                //        cmdbuf, timedur);
                serv_lock();
                atomic64_inc(&pstatserver->optimecoststat[timedurms]);
                serv_unlock();
            }
            freeReplyObject(reply);
        }
    }

    //fprintf_stdout("set startno:%ld endno:%ld DONE\n", 
    //                privdata->start_no, privdata->end_no);

    return RETURN_OK;
}

int op_handler(void *privdata)
{
    int rc = 0;
    int err = 0;
    long totalopnr = 0;
    int loopnr = 0;
    char *command = NULL;
    long commandlen = 0;
    long timems = 0;
    opration_privdata_t *tmpopprivdata = (opration_privdata_t *)privdata;
    stat_config_t *pconfig = get_statconfig();
    stat_server_t *pstatserver = get_statserver();

    commandlen = pconfig->conf_opdatalen + KEY_MAXLEN + FIELD_MAXLEN + RESERV_MAXLEN;
    command = zmalloc(commandlen);
    if (!command)
    {
        LOG_REC(LOG_LEVEL_ERR, "alloc %ld bytes for redis command failed\n", commandlen);
        return RETURN_ERR;
    }

    for (loopnr = 0; loopnr < pconfig->conf_oploopnr; loopnr++)
    {       
        err = op_core_task(tmpopprivdata, pconfig, pstatserver, command, commandlen);
        if (err < 0)
        {
            LOG_REC(LOG_LEVEL_ERR, "do_set_operation for loop %d failed\n", loopnr);
            rc = RETURN_ERR;
        }     
    }

    av_assert0(tmpopprivdata->ttimeus > 0);
    
    totalopnr = pconfig->conf_oploopnr * 
                (tmpopprivdata->end_no - tmpopprivdata->start_no);
    timems = tmpopprivdata->ttimeus / 1000;    
    timems = timems ? timems : 1;
    fprintf_stdout("STATISTIC OF THREAD %d: TOTALOPNR=%ld TIMECOST=%ldms "
                   "AVGTIME=%.2fms QPS=%ld\n",
                   tmpopprivdata->thread_desc->thread_id,
                   totalopnr, 
                   timems,
                   ((float)tmpopprivdata->ttimeus / totalopnr) / 1000.0,
                   (totalopnr * 1000000) / tmpopprivdata->ttimeus);

    free_space((void **)&command);
    
    return rc;
}

int op_create(thread_pool_t *pthdpool, 
          char *poolname, 
          int thdnr, 
          void *thd_hdl,
          void *thd_analyze,
          void *thd_releasepriv,
          thdpool_thdpriv_gen thdpriv_gen)
{
    int rc = 0;
    int err = 0;
    struct timeval timestart;
    struct timeval timeend;
    stat_server_t *pstatserver = get_statserver();

    //fprintf_stdout("thread_pool_init\n");
    err = thread_pool_init(pthdpool, poolname, thdnr, 
                     thd_hdl, thd_analyze, thd_releasepriv, thdpriv_gen);
    if (err < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "%s thread_pool_init failed\n", poolname);
        thread_pool_destroy(pthdpool);
        return RETURN_ERR;
    }

    get_time_tv(&timestart);
    
    //fprintf_stdout("thread_pool_start\n");
    err = thread_pool_start(pthdpool);
    if (err < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "%s thread_pool_start failed\n", poolname);
        rc = RETURN_ERR;
    }

    //fprintf_stdout("thread_pool_wait\n");
    err = thread_pool_wait(pthdpool);
    if (err < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "%s thread_pool_wait failed\n", poolname);
        rc = RETURN_ERR;
    }

    get_time_tv(&timeend);
    pstatserver->totalus = get_time_dur_us(timestart, timeend);

    //fprintf_stdout("thread_pool_destroy\n");
    thread_pool_destroy(pthdpool);
    
    return rc;
}

int op_run(void)
{
    int rc = 0;
    thread_pool_t thdpool;
    stat_config_t *pconfig = get_statconfig();
        
    rc = op_create(&thdpool, pconfig->conf_opname, pconfig->conf_thdnr, 
               op_handler, NULL, op_release_thdprivdata, op_gen_privdata);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "SET operation failed\n");
        return RETURN_ERR;
    }

    op_analyze();
   
    return RETURN_OK;
}

