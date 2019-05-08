/***************************************************************************
 * 
 * Copyright (c) 2015 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file config.c
 * @author taolizao33@gmail.com
 *
 */
 
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include "const.h"
#include "log.h"
#include "atomic.h"
#include "init.h"
#include "util.h"

const char *DEF_DELIM = ",";
const char *no_bns = "NO_BNS";
const char *no_tgt = "127.0.0.1";
const char *no_port = "9999";
const char *no_latency = "4,10,20,50,100";
const char *optstring = "b:i:p:t:n:o:d:L:l:m:c:T:s:k:f:v:a:DSFRh";
const char *deflogpath = "/tmp/redisstat.log";

stat_config_t g_statconfig;

stat_config_t *get_statconfig(void)
{
    return &g_statconfig;
}

void conf_definit(void)
{
    stat_config_t*pconfig = get_statconfig();
    log_desc_t *pglog = get_logdesc();

    memset(pconfig, 0, sizeof(*pconfig));
    
    pconfig->conf_thdnr = 1;
    pconfig->conf_thdopnr = 1;
    snprintf(pconfig->conf_opname, sizeof(pconfig->conf_opname), "get");
    memset(pconfig->conf_keyprefix, 0, sizeof(pconfig->conf_keyprefix));
    memset(pconfig->conf_fieldprefix, 0, sizeof(pconfig->conf_fieldprefix));
    memset(pconfig->conf_valueprefix, 0, sizeof(pconfig->conf_valueprefix));
    memset(pconfig->conf_cmdaddtion, 0, sizeof(pconfig->conf_cmdaddtion));
    pconfig->conf_opdatalen = 10;
    pconfig->conf_oploopnr = 1;
    snprintf(pglog->log_path, PATH_MAX_LEN, "%s", deflogpath);
    pglog->loglevel_limit = LOG_LEVEL_WARNING;
    pconfig->conf_maxlatencyms = OP_MAX_LATENCY_MS;
    pconfig->conf_optimeoutms = OP_TIMEOUT_DEF_MS;
    snprintf(pconfig->conf_tgtbns, BNS_STR_MAXLEN, "%s", no_bns);
    snprintf(pconfig->conf_tgtnodes, BNS_STR_MAXLEN, "%s", no_tgt);
    snprintf(pconfig->conf_tgtports, BNS_STR_MAXLEN, "%s", no_port);
    snprintf(pconfig->conf_latencyclass, OPTSTR_MAX_LEN, "%s", no_latency);
    pconfig->conf_keysame = 0;
    pconfig->conf_fieldsame = 1;
    pconfig->conf_keystartno = 1;
    pglog->log2out = 1;
    pconfig->conf_display_cmdreply = 0;
}

void conf_display(void)
{
    stat_config_t*pconfig = get_statconfig();
    log_desc_t *pglog = get_logdesc();
    
    printf("targetbns:%s targetnode:%s targetport:%s threadnr:%ld  "
           "threadopnr:%ld  optype:%s  keyprefix:%s fieldprefix:%s "
           "valueprefix:%s cmdaddtion:%s opdatalen:%ld "
           "oploopnr:%ld  logpath:%s  loglevel:%d  maxlatencyms:%ld "
           "latencyclass:%s  optimeoutms:%ld keystartidx:%ld "
           "keysame:%d  fieldsame:%d displaylog:%d displaycmdres:%d\n",
           pconfig->conf_tgtbns,
           pconfig->conf_tgtnodes,
           pconfig->conf_tgtports,
           pconfig->conf_thdnr, 
           pconfig->conf_thdopnr,
           pconfig->conf_opname,
           pconfig->conf_keyprefix,
           pconfig->conf_fieldprefix,
           pconfig->conf_valueprefix,
           pconfig->conf_cmdaddtion,
           pconfig->conf_opdatalen,
           pconfig->conf_oploopnr,
           pglog->log_path,
           pglog->loglevel_limit,
           pconfig->conf_maxlatencyms,
           pconfig->conf_latencyclass,
           pconfig->conf_optimeoutms,
           pconfig->conf_keystartno,
           pconfig->conf_keysame,
           pconfig->conf_fieldsame,
           pglog->log2out,
           pconfig->conf_display_cmdreply);
}

static void conf_help(char *exebin)
{
    printf("Usage as: %s -b XX -i XX -p XX -t XX -n XX -o XX -d XX " \
           "-L XX -l XX -m XX -c XX -T XX -s XX -k XX -f XX -v XX " \
           "-a XX -D -S -F -R -h\n" \
           "    -b    bns of target nodes, default %s\n" \
           "    -i    IP/hostname of target nodes, delim with ',' , default %s\n" \
           "    -p    port list of target nodes, delim with ',' , default %s\n" \
           "    -t    number of threads, default 1\n" \
           "    -n    number of operations for each thread, default 1\n" \
           "    -o    operation type, such as 'set', 'get' etc. default set\n" \
           "    -d    data length of operation, default 10\n" \
           "    -L    loop count for test, default 1\n" \
           "    -l    path of log file, default as %s\n" \
           "    -m    max latency time(ms), default %ld\n" \
           "    -c    latency class string with delim ',', such as 10,20, default %s\n" \
           "    -T    timeout(ms) set for each operation, default %d\n" \
           "    -s    start number of key index, default as 1\n" \
           "    -k    name prefix of KEY, default ''\n" \
           "    -f    name prefix of FIELD, default ''\n" \
           "    -v    name prefix of VALUE, default ''\n" \
           "    -a    addition content of command, default ''\n" \
           "    -S    refers to do operation on same key, "
           "          if use all keys name is -k argument\n" \
           "    -F    refers to do operation on different key field, "
           "          if not use all fileds name is -f argument\n" \
           "    -D    not display log information\n" \
           "    -R    display command result\n" \
           "    -h    get help information\n",
           exebin,
           no_bns,
           no_tgt,
           no_port,           
           deflogpath,
           OP_MAX_LATENCY_MS,
           no_latency,
           OP_TIMEOUT_DEF_MS);
}

static int conf_load(int argc, char **argv)
{    
    int rc = 0;
    int c = 0;
    opterr = 0;
    stat_config_t*pconfig = get_statconfig();
    log_desc_t *pglog = get_logdesc();

    while(1)
    {
        c = getopt(argc, argv, optstring);
        if (c == -1)
        {
            break;
        }
        
        switch(c)
        {
        case 'b':
            snprintf(pconfig->conf_tgtbns, BNS_STR_MAXLEN, "%s", optarg);
            break;
        case 'i':
            snprintf(pconfig->conf_tgtnodes, NODE_SET_STR_MAXLEN, "%s", optarg);
            break;
        case 'p':
            snprintf(pconfig->conf_tgtports, PORT_SET_STR_MAXLEN, "%s", optarg);
            break;
        case 't':
            rc = conf_item_str2long(optarg, &pconfig->conf_thdnr);
            break;
        case 'n':
            rc = conf_item_str2long(optarg, &pconfig->conf_thdopnr);
            break;
        case 'o':
            snprintf(pconfig->conf_opname, sizeof(pconfig->conf_opname), "%s", optarg);
            break;
        case 'k':
            snprintf(pconfig->conf_keyprefix, sizeof(pconfig->conf_keyprefix), "%s", optarg);
            break;
        case 'f':
            snprintf(pconfig->conf_fieldprefix, sizeof(pconfig->conf_fieldprefix), "%s", optarg);
            break;
        case 'v':
            snprintf(pconfig->conf_valueprefix, sizeof(pconfig->conf_valueprefix), "%s", optarg);
            break;
        case 'a':
            snprintf(pconfig->conf_cmdaddtion, sizeof(pconfig->conf_cmdaddtion), "%s", optarg);
            break;
        case 'd':
            rc = conf_item_str2long(optarg, &pconfig->conf_opdatalen);
            break;
        case 'L':
            rc = conf_item_str2long(optarg, &pconfig->conf_oploopnr);
            break;
        case 'l':
            snprintf(pglog->log_path, PATH_MAX_LEN, "%s", optarg);
            break;
        case 'm':
            rc = conf_item_str2long(optarg, &pconfig->conf_maxlatencyms);
            break;
        case 'c':
            snprintf(pconfig->conf_latencyclass, OPTSTR_MAX_LEN, "%s", optarg);
            break;
        case 'T':
            rc = conf_item_str2long(optarg, &pconfig->conf_optimeoutms);
            break;
        case 's':
            rc = conf_item_str2long(optarg, &pconfig->conf_keystartno);
            break;
        case 'S':
            pconfig->conf_keysame = 1;
            break;
        case 'F':
            pconfig->conf_fieldsame = 0;
            break;
        case 'D':
            pglog->log2out = 0;
            break;
        case 'R':
            pconfig->conf_display_cmdreply = 1;
            break;
        case 'h':
            conf_help(argv[0]);
            exit(0);
            break;
        default:
            LOG_REC(LOG_LEVEL_WARNING, "unrecgnize argument '%d'\n", c);
            break;
        }

        if (rc < 0)
        {
            LOG_REC(LOG_LEVEL_ERR, "conf load failed\n");
            return rc;
        }
    }
   
    return 0;
}

int conf_init(int argc, char **argv)
{
    int rc = 0;
    
    rc = conf_load(argc, argv);
    if (rc < 0)
    {
        LOG_REC(LOG_LEVEL_ERR, "load config from arguments failed\n");
    }

    conf_display();

    return rc;
}

