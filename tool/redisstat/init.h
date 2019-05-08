/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file list.h
 * @author taolizao33@gmail.com
 *
 */

#ifndef BDRP_TOOL_INIT_H
#define BDRP_TOOL_INIT_H

enum {
    OP_TYPE_GET = 0,
    OP_TYPE_SET,
    OP_TYPE_NR,
};

typedef struct stat_config {
    char conf_tgtbns[BNS_STR_MAXLEN];  /*target bns name*/
    char conf_tgtnodes[NODE_SET_STR_MAXLEN]; /*target nodes name with delim ','*/
    char conf_tgtports[PORT_SET_STR_MAXLEN]; /*target ports with delim ','*/
    long conf_thdnr;       /*thread number*/
    long conf_thdopnr;     /*operation for each thread*/
    char conf_opname[OPNAME_MAXLEN];
    char conf_keyprefix[PREFIX_MAXLEN];
    char conf_fieldprefix[PREFIX_MAXLEN];
    char conf_valueprefix[PREFIX_MAXLEN];
    char conf_cmdaddtion[CMD_ADDTION_MAXLEN];
    long conf_opdatalen;   /*datalen for each operation*/
    long conf_oploopnr;    /*loop number of operation*/
    long conf_maxlatencyms;           /*maxmum laytency time with unit millisecond*/
    char conf_latencyclass[OPTSTR_MAX_LEN];/*string of latency class*/
    long conf_latencyclassnr;
    long conf_optimeoutms;    /*timeout set for operation*/
    long conf_keystartno;     /* start index of key */
    char conf_keysame;   /* operation on same key */
    char conf_fieldsame; /* operation on same field */
    char conf_display_cmdreply;
    char conf_pad[3];
} stat_config_t;

typedef struct stat_server {
    atomic64_t *optimecoststat;  /*statistic buf for time const of operations*/
    long optimecostsize;  /*sizeof statistic buf*/
    atomic64_t opfailnr;  /*number of failed operations*/
    long optotalnr;       /*total operation numbers*/
    long laytencyclass[LATENCY_CLASS_MAX]; /*latency classes*/
    long laytencyclassnr;    /*number of latency classes*/
    char *nodes[NODENR_MAX]; /*node name array*/
    int nodenr;              /*node number*/
    long ports[PORTNR_MAX];  /*port array*/
    int portnr;              /*number of ports*/
    char *valuedata;         /*data with certain length*/
    long totalus;
    pthread_mutex_t serv_mutex;
    int mutex_inited;
} stat_server_t;

stat_config_t *get_statconfig(void);
void conf_definit(void);
int conf_init(int argc, char **argv);

int server_init(int argc, char *argv[]);
void server_finalize(void);
int start_run(void);
stat_server_t *get_statserver(void);

void serv_lock(void);
void serv_unlock(void);
void serv_lock_init(void);
void serv_lock_finalize(void);

#endif  /*BDRP_TOOL_INIT_H*/

