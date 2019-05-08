/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file const.h
 * @author taolizao33@gmail.com
 *
 */

#ifndef BDRP_TOOL_CONST_H
#define BDRP_TOOL_CONST_H

enum {
    PATH_MAX_LEN = 256,
    BNS_STR_MAXLEN = 64,
    POPEN_RESLINE_MAXLEN = 1024,
    SHELLCMD_MAXLEN = 1024,
    KEY_MAXLEN = 128,
    FIELD_MAXLEN = 128,
    RESERV_MAXLEN = 128,
    
    OPTSTR_MAX_LEN = 128,
    OP_MAX_LATENCY_MS = 10 * 1000,
    OP_TIMEOUT_DEF_MS = 10,
    LATENCY_CLASS_MAX = 128,

    NODE_SET_STR_MAXLEN = 1024,
    PORT_SET_STR_MAXLEN = 128,

    NODENR_MAX = 1024,
    PORTNR_MAX = 128,

    OPNAME_MAXLEN = 32,
    PREFIX_MAXLEN = 128,
    CMD_ADDTION_MAXLEN = 1024,
};

enum {
    RETURN_OK = 0,
    RETURN_ERR = -1,
};

#endif  /*BDRP_TOOL_CONST_H*/

