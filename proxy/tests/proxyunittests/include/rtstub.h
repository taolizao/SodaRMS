/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file rtstub.h
 * @author taolizao(taolizao@gmail.com)
 *
 */

#ifndef TSTINC_RTSTUB_HEAD_H
#define TSTINC_RTSTUB_HEAD_H

int rtstub_init(void);
int rtstub_create(void *func, void *stub_func);
void rtstub_destroy(void *func);
void rtstub_finalize(void);

#endif

