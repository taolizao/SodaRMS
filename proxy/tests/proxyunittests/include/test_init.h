/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file test_init.h
 * @author taolizao(taolizao@gmail.com)
 *
 */

#ifndef TESTINC_TEST_INIT_H
#define TESTINC_TEST_INIT_H

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>

int service_init(void);
int service_finalize(void);
int suite_success_init(void);
int suite_success_clean(void);

#endif

