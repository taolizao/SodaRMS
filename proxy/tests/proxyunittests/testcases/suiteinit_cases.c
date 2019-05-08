/**
 * @file example_testcases.c
 * @author taolizao@gmail.com
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

void suitinit_case1(void)
{
    //printf("This case is for suite_init\n");
}

CU_TestInfo g_suitinitcases[] = 
{
    {"suitinit_case1", suitinit_case1},
    CU_TEST_INFO_NULL
};

void suitend_case1(void)
{
    //printf("This case is for suite_end\n");
}

CU_TestInfo g_suitendcases[] = 
{
    {"suitend_case1", suitend_case1},
    CU_TEST_INFO_NULL
};

