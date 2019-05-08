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
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <nc_core.h>
#include <nc_log.h>
#include <nc_message.h>

extern bool
redis_args(struct msg *r);

void lua_testcase1(void)
{
    bool rc = true;

    struct msg r;

    r.type = MSG_REQ_REDIS_SCRIPT;
    rc = redis_args(&r);
    CU_ASSERT(rc == true);
}

void lua_testcase2(void)
{
    bool rc = true;

    struct msg r;

    r.type = MSG_REQ_REDIS_PERSIST;
    rc = redis_args(&r);
    CU_ASSERT(rc == false);
}

CU_TestInfo lua_testcases[] = 
{
    {"lua_testcase1", lua_testcase1},
    {"lua_testcase2", lua_testcase2},
    CU_TEST_INFO_NULL
};
