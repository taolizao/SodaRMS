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
#include <Automated.h>
#include <CUError.h>
#include <MyMem.h>
#include <TestRun.h>
#include <Util.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <nc_core.h>
#include <nc_log.h>

#include "rtstub.h"

#define INFO(msg) print_info(msg)

void print_info(char *msg){
    printf("%s\n", msg);
}

// cunit test case for function log_init
void proxy_slowlog_testcase1(void)
{
    int level;
    int rc = 0;

    INFO("function log_init cunit test case start......");
    INFO("start to create nutcracker instance structure");
    struct instance nci;
    INFO("set slow_log_filename to \"proxy_unit_test_slowlog_filename\"");
    nci.slow_log_filename = "proxy_unit_test_slowlog_filename";
    INFO("set slow_log_switch to 1, make slow log function work");
    nci.slow_log_switch = 1;
    INFO("set slow_query_time_limit to 10ms");
    nci.slow_query_time_limit = 10;
    INFO("nutcracker instance create success");
    INFO("set normal log level to LOG_EMERG");
    level = LOG_EMERG;
    INFO("set normal log name to proxy_unit_test_log_filename");
    char *name = "proxy_unit_test_log_filename";
    INFO("call function log_init");
    rc = log_init(level, name, &nci);
    CU_ASSERT(rc >= 0);
    if(rc >= 0){
        INFO("test case for function log_init success");
    } else{
        INFO("test case for function log_init **failed**");
        return;
    }
    INFO("function log_init cunit test case end\n");
}

static struct msg pmsg; // static variable used by test case 2 and 3

// cunit test case for function slow_log_hexdump
void proxy_slowlog_testcase2(void)
{
    int rc = 0;
    
    INFO("function slow_log_hexdump cunit test case start......");
    INFO("the command set for function slow_log_hexdump is: SET msg HelloWorld");
    INFO("in redis protocol, command 'SET msg HelloWorld' is *3\\r\\n$3\\r\\nSET\\r\\n$3\\r\\nmsg\\r\\n$10\\r\\nHelloWorld\\r\\n");
    INFO("start to create args for function slow_log_hexdump");
    INFO("malloc buf with 1024 bytes and clean it");
    char *buf = (char *)malloc(1024);
    memset(buf, 0, 1024);
    INFO("create struct msg pmsg");
    INFO("malloc psg.narg_start with 512 bytes");
    pmsg.narg_start = (uint8_t *)malloc(512);
    INFO("fill pmsg.narg_start with command 'SET msg HelloWorld' in redis protocol: *3\\r\\n$3\\r\\nSET\\r\\n$3\\r\\nmsg\\r\\n$10\\r\\nHelloWorld\\r\\n");
    pmsg.narg_start = (uint8_t *)"*3\r\n$3\r\nSET\r\n$3\r\nmsg\r\n$10\r\nHelloWorld\r\n";
    INFO("set pmsg.pos with the end of pmsg.narg_start string buffer");
    pmsg.pos = pmsg.narg_start + strlen("*3\r\n$3\r\nSET\r\n$3\r\nmsg\r\n$10\r\nHelloWorld\r\n");
    INFO("create args success");
    INFO("call function slow_log_hexdump(buf, &pmsg)");
    slow_log_hexdump(buf, &pmsg);
    INFO("expect output:");
    char *expect_output = "00000000  2a 33 0d 0a 24 33 0d 0a  53 45 54 0d 0a 24 33 0d   |*3..$3..SET..$3.|\n00000010  0a 6d 73 67 0d 0a 24 31  30 0d 0a 48 65 6c 6c 6f   |.msg..$10..Hello|\n00000020  57 6f 72 6c 64 0d 0a                               |World..|\n";
    printf("%s", expect_output);
    INFO("actual output:");
    printf("%s", buf);
    rc = strcmp(buf, expect_output);
    CU_ASSERT(rc != 0);
    if(rc != 0){
        INFO("unexpected output, test case for function slow_log_hexdump **failed**");
    } else {
        INFO("test case for function slow_log_hexdump success");
    }
    INFO("function slow_log_hexdump cunit test case end\n");
}

// cunit test case for function log_slow
void proxy_slowlog_testcase3(void)
{
    int rc = 0;

    INFO("function log_slow cunit test case start......");
    INFO("use struct msg pmsg created before");
    INFO("set pmsg dealing time to 30000us");
    INFO("call function log_slow(&pmsg, 30000, NULL, NULL)");
    struct context ctx;
    ctx.slow_cmd_count = 0;
    rc = log_slow(&pmsg, 30000, &ctx, NULL);
    CU_ASSERT(rc == 0);
    if(rc == 0){
        INFO("test case for function log_slow success");
    } else {
        INFO("test case for function log_slow **failed**");
    }
    INFO("function log_slow cunit test case end\n");

}

CU_TestInfo g_examplecases[] = 
{
    {"proxy_slowlog_testcase1", proxy_slowlog_testcase1},
    {"proxy_slowlog_testcase2", proxy_slowlog_testcase2},
    {"proxy_slowlog_testcase3", proxy_slowlog_testcase3},
    CU_TEST_INFO_NULL
};
