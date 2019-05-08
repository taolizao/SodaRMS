/***************************************************************************
 * 
 * Copyright (c) 2014 gmail.com, Inc. All Rights Reserved
 * 
 **************************************************************************/

/**
 * @file test_main.c
 * @author taolizao(taolizao@gmail.com)
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

#include "test_init.h"
#include "rtstub.h"

extern CU_TestInfo g_suitinitcases[];
extern CU_TestInfo g_suitendcases[];
extern CU_TestInfo g_examplecases[];
extern CU_TestInfo lua_testcases[];

CU_SuiteInfo g_suites[] = {
    {"suite_init", suite_success_init, NULL, g_suitinitcases},
    {"suite_example", NULL, NULL, g_examplecases},
    {"lua_testcase", NULL, NULL, lua_testcases},
    {"suite_clean", NULL, suite_success_clean, g_suitendcases},
    CU_SUITE_INFO_NULL
};

void AddTests(void)
{
    assert(NULL != CU_get_registry());
    assert(!CU_is_test_running());

    if(CUE_SUCCESS != CU_register_suites(g_suites))
    {
        exit(EXIT_FAILURE);
    }
}

int RunTest()
{
    if(CU_initialize_registry())
    {
        fprintf(stderr, " Initialization of Test Registry failed. ");
        exit(EXIT_FAILURE);
    }
    else
    {
        AddTests();

        /**** Automated Mode *****************/
        CU_set_output_filename("test_main");
        CU_list_tests_to_file();
        CU_automated_run_tests();
        /************************************/

        /***** Basice Mode *******************
        CU_basic_set_mode(CU_BRM_VERBOSE);
        CU_basic_run_tests();
        ************************************/

        /*****Console Mode ********************
        CU_console_run_tests();
        ************************************/

        CU_cleanup_registry();

        return CU_get_error();
    }
}

int main(int argc, char *argv[])
{
    return  RunTest();
}

/* testcase.c ends here */
