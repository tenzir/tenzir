#ifndef TEST_H
#define TEST_H

#ifdef SUITE
#define CAF_SUITE SUITE
#endif

#include <caf/test/unit_test.hpp>

// Logging
#define ERROR CAF_TEST_ERROR
#define INFO CAF_TEST_INFO
#define VERBOSE CAF_TEST_VERBOSE
#define MESSAGE CAF_TEST_VERBOSE

// Test setup
#define TEST CAF_TEST
#define FIXTURE_SCOPE CAF_TEST_FIXTURE_SCOPE
#define FIXTURE_SCOPE_END CAF_TEST_FIXTURE_SCOPE_END

// Checking
#define REQUIRE CAF_REQUIRE
#define CHECK CAF_CHECK
#define CHECK_FAIL CAF_CHECK_FAIL
#define FAIL CAF_FAIL

#endif
