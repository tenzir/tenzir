#ifndef TEST_UNIT_TEST_H
#define TEST_UNIT_TEST_H

// Boost Accumulators spits out quite a few warnings, which we'll disable here.
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
#endif

#include <boost/test/unit_test.hpp>

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#endif
