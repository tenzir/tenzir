#ifndef TEST_UNIT_TEST_H
#define TEST_UNIT_TEST_H

#include "vast/config.h"

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
#endif

#include <boost/test/unit_test.hpp>

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

#endif
