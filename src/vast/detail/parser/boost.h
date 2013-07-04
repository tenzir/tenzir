#ifndef VAST_DETAIL_PARSER_BOOST_H
#define VAST_DETAIL_PARSER_BOOST_H

#include "vast/config.h"

// Improves compile times significantly at the cost of having to manually
// predefine terminals.
#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

// Turns on to debug the parse process. All productions that should be debugged
// must occur in the BOOST_SPIRIT_DEBUG_NODES macro specified in the grammar
// constructor..
#undef BOOST_SPIRIT_QI_DEBUG

#ifdef VAST_CLANG
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix.hpp>
#include <boost/spirit/include/support_multi_pass.hpp>

#ifdef VAST_CLANG
#pragma clang diagnostic pop
#endif

#endif
