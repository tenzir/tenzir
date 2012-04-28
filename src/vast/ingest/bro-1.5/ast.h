#ifndef VAST_INGEST_BRO_1_5_AST_H
#define VAST_INGEST_BRO_1_5_AST_H

#include <vector>
#include <boost/fusion/include/adapt_struct.hpp>
#include <ze/type/address.h>

namespace vast {
namespace ingest {
namespace bro15 {
namespace ast {

/// A single connection log entry.
struct conn
{
    double timestamp;
    boost::optional<double> duration;
    ze::address orig_h;
    ze::address resp_h;
    boost::optional<std::string> service;
    uint16_t orig_p;
    uint16_t resp_p;
    std::string proto;
    boost::optional<uint64_t> orig_bytes;
    boost::optional<uint64_t> resp_bytes;
    std::string state;
    char flags;
    boost::optional<std::string> addl;
};

} // namespace parser
} // namespace bro15
} // namespace ingest
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::ingest::bro15::ast::conn,
    (double, timestamp)
    (boost::optional<double>, duration)
    (ze::address, orig_h)
    (ze::address, resp_h)
    (boost::optional<std::string>, service)
    (uint16_t, orig_p)
    (uint16_t, resp_p)
    (std::string, proto)
    (boost::optional<uint64_t>, orig_bytes)
    (boost::optional<uint64_t>, resp_bytes)
    (std::string, state)
    (char, flags)
    (boost::optional<std::string>, addl)
)

#endif

