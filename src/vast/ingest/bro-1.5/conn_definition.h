#ifndef VAST_INGEST_BRO_1_5_CONN_DEFINITION_H
#define VAST_INGEST_BRO_1_5_CONN_DEFINITION_H

#include "vast/ingest/bro-1.5/conn.h"
#include "vast/util/logger.h"

namespace vast {
namespace ingest {
namespace bro15 {
namespace parser {

struct error_handler
{
    template <typename>
    struct result
    {
        typedef void type;
    };

    template <typename Production>
    void operator()(Production const& production) const
    {
        LOG(error, ingest) << "parse error at production " << production;
    }
};

template <typename Iterator>
connection<Iterator>::connection()
  : connection::base_type(conn)
{
    using qi::on_error;
    using qi::fail;
    qi::_4_type _4;
    ascii::space_type space;
    ascii::char_type chr;
    ascii::print_type printable;
    qi::ushort_type uint16;
    qi::ulong_long_type uint64;
    qi::real_parser<double, qi::strict_real_policies<double>> strict_double;

    conn
        =   strict_double           // Timestamp
        >   ('?' | -strict_double)  // Duration
        >   addr                    // Originator address
        >   addr                    // Responder address
        >   ('?' | -id)             // Service
        >   uint16                  // Originator transport-layer port
        >   uint16                  // Responder transport-layer port
        >   id                      // Transport protocol
        >   ('?' | -uint64)         // Originator bytes
        >   ('?' | -uint64)         // Responder bytes
        >   id                      // State
        >   (chr('X') | chr('L'))   // Flags
        >   -addl                   // Additional Information
        ;

    id
        =   +(printable - space);
        ;

    addl
        =   +(printable - "\n");
        ;

    on_error<fail>(conn, boost::phoenix::function<error_handler>()(_4));

    conn.name("connection");
    id.name("identifier");
    addr.name("address");
}

} // namespace parser
} // namespace bro15
} // namespace ingest
} // namespace vast

#endif
