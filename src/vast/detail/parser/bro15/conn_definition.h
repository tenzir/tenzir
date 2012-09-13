#ifndef VAST_DETAIL_PARSER_BRO15_CONN_DEFINITION_H
#define VAST_DETAIL_PARSER_BRO15_CONN_DEFINITION_H

#include "vast/detail/parser/bro15/conn.h"
#include "vast/logger.h"

namespace vast {
namespace detail {
namespace parser {
namespace bro15 {

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

struct stamper
{
  template <typename, typename>
  struct result
  {
    typedef void type;
  };

  void operator()(ze::event& e, double d) const
  {
    e.timestamp(ze::time_point(ze::time_range::fractional(d)));
  }
};

struct pusher
{
  template <typename, typename>
  struct result
  {
    typedef void type;
  };

  template <typename T>
  void operator()(ze::event& e, T const& x) const
  {
    e.push_back(x);
  }

  // All doubles are timestamps in Bro's 1.5 conn.log.
  void operator()(ze::event& e, double d) const
  {
    e.push_back(ze::time_point(ze::time_range::fractional(d)));
  }

  // This overload simply casts the attribute to a type that the constructor
  // of ze::value understands.
  void operator()(ze::event& e, unsigned long long x) const
  {
    e.push_back(static_cast<uint64_t>(x));
  }
};

struct port_maker
{
  template <typename, typename>
  struct result
  {
    typedef ze::port type;
  };

  ze::port operator()(uint16_t number, ze::string const& str) const
  {
    if (std::strncmp(str.data(), "tcp", str.size()) == 0)
      return {number, ze::port::tcp};
    if (std::strncmp(str.data(), "udp", str.size()) == 0)
      return {number, ze::port::udp};
    if (std::strncmp(str.data(), "icmp", str.size()) == 0)
      return {number, ze::port::icmp};

    return {number, ze::port::unknown};
  }
};

struct string_maker
{
  template <typename, typename>
  struct result
  {
    typedef ze::string type;
  };

  template <typename Iterator>
  ze::string operator()(Iterator begin, Iterator end) const
  {
    return {begin, end};
  }
};

template <typename Iterator>
connection<Iterator>::connection()
  : connection::base_type(conn)
{
  using boost::phoenix::begin;
  using boost::phoenix::end;
  using qi::on_error;
  using qi::fail;

  qi::_1_type _1;
  qi::_4_type _4;
  qi::_a_type _a;
  qi::_b_type _b;
  qi::_val_type _val;
  qi::lit_type lit;
  qi::raw_type raw;
  ascii::char_type chr;
  ascii::print_type printable;
  qi::ushort_type uint16;
  qi::ulong_long_type uint64;
  qi::real_parser<double, qi::strict_real_policies<double>> strict_double;

  boost::phoenix::function<stamper> stamp;
  boost::phoenix::function<pusher> push_back;
  boost::phoenix::function<port_maker> make_port;
  boost::phoenix::function<string_maker> make_string;

  conn
    =   strict_double       [stamp(_val, _1)]
    >   (   lit('?')        [push_back(_val, ze::nil)]
        |   strict_double   [push_back(_val, _1)]       // Duration
        )
    >   addr                [push_back(_val, _1)]       // Originator addr
    >   addr                [push_back(_val, _1)]       // Responder addr
    >   (   lit('?')        [push_back(_val, ze::nil)]  // Service
        |   id              [push_back(_val, _1)]
        )
    >   uint16              [_a = _1]                   // Originator port
    >   uint16              [_b = _1]                   // Responder port
    >   id                  [push_back(_val, make_port(_a, _1))]
                            [push_back(_val, make_port(_b, _1))]
                            [push_back(_val, _1)]       // Transport proto
    >   (   lit('?')        [push_back(_val, ze::nil)]  // Originator bytes
        |   uint64          [push_back(_val, _1)]       
        )
    >   (   lit('?')        [push_back(_val, ze::nil)]
        |   uint64          [push_back(_val, _1)]       // Responder bytes
        )
    >   id                  [push_back(_val, _1)]       // State
    >   (   chr('X')        [push_back(_val, _1)]
        |   chr('L')        [push_back(_val, _1)]
        )                                               // Flags
    >   -(  addl            [push_back(_val, _1)]       // Additional info
        )
    >   '\n'
    ;

  id
      =   raw[+(printable - ' ')]  [_val = make_string(begin(_1), end(_1))]
      ;

  addl
      =   raw[+(printable - '\n')] [_val = make_string(begin(_1), end(_1))]
      ;

  on_error<fail>(conn, boost::phoenix::function<error_handler>()(_4));

  conn.name("connection");
  id.name("identifier");
  addr.name("address");
}

} // namespace bro15
} // namespace parser
} // namespace detail
} // namespace vast

#endif
