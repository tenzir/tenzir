#ifndef VAST_DETAIL_PARSER_BRO15_CONN_H
#define VAST_DETAIL_PARSER_BRO15_CONN_H

// This grammar has more struct fields than the default can handle.
#define FUSION_MAX_VECTOR_SIZE 20

#include "vast/event.h"
#include "vast/detail/parser/address.h"

namespace vast {
namespace detail {
namespace parser {
namespace bro15 {

namespace qi = boost::spirit::qi;
namespace ascii = qi::ascii;

struct error_handler
{
  template <typename>
  struct result
  {
    typedef void type;
  };

  error_handler(std::string& error)
    : error{error}
  {
  }

  template <typename Production>
  void operator()(Production const& production) const
  {
    std::stringstream ss;
    ss << "parse error at production " << production;
    error = ss.str();
  }

  std::string& error;
};

struct stamper
{
  template <typename, typename>
  struct result
  {
    typedef void type;
  };

  void operator()(event& e, double d) const
  {
    e.timestamp(time_point(time_range::fractional(d)));
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
  void operator()(event& e, T const& x) const
  {
    e.push_back(x);
  }

  void operator()(event& e, value v) const
  {
    e.push_back(std::move(v));
  }

  // All doubles are timestamps in Bro's 1.5 conn.log.
  void operator()(event& e, double d) const
  {
    e.push_back(time_point(time_range::fractional(d)));
  }

  // This overload simply casts the attribute to a type that the constructor
  // of value understands.
  void operator()(event& e, unsigned long long x) const
  {
    e.push_back(static_cast<uint64_t>(x));
  }
};

struct port_maker
{
  template <typename, typename>
  struct result
  {
    typedef port type;
  };

  port operator()(uint16_t number, string const& str) const
  {
    if (std::strncmp(str.data(), "tcp", str.size()) == 0)
      return {number, port::tcp};
    if (std::strncmp(str.data(), "udp", str.size()) == 0)
      return {number, port::udp};
    if (std::strncmp(str.data(), "icmp", str.size()) == 0)
      return {number, port::icmp};

    return {number, port::unknown};
  }
};

struct string_maker
{
  template <typename, typename>
  struct result
  {
    typedef string type;
  };

  template <typename Iterator>
  string operator()(Iterator begin, Iterator end) const
  {
    return {begin, end};
  }
};

struct empty_maker
{
  template <typename>
  struct result
  {
    typedef value type;
  };

  value operator()(value_type t) const
  {
    return value(t);
  }
};

template <typename Iterator>
struct skipper : qi::grammar<Iterator>
{
  skipper()
    : skipper::base_type(start)
  {
    ascii::space_type space;

    start 
      = space - '\n'    // Tab & space
      ;
  };

  qi::rule<Iterator> start;
};

template <typename Iterator>
struct connection : qi::grammar<Iterator, event(), qi::locals<uint16_t, uint16_t>, skipper<Iterator>>
{
  connection(std::string& error)
    : connection::base_type{conn}, error{error}
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
    boost::phoenix::function<empty_maker> make_empty;

    conn
      =   strict_double       [stamp(_val, _1)]
      >   (   lit('?')        [push_back(_val, make_empty(time_range_value))]
          |   strict_double   [push_back(_val, _1)]       // Duration
          )
      >   addr                [push_back(_val, _1)]       // Originator addr
      >   addr                [push_back(_val, _1)]       // Responder addr
      >   (   lit('?')        [push_back(_val, make_empty(string_value))]
          |   id              [push_back(_val, _1)]       // Service
          )
      >   uint16              [_a = _1]                   // Originator port
      >   uint16              [_b = _1]                   // Responder port
      >   id                  [push_back(_val, make_port(_a, _1))]
                              [push_back(_val, make_port(_b, _1))]
                              [push_back(_val, _1)]       // Transport proto
      >   (   lit('?')        [push_back(_val, make_empty(uint_value))]
          |   uint64          [push_back(_val, _1)]       // Originator bytes
          )
      >   (   lit('?')        [push_back(_val, make_empty(uint_value))]
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

    on_error<fail>(conn, boost::phoenix::function<error_handler>(error)(_4));

    conn.name("connection");
    id.name("identifier");
    addr.name("address");
  }

  qi::rule<Iterator, event(), qi::locals<uint16_t, uint16_t>, skipper<Iterator>> conn;
  qi::rule<Iterator, string()> id;
  qi::rule<Iterator, string()> addl;

  parser::address<Iterator> addr;

  std::string& error;
};

} // namespace bro15
} // namespace parser
} // namespace detail
} // namespace vast

#endif
