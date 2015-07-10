#include "vast/detail/bro_parser_factory.h"

#define SUITE parseable
#include "test.h"

using namespace vast;
using namespace std::string_literals;

namespace {

template <typename Attribute>
bool bro_parse(type const& t, std::string const& s, Attribute& attr)
{
  return detail::make_bro_parser<std::string::const_iterator>(t)(s, attr);
}

} // namspace <anonymous>

TEST(bro data)
{
  data d;
  CHECK(bro_parse(type::boolean{}, "T", d));
  CHECK(d == true);
  CHECK(bro_parse(type::integer{}, "-49329", d));
  CHECK(d == integer{-49329});
  CHECK(bro_parse(type::count{}, "49329"s, d));
  CHECK(d == count{49329});
  CHECK(bro_parse(type::time_point{}, "1258594163.566694", d));
  CHECK(d == time::point{time::fractional(1258594163.566694)});
  CHECK(bro_parse(type::time_duration{}, "1258594163.566694", d));
  CHECK(d == time::fractional(1258594163.566694));
  CHECK(bro_parse(type::string{}, "\\x2afoo*"s, d));
  CHECK(d == "*foo*");
  CHECK(bro_parse(type::address{}, "192.168.1.103", d));
  CHECK(d == *to<address>("192.168.1.103"));
  CHECK(bro_parse(type::subnet{}, "10.0.0.0/24", d));
  CHECK(d == *to<subnet>("10.0.0.0/24"));
  CHECK(bro_parse(type::port{}, "49329", d));
  CHECK(d == port{49329, port::unknown});
  CHECK(bro_parse(type::vector{type::integer{}}, "49329", d));
  CHECK(d == vector{49329});
  CHECK(bro_parse(type::set{type::string{}}, "49329,42", d));
  CHECK(d == set{"49329", "42"});
}
