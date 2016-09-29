#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/detail/bro_parser_factory.hpp"

#define SUITE parseable
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

template <typename Attribute>
bool bro_parse(type const& t, std::string const& s, Attribute& attr) {
  return detail::make_bro_parser<std::string::const_iterator>(t)(s, attr);
}

} // namspace <anonymous>

TEST(bro data) {
  using namespace std::chrono;
  data d;
  CHECK(bro_parse(boolean_type{}, "T", d));
  CHECK(d == true);
  CHECK(bro_parse(integer_type{}, "-49329", d));
  CHECK(d == integer{-49329});
  CHECK(bro_parse(count_type{}, "49329"s, d));
  CHECK(d == count{49329});
  CHECK(bro_parse(timestamp_type{}, "1258594163.566694", d));
  auto i = duration_cast<interval>(double_seconds{1258594163.566694});
  CHECK(d == timestamp{i});
  CHECK(bro_parse(interval_type{}, "1258594163.566694", d));
  CHECK(d == i);
  CHECK(bro_parse(string_type{}, "\\x2afoo*"s, d));
  CHECK(d == "*foo*");
  CHECK(bro_parse(address_type{}, "192.168.1.103", d));
  CHECK(d == *to<address>("192.168.1.103"));
  CHECK(bro_parse(subnet_type{}, "10.0.0.0/24", d));
  CHECK(d == *to<subnet>("10.0.0.0/24"));
  CHECK(bro_parse(port_type{}, "49329", d));
  CHECK(d == port{49329, port::unknown});
  CHECK(bro_parse(vector_type{integer_type{}}, "49329", d));
  CHECK(d == vector{49329});
  CHECK(bro_parse(set_type{string_type{}}, "49329,42", d));
  CHECK(d == set{"49329", "42"});
}
