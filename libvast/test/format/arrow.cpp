#include "vast/event.hpp"

#include "vast/format/arrow.hpp"

#define SUITE format
#include "test.hpp"
#include "fixtures/events.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(arrow_tests, fixtures::events)

TEST(arrow writer conn) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  for (auto& x : bro_conn_log)
    CHECK(writer.write(x));
  CHECK(writer.flush());
}
TEST(arrow writer http) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  for (auto& x : bro_http_log)
    CHECK(writer.write(x));
  CHECK(writer.flush());
}
TEST(arrow writer dns) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  for (auto& x : bro_dns_log)
    CHECK(writer.write(x));
  CHECK(writer.flush());
}
TEST(arrow writer random) {
  format::arrow::writer writer{"/tmp/plasma"};
  REQUIRE(writer.connected());
  for (auto& x : random)
    CHECK(writer.write(x));
  CHECK(writer.flush());
}

FIXTURE_SCOPE_END()
