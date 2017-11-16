#include "vast/event.hpp"

#include "vast/format/arrow.hpp"

#define SUITE format
#include "test.hpp"
#include "fixtures/events.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(bro_tests, fixtures::events)

TEST(arrow writer) {
  format::arrow::writer writer{"/tmp/plasma"};
  CHECK(!writer.connected());
}

FIXTURE_SCOPE_END()
