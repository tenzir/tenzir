#include <iomanip>

#include "vast/packer.hpp"
#include "vast/unpacker.hpp"

#include "test.hpp"

#include "fixtures/events.hpp"

using namespace vast;

namespace {

struct fixture : fixtures::events {
  fixture() {
    std::transform(bro_conn_log.begin(), bro_conn_log.end(),
                   std::back_inserter(xs), [](auto& x) { return x.data(); });
  }

  vector xs;
};

} // namespace <anonymous>

FIXTURE_SCOPE(view_tests, fixture)

TEST(packing-unpacking) {
  // Serialize a vector of data.
  std::stringbuf sb;
  packer pkg{sb};
  for (auto& x : xs)
    pkg.pack(x);
  auto size = pkg.finish();
  auto ascii_size = 1'026'256.0; // bro-cut < conn.log | wc -c
  auto ratio = size / ascii_size;
  MESSAGE("packed/ASCII bytes ratio: " << std::setprecision(3) << ratio);
  // Selectively deserialize values.
  unpacker unpkg{sb};
  // Check first.
  auto x = unpkg.unpack<data>(0);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.front());
  // Check second.
  x = unpkg.unpack<data>(1);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[1]);
  // Check random.
  x = unpkg.unpack<data>(42);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[42]);
  // Check last.
  x = unpkg.unpack<data>(unpkg.size() - 1);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.back());
}

FIXTURE_SCOPE_END()
