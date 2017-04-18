#include <iomanip>

#include "vast/layout.hpp"

#include "vast/detail/mmapbuf.hpp"

#define SUITE layout
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

FIXTURE_SCOPE(layout_tests, fixture)

TEST(writing and reading) {
  // Serialize a vector of data.
  std::stringbuf sb;
  layout::writer writer{sb};
  for (auto& x : xs)
    CHECK(writer.write(x));
  auto size = writer.finish();
  auto ascii_size = 1'026'256.0; // bro-cut < conn.log | wc -c
  auto ratio = size / ascii_size;
  MESSAGE("packed/ASCII bytes ratio: " << std::setprecision(3) << ratio);
  // Selectively deserialize values.
  layout::reader reader{sb};
  // Check first.
  auto x = reader.read<data>(0);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.front());
  // Check second.
  x = reader.read<data>(1);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[1]);
  // Check random.
  x = reader.read<data>(42);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[42]);
  // Check last.
  x = reader.read<data>(reader.size() - 1);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.back());
}

TEST(reading while writing) {
  std::stringbuf sb;
  MESSAGE("writing");
  layout::writer writer{sb};
  for (auto i = 0u; i < 10; ++i)
    CHECK(writer.write(xs[i]));
  CHECK_EQUAL(writer.size(), 10u);
  auto x = writer.read<data>(0);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs.front());
  x = writer.read<data>(7);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[7]);
  for (auto i = 10u; i < 20; ++i)
    CHECK(writer.write(xs[i]));
  CHECK_EQUAL(writer.size(), 20u);
  x = writer.read<data>(15);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[15]);
  writer.finish();
  MESSAGE("reading");
  layout::reader reader{sb};
  REQUIRE_EQUAL(reader.size(), 20u);
  for (auto i = 0u; i < reader.size(); ++i) {
    x = reader.read<data>(i);
    REQUIRE(x);
    CHECK_EQUAL(*x, xs[i]);
  }
}

TEST(viewing) {
  MESSAGE("writing");
  detail::mmapbuf sb{2 << 20}; // 2 MB anon map
  layout::writer writer{sb};
  for (auto& x : xs)
    CHECK(writer.write(x));
  auto size = writer.finish();
  REQUIRE(size < sb.size());
  REQUIRE(sb.truncate(size));
  MESSAGE("viewer access");
  auto view = layout::viewer{sb.release()};
  CHECK_EQUAL(view.size(), xs.size());
  CHECK_EQUAL(view[0] - view.chunk()->begin(), 0);
  auto x = view.unpack<data>(42);
  REQUIRE(x);
  CHECK_EQUAL(*x, xs[42]);
}

FIXTURE_SCOPE_END()
