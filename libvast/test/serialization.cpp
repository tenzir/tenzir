#include <iomanip>

#include <caf/streambuf.hpp>

#include "vast/detail/coded_deserializer.hpp"
#include "vast/detail/coded_serializer.hpp"

#include "test.hpp"
#include "fixtures/events.hpp"

using namespace vast;
using namespace vast::detail;

FIXTURE_SCOPE(view_tests, fixtures::events)

TEST(coded serialization) {
  std::vector<char> buf;
  MESSAGE("establishing baseline");
  caf::stream_serializer<caf::vectorbuf> ss{nullptr, buf};
  ss << bro_conn_log;
  double baseline_size = buf.size();
  buf.clear();
  MESSAGE("serializing conn.log");
  coded_serializer<caf::vectorbuf> sink{nullptr, buf};
  sink << bro_conn_log;
  double coded_size = buf.size();
  MESSAGE("deserializing conn.log");
  coded_deserializer<caf::vectorbuf> source{nullptr, buf};
  std::vector<event> conn_log;
  source >> conn_log;
  CHECK_EQUAL(bro_conn_log, conn_log);
  auto ratio = coded_size / baseline_size;
  MESSAGE("coding/baseline ratio = " << std::setprecision(2) << ratio);
}

FIXTURE_SCOPE_END()
