#include "vast/detail/string.hpp"
#include "vast/format/ascii.hpp"

#define SUITE format
#include "test.hpp"
#include "event_fixture.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(ascii_tests, event_fixture)

namespace {

auto last_http_log_line = R"__(bro::http [18446744073709551615|+1258617362396400896ns] [+1258617362396400896ns, "aRcY4DjxcQ5", [192.168.1.103, 1232/?, 87.106.12.47, 80/?], 1, "POST", "87.106.12.47", "/rpc.html?e=bl", nil, "SCSDK-6.0.0", 992, 96, 200, "OK", nil, nil, nil, {}, nil, nil, nil, "application/octet-stream", nil, nil])__";

} // namespace <anonymous>

TEST(ascii writer) {
  std::string str;
  auto sb = new caf::containerbuf<std::string>{str};
  auto out = std::make_unique<std::ostream>(sb);
  format::ascii::writer writer{std::move(out)};
  for (auto& e : bro_http_log)
    if (!writer.write(e))
      FAIL("failed to write event");
  writer.flush();
  REQUIRE(!str.empty());
  auto lines = detail::split_to_str(str, "\n"s);
  REQUIRE(!lines.empty());
  CHECK_EQUAL(lines.back(), last_http_log_line);
}

FIXTURE_SCOPE_END()
