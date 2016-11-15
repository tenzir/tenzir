#include "vast/detail/string.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/csv.hpp"

#define SUITE format
#include "test.hpp"
#include "event_fixture.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(ascii_tests, event_fixture)

namespace {

auto last_bro_http_log_line = R"__(bro::http [18446744073709551615|+1258617362396400896ns] [+1258617362396400896ns, "aRcY4DjxcQ5", [192.168.1.103, 1232/?, 87.106.12.47, 80/?], 1, "POST", "87.106.12.47", "/rpc.html?e=bl", nil, "SCSDK-6.0.0", 992, 96, 200, "OK", nil, nil, nil, {}, nil, nil, nil, "application/octet-stream", nil, nil])__";

auto first_csv_http_log_line = "type,id,timestamp,ts,uid,id.orig_h,id.orig_p,id.resp_h,id.resp_p,trans_depth,method,host,uri,referrer,user_agent,request_body_len,response_body_len,status_code,status_msg,info_code,info_msg,filename,tags,username,password,proxied,mime_type,md5,extraction_file";

auto last_csv_http_log_line = R"__(bro::http,18446744073709551615,1258617362396400896,+1258617362396400896ns,"aRcY4DjxcQ5",192.168.1.103,1232/?,87.106.12.47,80/?,1,"POST","87.106.12.47","/rpc.html?e=bl",,"SCSDK-6.0.0",992,96,200,"OK",,,,"",,,,"application/octet-stream",,)__";

template <class Writer>
std::vector<std::string> generate(std::vector<event> const& xs) {
  std::string str;
  auto sb = new caf::containerbuf<std::string>{str};
  auto out = std::make_unique<std::ostream>(sb);
  Writer writer{std::move(out)};
  for (auto& e : xs)
    if (!writer.write(e))
      FAIL("failed to write event");
  writer.flush();
  REQUIRE(!str.empty());
  auto lines = detail::split_to_str(str, "\n"s);
  REQUIRE(!lines.empty());
  return lines;
}

} // namespace <anonymous>

TEST(ascii writer) {
  auto lines = generate<format::ascii::writer>(bro_http_log);
  CHECK_EQUAL(lines.back(), last_bro_http_log_line);
}

TEST(csv writer) {
  auto lines = generate<format::csv::writer>(bro_http_log);
  CHECK_EQUAL(lines.front(), first_csv_http_log_line);
  CHECK_EQUAL(lines.back(), last_csv_http_log_line);
}

FIXTURE_SCOPE_END()
