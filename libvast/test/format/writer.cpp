/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/detail/string.hpp"

#include "vast/format/ascii.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"

#define SUITE format
#include "vast/test/test.hpp"
#include "vast/test/fixtures/events.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(ascii_tests, fixtures::events)

namespace {

// clang-format off
auto last_zeek_http_log_line = R"__(<2009-11-19+07:17:28.829, "rydI6puScNa", 192.168.1.104, 1224/tcp, 87.106.66.233, 80/tcp, 1, "POST", "87.106.66.233", "/rpc.html?e=bl", nil, "SCSDK-6.0.0", 1064, 96, 200, "OK", 100, "Continue", nil, {}, nil, nil, nil, "application/octet-stream", nil, nil>)__";

auto first_csv_http_log_line = "type,ts,uid,id.orig_h,id.orig_p,id.resp_h,id.resp_p,trans_depth,method,host,uri,referrer,user_agent,request_body_len,response_body_len,status_code,status_msg,info_code,info_msg,filename,tags,username,password,proxied,mime_type,md5,extraction_file";

auto last_csv_http_log_line = R"__(zeek.http,2009-11-19+07:17:28.829,"rydI6puScNa",192.168.1.104,1224/tcp,87.106.66.233,80/tcp,1,"POST","87.106.66.233","/rpc.html?e=bl",,"SCSDK-6.0.0",1064,96,200,"OK",100,"Continue",,"",,,,"application/octet-stream",,)__";

auto first_ascii_bgpdump_txt_line = R"__(<2018-01-24+11:05:17.0, 27.111.229.79, 17639, "1", "3">)__";

auto first_json_bgpdump_txt_line = R"__({"timestamp": 1516791917, "source_ip": "27.111.229.79", "source_as": 17639, "old_state": "1", "new_state": "3"})__";

auto first_zeek_conn_log_line = R"__({"ts": 1258531221.486539, "uid": "Pii6cUUq1v4", "id.orig_h": "192.168.1.102", "id.orig_p": 68, "id.resp_h": "192.168.1.1", "id.resp_p": 67, "proto": "udp", "service": null, "duration": 0.16382, "orig_bytes": 301, "resp_bytes": 300, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 329, "resp_pkts": 1, "resp_ip_bytes": 328, "tunnel_parents": []})__";
// clang-format on

template <class Writer>
std::vector<std::string> generate(const std::vector<event>& xs) {
  std::string str;
  auto sb = new caf::containerbuf<std::string>{str};
  auto out = std::make_unique<std::ostream>(sb);
  Writer writer{std::move(out)};
  for (auto& e : xs)
    if (!writer.write(e))
      FAIL("failed to write event");
  writer.flush();
  REQUIRE(!str.empty());
  auto lines = detail::to_strings(detail::split(str, "\n"));
  REQUIRE(!lines.empty());
  return lines;
}

} // namespace <anonymous>

TEST(Zeek writer) {
  auto lines = generate<format::ascii::writer>(zeek_http_log);
  CHECK_EQUAL(lines.back(), last_zeek_http_log_line);
}

TEST(BGPdump writer) {
  auto lines = generate<format::ascii::writer>(bgpdump_txt);
  CHECK_EQUAL(lines.size(), 100u);
  CHECK_EQUAL(lines.front(), first_ascii_bgpdump_txt_line);
}

TEST(CSV writer) {
  auto lines = generate<format::csv::writer>(zeek_http_log);
  CHECK_EQUAL(lines.front(), first_csv_http_log_line);
  CHECK_EQUAL(lines.back(), last_csv_http_log_line);
}

TEST(JSON writer) {
  auto lines = generate<format::json::writer>(bgpdump_txt);
  CHECK_EQUAL(lines.front(), first_json_bgpdump_txt_line);
  lines = generate<format::json::writer>(zeek_conn_log);
  CHECK_EQUAL(lines.front(), first_zeek_conn_log_line);
}

FIXTURE_SCOPE_END()
