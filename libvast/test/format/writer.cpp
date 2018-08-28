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
#include "test.hpp"
#include "fixtures/events.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(ascii_tests, fixtures::events)

namespace {

auto last_bro_http_log_line = R"__(bro::http [2009-11-19+07:17:28.829] [2009-11-19+07:17:28.829, "rydI6puScNa", [192.168.1.104, 1224/?, 87.106.66.233, 80/?], 1, "POST", "87.106.66.233", "/rpc.html?e=bl", nil, "SCSDK-6.0.0", 1064, 96, 200, "OK", 100, "Continue", nil, {}, nil, nil, nil, "application/octet-stream", nil, nil])__";

auto first_csv_http_log_line = "type,id,timestamp,ts,uid,id.orig_h,id.orig_p,id.resp_h,id.resp_p,trans_depth,method,host,uri,referrer,user_agent,request_body_len,response_body_len,status_code,status_msg,info_code,info_msg,filename,tags,username,password,proxied,mime_type,md5,extraction_file";

auto last_csv_http_log_line = R"__(bro::http,1239,1258615048829955072,2009-11-19+07:17:28.829,"rydI6puScNa",192.168.1.104,1224/?,87.106.66.233,80/?,1,"POST","87.106.66.233","/rpc.html?e=bl",,"SCSDK-6.0.0",1064,96,200,"OK",100,"Continue",,"",,,,"application/octet-stream",,)__";

auto first_ascii_bgpdump_txt_line = R"__(bgpdump::state_change [2018-01-24+11:05:17.0] [2018-01-24+11:05:17.0, 27.111.229.79, 17639, "1", "3"])__";

auto first_json_bgpdump_txt_line = R"__({"id": 1300, "timestamp": 1516791917000000000, "value": {"type": {"name": "bgpdump::state_change", "kind": "record", "structure": {"timestamp": {"name": "", "kind": "timestamp", "structure": null, "attributes": {}}, "source_ip": {"name": "", "kind": "address", "structure": null, "attributes": {}}, "source_as": {"name": "", "kind": "count", "structure": null, "attributes": {}}, "old_state": {"name": "", "kind": "string", "structure": null, "attributes": {}}, "new_state": {"name": "", "kind": "string", "structure": null, "attributes": {}}}, "attributes": {}}, "data": {"timestamp": 1516791917000000000, "source_ip": "27.111.229.79", "source_as": 17639, "old_state": "1", "new_state": "3"}}})__";

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

TEST(Bro writer) {
  auto lines = generate<format::ascii::writer>(bro_http_log);
  CHECK_EQUAL(lines.back(), last_bro_http_log_line);
}

TEST(BGPdump writer) {
  auto lines = generate<format::ascii::writer>(bgpdump_txt);
  CHECK_EQUAL(lines.size(), 100u);
  CHECK_EQUAL(lines.front(), first_ascii_bgpdump_txt_line);
}

TEST(CSV writer) {
  auto lines = generate<format::csv::writer>(bro_http_log);
  CHECK_EQUAL(lines.front(), first_csv_http_log_line);
  CHECK_EQUAL(lines.back(), last_csv_http_log_line);
}

TEST(JSON writer) {
  auto lines = generate<format::json::writer>(bgpdump_txt);
  CHECK_EQUAL(lines.front(), first_json_bgpdump_txt_line);
}

FIXTURE_SCOPE_END()
