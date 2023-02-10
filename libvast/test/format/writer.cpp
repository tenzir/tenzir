//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/string.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

FIXTURE_SCOPE(ascii_tests, fixtures::events)

namespace {

// clang-format off
auto last_zeek_http_log_line = R"__(<2009-11-19T07:17:28.829955, "rydI6puScNa", 192.168.1.104, 1224, 87.106.66.233, 80, 1, "POST", "87.106.66.233", "/rpc.html?e=bl", nil, "SCSDK-6.0.0", 1064, 96, 200, "OK", 100, "Continue", nil, [], nil, nil, nil, "application/octet-stream", nil, nil>)__";

auto first_csv_http_log_line = "type,ts,uid,id.orig_h,id.orig_p,id.resp_h,id.resp_p,trans_depth,method,host,uri,referrer,user_agent,request_body_len,response_body_len,status_code,status_msg,info_code,info_msg,filename,tags,username,password,proxied,mime_type,md5,extraction_file";

auto last_csv_http_log_line = R"__(zeek.http,2009-11-19T07:17:28.829955,"rydI6puScNa",192.168.1.104,1224,87.106.66.233,80,1,"POST","87.106.66.233","/rpc.html?e=bl",,"SCSDK-6.0.0",1064,96,200,"OK",100,"Continue",,"",,,,"application/octet-stream",,)__";

auto first_zeek_conn_log_line = R"__({"ts": "2009-11-18T08:00:21.486539", "uid": "Pii6cUUq1v4", "id.orig_h": "192.168.1.102", "id.orig_p": 68, "id.resp_h": "192.168.1.1", "id.resp_p": 67, "proto": "udp", "service": null, "duration": "163.82ms", "orig_bytes": 301, "resp_bytes": 300, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 329, "resp_pkts": 1, "resp_ip_bytes": 328, "tunnel_parents": []})__";

auto last_zeek_http_log_line_json = R"__({"ts": "2009-11-19T07:17:28.829955", "uid": "rydI6puScNa", "id.orig_h": "192.168.1.104", "id.orig_p": 1224, "id.resp_h": "87.106.66.233", "id.resp_p": 80, "trans_depth": 1, "method": "POST", "host": "87.106.66.233", "uri": "/rpc.html?e=bl", "user_agent": "SCSDK-6.0.0", "request_body_len": 1064, "response_body_len": 96, "status_code": 200, "status_msg": "OK", "info_code": 100, "info_msg": "Continue", "tags": [], "mime_type": "application/octet-stream"})__";
// clang-format on

template <class Writer>
std::vector<std::string>
generate(const std::vector<table_slice>& xs, caf::settings options = {}) {
  auto out = std::make_unique<std::stringstream>();
  auto& stream = *out;
  Writer writer{std::move(out), options};
  for (const auto& x : xs)
    if (auto err = writer.write(x))
      FAIL("failed to write event");
  writer.flush();
  REQUIRE(!stream.str().empty());
  auto lines = detail::to_strings(detail::split(stream.str(), "\n"));
  REQUIRE(!lines.empty());
  return lines;
}

} // namespace

TEST(Zeek writer) {
  auto lines = generate<format::ascii::writer>(zeek_http_log);
  CHECK_EQUAL(lines.back(), last_zeek_http_log_line);
}

TEST(CSV writer) {
  auto lines = generate<format::csv::writer>(zeek_http_log);
  CHECK_EQUAL(lines.front(), first_csv_http_log_line);
  CHECK_EQUAL(lines.back(), last_csv_http_log_line);
}

TEST(JSON writer - defaults) {
  auto lines = generate<format::json::writer>(zeek_conn_log);
  CHECK_EQUAL(lines.front(), first_zeek_conn_log_line);
}

TEST(JSON writer - omit - nulls) {
  caf::settings options;
  caf::put(options, "vast.export.json.omit-nulls", true);
  auto lines = generate<format::json::writer>(zeek_http_log, options);
  CHECK_EQUAL(lines.back(), last_zeek_http_log_line_json);
}

FIXTURE_SCOPE_END()
