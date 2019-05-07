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

#include "vast/format/json.hpp"
#include "vast/format/json/suricata.hpp"

#define SUITE format

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/json.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/default_table_slice_builder.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

auto http = record_type{{"ts", timestamp_type{}},
                        {"uid", string_type{}},
                        {"id.orig_h", address_type{}},
                        {"id.orig_p", port_type{}},
                        {"id.resp_h", address_type{}},
                        {"id.resp_p", port_type{}},
                        {"trans_depth", count_type{}},
                        {"method", string_type{}},
                        {"host", string_type{}},
                        {"uri", string_type{}},
                        {"version", string_type{}},
                        {"user_agent", string_type{}},
                        {"request_body_len", count_type{}},
                        {"response_body_len", count_type{}},
                        {"status_code", count_type{}},
                        {"status_msg", string_type{}},
                        {"tags", vector_type{string_type{}}},
                        {"resp_fuids", vector_type{string_type{}}},
                        {"resp_mime_types", vector_type{string_type{}}}}
              .name("http");

std::string_view http_log
  = R"__({"ts":"2011-08-12T13:00:36.349948Z","uid":"CBcjvm3neo85kOzgnh","id.orig_h":"147.32.84.165","id.orig_p":1027,"id.resp_h":"74.125.232.202","id.resp_p":80,"trans_depth":1,"method":"GET","host":"cr-tools.clients.google.com","uri":"/service/check2?appid={430FD4D0-B729-4F61-AA34-91526481799D}\u0026appversion=1.3.21.65\u0026applang=\u0026machine=0\u0026version=1.3.21.65\u0026osversion=5.1\u0026servicepack=Service Pack 2","version":"1.1","user_agent":"Google Update/1.3.21.65;winhttp","request_body_len":0,"response_body_len":0,"status_code":204,"status_msg":"No Content","tags":[]}
{"ts":"2011-08-12T13:05:47.628001Z","uid":"CF5gKc16Phk3bf86Sb","id.orig_h":"147.32.84.165","id.orig_p":3101,"id.resp_h":"195.113.232.73","id.resp_p":80,"trans_depth":1,"method":"GET","host":"javadl-esd.sun.com","uri":"/update/1.6.0/map-1.6.0.xml","user_agent":"jupdate","request_body_len":0,"response_body_len":0,"tags":[]}
{"ts":"2011-08-12T13:08:01.360925Z","uid":"C9N7yE3y8Ym1QjMoHh","id.orig_h":"147.32.84.165","id.orig_p":1029,"id.resp_h":"74.125.232.193","id.resp_p":80,"trans_depth":1,"method":"GET","host":"cr-tools.clients.google.com","uri":"/service/check2?appid={430FD4D0-B729-4F61-AA34-91526481799D}\u0026appversion=1.3.21.65\u0026applang=\u0026machine=0\u0026version=1.3.21.65\u0026osversion=5.1\u0026servicepack=Service Pack 2","version":"1.1","user_agent":"Google Update/1.3.21.65;winhttp","request_body_len":0,"response_body_len":0,"status_code":204,"status_msg":"No Content","tags":[]}
{"ts":"2011-08-12T13:09:35.498887Z","uid":"CQAKf71wEMPlOXX1a7","id.orig_h":"147.32.84.165","id.orig_p":1029,"id.resp_h":"74.125.232.200","id.resp_p":80,"trans_depth":1,"method":"GET","host":"cr-tools.clients.google.com","uri":"/service/check2?appid={430FD4D0-B729-4F61-AA34-91526481799D}\u0026appversion=1.3.21.65\u0026applang=\u0026machine=0\u0026version=1.3.21.65\u0026osversion=5.1\u0026servicepack=Service Pack 2","version":"1.1","user_agent":"Google Update/1.3.21.65;winhttp","request_body_len":0,"response_body_len":0,"status_code":204,"status_msg":"No Content","tags":[]}
{"ts":"2011-08-12T13:14:36.012344Z","uid":"CBnMUD2ZYU0sqVxFjc","id.orig_h":"147.32.84.165","id.orig_p":1041,"id.resp_h":"137.254.16.78","id.resp_p":80,"trans_depth":1,"method":"GET","host":"dl.javafx.com","uri":"/javafx-cache.jnlp","version":"1.1","user_agent":"JNLP/6.0 javaws/1.6.0_26 (b03) Java/1.6.0_26","request_body_len":0,"response_body_len":0,"status_code":304,"status_msg":"Not Modified","tags":[]}
{"ts":"2011-08-12T14:59:11.994970Z","uid":"CvfbWf1i6CVMOig5Tc","id.orig_h":"147.32.84.165","id.orig_p":1046,"id.resp_h":"74.207.254.18","id.resp_p":80,"trans_depth":1,"method":"GET","host":"www.nmap.org","uri":"/","version":"1.1","user_agent":"Mozilla/4.0 (compatible)","request_body_len":0,"response_body_len":301,"status_code":301,"status_msg":"Moved Permanently","tags":[],"resp_fuids":["Fy9oAx1jLDirZfhaVf"],"resp_mime_types":["text/html"]}
{"ts":"2011-08-12T14:59:12.448311Z","uid":"CVnbsh4O1Ur04Fzptd","id.orig_h":"147.32.84.165","id.orig_p":1047,"id.resp_h":"74.207.254.18","id.resp_p":80,"trans_depth":1,"method":"GET","host":"nmap.org","uri":"/","user_agent":"Mozilla/4.0 (compatible)","request_body_len":0,"response_body_len":0,"tags":[]}
{"ts":"2011-08-13T13:04:24.640406Z","uid":"CHwIy2itrn680kzT4","id.orig_h":"147.32.84.165","id.orig_p":1089,"id.resp_h":"95.100.248.24","id.resp_p":80,"trans_depth":1,"method":"GET","host":"crl.microsoft.com","uri":"/pki/crl/products/CodeSignPCA.crl","version":"1.1","user_agent":"Microsoft-CryptoAPI/5.131.2600.2180","request_body_len":0,"response_body_len":558,"status_code":200,"status_msg":"OK","tags":[],"resp_fuids":["Fo3McC1Z0zG90ximd6"]}
{"ts":"2011-08-14T20:37:05.912842Z","uid":"CyOGV53NZM39U6MpVh","id.orig_h":"147.32.84.165","id.orig_p":1391,"id.resp_h":"137.254.16.78","id.resp_p":80,"trans_depth":1,"method":"GET","host":"dl.javafx.com","uri":"/javafx-cache.jnlp","version":"1.1","user_agent":"JNLP/6.0 javaws/1.6.0_26 (b03) Java/1.6.0_26","request_body_len":0,"response_body_len":0,"status_code":304,"status_msg":"Not Modified","tags":[]})__";

std::string_view eve_log
  = R"json({"timestamp":"2011-08-12T14:52:57.716360+0200","flow_id":1031464864740687,"pcap_cnt":83,"event_type":"alert","src_ip":"147.32.84.165","src_port":1181,"dest_ip":"78.40.125.4","dest_port":6667,"proto":"TCP","alert":{"action":"allowed","gid":1,"signature_id":2017318,"rev":4,"signature":"ET CURRENT_EVENTS SUSPICIOUS IRC - PRIVMSG *.(exe|tar|tgz|zip)  download command","category":"Potentially Bad Traffic","severity":2},"flow":{"pkts_toserver":27,"pkts_toclient":35,"bytes_toserver":2302,"bytes_toclient":4520,"start":"2011-08-12T14:47:24.357711+0200"},"payload":"UFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","payload_printable":"PRIVMSG #zarasa48 : smss.exe (368)\r\n","stream":0,"packet":"AB5J2xnDCAAntbcZCABFAABMGV5AAIAGLlyTIFSlTih9BASdGgvw0QvAxUWHdVAY+rCL4gAAUFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","packet_info":{"linktype":1}}
  {"timestamp":"2011-08-12T14:52:57.716360+0200","flow_id":1031464864740687,"pcap_cnt":83,"event_type":"alert","src_ip":"147.32.84.165","src_port":1181,"dest_ip":"78.40.125.4","dest_port":6667,"proto":"TCP","alert":{"action":"allowed","gid":1,"signature_id":2017318,"rev":4,"signature":"ET CURRENT_EVENTS SUSPICIOUS IRC - PRIVMSG *.(exe|tar|tgz|zip)  download command","category":"Potentially Bad Traffic","severity":2},"flow":{"pkts_toserver":27,"pkts_toclient":35,"bytes_toserver":2302,"bytes_toclient":4520,"start":"2011-08-12T14:47:24.357711+0200"},"payload":"UFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","payload_printable":"PRIVMSG #zarasa48 : smss.exe (368)\r\n","stream":0,"packet":"AB5J2xnDCAAntbcZCABFAABMGV5AAIAGLlyTIFSlTih9BASdGgvw0QvAxUWHdVAY+rCL4gAAUFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","packet_info":{"linktype":1}})json";

} // namespace

FIXTURE_SCOPE(zeek_reader_tests, fixtures::deterministic_actor_system)

TEST(json to data) {
  auto layout = record_type{{"b", boolean_type{}},
                            {"c", count_type{}},
                            {"r", real_type{}},
                            {"i", integer_type{}},
                            {"s", string_type{}},
                            {"a", address_type{}},
                            {"p", port_type{}},
                            {"sn", subnet_type{}},
                            {"t", timestamp_type{}},
                            {"d", timespan_type{}},
                            {"d2", timespan_type{}},
                            {"e", enumeration_type{{"FOO", "BAR", "BAZ"}}},
                            {"sc", set_type{count_type{}}},
                            {"vp", vector_type{port_type{}}},
                            {"vt", vector_type{timestamp_type{}}},
                            {"rec", record_type{{"c", count_type{}},
                                                {"s", string_type{}}}},
                            {"msa", map_type{string_type{}, address_type{}}},
                            {"mcs", map_type{count_type{}, string_type{}}}}
                  .name("layout");
  auto flat = flatten(layout);
  auto builder = default_table_slice_builder{flat};
  std::string_view str = R"json({
    "b": true,
    "c": 424242,
    "r": 4.2,
    "i": -1337,
    "a": "147.32.84.165",
    "p": "42/udp",
    "sn": "192.168.0.1/24",
    "t": "2011-08-12+14:59:11.994970",
    "d": "42s",
    "d2": 3.006088,
    "e": "BAZ",
    "sc": [ 44, 42, 43 ],
    "vp": [ 19, "5555/tcp", 0, "0/icmp" ],
    "vt": [ 1556624773, "2019-04-30T11:46:13Z" ],
    "rec": { "c": 421, "s":"test" },
    "msa": { "foo": "1.2.3.4", "bar": "2001:db8::" },
    "mcs": { "1": "FOO", "1024": "BAR!" }
  })json";
  auto jn = unbox(to<json>(str));
  auto xs = caf::get<json::object>(jn);
  format::json::add(builder, xs, flat, "json-test");
  auto ptr = builder.finish();
  REQUIRE(ptr);
  CHECK(ptr->at(0, 10) == enumeration{2});
  auto reference = map{};
  reference[1] = "FOO";
  reference[1024] = "BAR!";
  CHECK(ptr->at(0, 16) == reference);
}

TEST(json reader) {
  using reader_type = format::json::reader<format::json::default_selector>;
  reader_type reader{defaults::system::table_slice_type,
                     std::make_unique<std::istringstream>(
                       std::string{http_log})};
  schema s;
  REQUIRE(s.add(http));
  reader.schema(s);
  std::vector<table_slice_ptr> slices;
  auto add_slice = [&](table_slice_ptr ptr) {
    slices.emplace_back(std::move(ptr));
  };
  auto [err, num] = reader.read(9, 5, add_slice);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(num, 9);
  CHECK(slices[1]->at(0, 0)
        == data{unbox(to<timestamp>("2011-08-12T14:59:11.994970Z"))});
  CHECK(slices[1]->at(0, 18) == vector{data{"text/html"}});
}

TEST(suricata) {
  using reader_type = format::json::reader<format::json::suricata>;
  reader_type reader{defaults::system::table_slice_type,
                     std::make_unique<std::istringstream>(
                       std::string{eve_log})};
  std::vector<table_slice_ptr> slices;
  auto add_slice = [&](table_slice_ptr ptr) {
    slices.emplace_back(std::move(ptr));
  };
  auto [err, num] = reader.read(2, 5, add_slice);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(num, 2);
  CHECK_EQUAL(slices[0]->rows(), 2);
  CHECK_EQUAL(slices[0]->columns(), 35);
  CHECK(slices[0]->at(0, 19) == count{4520});
}

FIXTURE_SCOPE_END()
