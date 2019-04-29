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

#define SUITE format

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/event.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

auto http = record_type{{"ts", vast::timestamp_type{}},
                        {"uid", vast::string_type{}},
                        {"id.orig_h", vast::address_type{}},
                        {"id.orig_p", vast::port_type{}},
                        {"id.resp_h", vast::address_type{}},
                        {"id.resp_p", vast::port_type{}},
                        {"trans_depth", vast::count_type{}},
                        {"method", vast::string_type{}},
                        {"host", vast::string_type{}},
                        {"uri", vast::string_type{}},
                        {"version", vast::string_type{}},
                        {"user_agent", vast::string_type{}},
                        {"request_body_len", vast::count_type{}},
                        {"response_body_len", vast::count_type{}},
                        {"status_code", vast::count_type{}},
                        {"status_msg", vast::string_type{}},
                        {"tags", vast::vector_type{vast::string_type{}}},
                        {"resp_fuids", vast::vector_type{vast::string_type{}}}}
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

} // namespace

TEST(json reader) {
  using reader_type = format::json::reader<format::json::default_selector>;
  reader_type reader{defaults::system::table_slice_type,
                     std::make_unique<std::istringstream>(
                       std::string{http_log})};
  vast::schema s;
  REQUIRE(s.add(http));
  reader.schema(s);
  std::vector<table_slice_ptr> slices;
  auto add_slice = [&](table_slice_ptr ptr) {
    slices.emplace_back(std::move(ptr));
  };
  auto [err, num] = reader.read(9, 5, add_slice);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(num, 9);
}
