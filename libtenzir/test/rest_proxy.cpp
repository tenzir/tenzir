//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/node.hpp>
#include <tenzir/test/fixtures/node.hpp>
#include <tenzir/test/test.hpp>

namespace {

struct fixture : public fixtures::node {
  fixture() : fixtures::node(TENZIR_PP_STRINGIFY(SUITE)) {
  }

  ~fixture() override = default;
};

} // namespace

TEST(parameter parsing) {
  auto endpoint = tenzir::rest_endpoint {
    .method = tenzir::http_method::post,
    .path = "/dummy",
    .params = tenzir::record_type{
      {"id", tenzir::int64_type{}},
      {"uid", tenzir::uint64_type{}},
      {"timeout", tenzir::duration_type{}},
      {"value", tenzir::string_type{}},
      {"li", tenzir::list_type{tenzir::ip_type{}}},
      {"ls", tenzir::list_type{tenzir::string_type{}}},
    },
    .version = tenzir::api_version::v0,
    .content_type = tenzir::http_content_type::json,
  };
  auto params = tenzir::detail::stable_map<std::string, tenzir::data>{
    {"id", std::string{"+0"}},
    {"uid", std::string{"0"}},
    {"timeout", std::string{"1m"}},
    {"value", std::string{"1"}},
    {"li", tenzir::list{std::string{"12.34.1.2"}}},
    {"ls", tenzir::list{std::string{"1"}, std::string{"2"}}},
  };
  tenzir::http_parameter_map pmap;
  pmap.get_unsafe() = params;
  auto result = parse_endpoint_parameters(endpoint, pmap);
  REQUIRE_NOERROR(result);
}

FIXTURE_SCOPE(rest_api_tests, fixture)

TEST(proxy requests) {
  // /status
  auto desc = tenzir::http_request_description{
    .canonical_path = "POST /status (v0)",
    .json_body
    = R"_({"verbosity": "detailed", "components": ["catalog", "index"]})_",
  };
  auto rp
    = self->request(test_node, caf::infinite, tenzir::atom::proxy_v, desc);
  run();
  rp.receive(
    [](tenzir::rest_response& response) {
      CHECK_EQUAL(response.code(), size_t{200});
      auto body = std::move(response).release();
      auto parsed = tenzir::from_json(body);
      CHECK(parsed);
      CHECK(caf::holds_alternative<tenzir::record>(*parsed));
      CHECK(caf::get<tenzir::record>(*parsed).contains("catalog"));
      CHECK(caf::get<tenzir::record>(*parsed).contains("index"));
    },
    [](const caf::error& e) {
      FAIL(e);
    });

  // TODO: Enable this test once the node test fixture includes a
  // "serve-manager".
  // auto desc2 = tenzir::http_request_description{
  //   .canonical_path = "POST /serve (v0)",
  //   .json_body = R"_({"serve_id": "foo", "max_events", "1"})_",
  //   },
  // };
  // auto rp2 = self->request(test_node, caf::infinite, tenzir::atom::proxy_v,
  //                          desc2);
  // run();
  // rp2.receive(
  //   [](tenzir::rest_response& response) {
  //     // We expect an "Unknown serve id" error.
  //     CHECK_EQUAL(response.code(), size_t{400});
  //     auto body = std::move(response).release();
  //     auto parsed = tenzir::from_json(body);
  //     CHECK(parsed);
  //     CHECK(caf::holds_alternative<tenzir::record>(*parsed));
  //     CHECK(caf::get<tenzir::record>(*parsed).contains("error"));
  //   },
  //   [](const caf::error& e) {
  //     FAIL(e);
  //   });
}

TEST(invalid request) {
  MESSAGE("invalid path");
  auto desc = tenzir::http_request_description{
    .canonical_path = "foo",
    .json_body = {},
  };
  auto rp
    = self->request(test_node, caf::infinite, tenzir::atom::proxy_v, desc);
  run();
  rp.receive(
    [](tenzir::rest_response& response) {
      CHECK(response.is_error());
    },
    [](const caf::error& e) {
      FAIL(e);
    });

  MESSAGE("invalid params");
  auto desc2 = tenzir::http_request_description{
    .canonical_path = "POST /status (v0)",
    .json_body = R"_({"verbosity": "jklo"})_",
  };
  auto rp2
    = self->request(test_node, caf::infinite, tenzir::atom::proxy_v, desc2);
  run();
  rp2.receive(
    [](tenzir::rest_response& response) {
      CHECK(response.is_error());
    },
    [](const caf::error& e) {
      FAIL(e);
    });
}

FIXTURE_SCOPE_END()
