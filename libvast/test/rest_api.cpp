//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>
#include <vast/system/node.hpp>
#include <vast/test/fixtures/node.hpp>
#include <vast/test/test.hpp>

#include <regex>
#include <simdjson.h>

namespace {

// "/query/{id}/next" -> "/query/:id/next"
std::string to_express_format(const std::string& openapi_path) {
  static const auto path_param = std::regex{"\\{(.+?)\\}"};
  return std::regex_replace(openapi_path, path_param, ":$1");
}

std::string method_to_string(const vast::http_method method) {
  std::string result;
  switch (method) {
    case vast::http_method::get:
      result = "get";
      break;
    case vast::http_method::post:
      result = "post";
      break;
  }
  return result;
}

struct fixture : public fixtures::node {
  fixture() : fixtures::node(VAST_PP_STRINGIFY(SUITE)) {
  }

  ~fixture() override = default;
};

class test_response final : public vast::http_response {
public:
  /// Append data to the response body.
  void append(std::string body) override {
    body_ += body;
  }

  /// Return an error and close the connection.
  //  TODO: Statically verify that we can only abort
  //  with the documented error codes.
  void abort(uint16_t error_code, std::string message) override {
    body_ = "";
    error_
      = caf::make_error(vast::ec::unspecified,
                        fmt::format("http error {}: {}", error_code, message));
  }

  std::string body_ = {};
  caf::error error_ = {};
};

} // namespace

TEST(OpenAPI specs) {
  auto version = vast::api_version::v0;
  for (auto const* rest_plugin :
       vast::plugins::get<vast::rest_endpoint_plugin>()) {
    MESSAGE("verifying spec for plugin " << rest_plugin->name());
    auto endpoints = rest_plugin->rest_endpoints();
    auto spec = rest_plugin->openapi_specification(version);
    REQUIRE(caf::holds_alternative<vast::record>(spec));
    auto spec_record = caf::get<vast::record>(spec);
    auto endpoint_methods = size_t{0ull};
    for (auto const& [key, value] : spec_record) {
      auto path = to_express_format(key);
      auto endpoints_it = std::find_if(endpoints.begin(), endpoints.end(),
                                       [&](auto const& endpoint) {
                                         return endpoint.path == path;
                                       });
      REQUIRE(endpoints_it != endpoints.end());
      auto& endpoint = *endpoints_it;
      CHECK_EQUAL(endpoint.version, version);
      REQUIRE(caf::holds_alternative<vast::record>(value));
      auto const& as_record = caf::get<vast::record>(value);
      auto method = method_to_string(endpoint.method);
      CHECK(as_record.contains(method));
      endpoint_methods += as_record.size();
      // TODO: Implement a a more convenient accessor API into `vast::data`,
      // so we can also check things like number of parameters and content type.
    }
    CHECK_EQUAL(endpoints.size(), endpoint_methods);
  }
}

FIXTURE_SCOPE(rest_api_tests, fixture)

TEST(status endpoint) {
  auto const* plugin
    = vast::plugins::find<vast::rest_endpoint_plugin>("api-status");
  REQUIRE(plugin);
  auto endpoints = plugin->rest_endpoints();
  REQUIRE_EQUAL(endpoints.size(), 1ull);
  auto const& status_endpoint = endpoints[0];
  auto handler = plugin->handler(self->system(), test_node);
  auto response = std::make_shared<test_response>();
  auto request = vast::http_request{
    .params = {{"component", "system"}},
    .response = response,
  };
  self->send(handler, vast::atom::http_request_v, status_endpoint.endpoint_id,
             std::move(request));
  run();
  CHECK_EQUAL(response->error_, caf::error{});
  CHECK(!response->body_.empty());
  auto padded_string = simdjson::padded_string{response->body_};
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  auto error = parser.parse(padded_string).get(doc);
  REQUIRE(!error);
}

TEST(export endpoint) {
  auto const* plugin
    = vast::plugins::find<vast::rest_endpoint_plugin>("api-export");
  REQUIRE(plugin);
  auto endpoints = plugin->rest_endpoints();
  REQUIRE_EQUAL(endpoints.size(), 3ull);
  { // GET /export
    auto const& export_endpoint = endpoints[0];
    auto handler = plugin->handler(self->system(), test_node);
    auto response = std::make_shared<test_response>();
    auto request = vast::http_request{
      .params = {
        {"expression", ":ip in 192.168.0.0/16"},
        {"limit", uint64_t{16}},
      },
      .response = response,
    };
    self->send(handler, vast::atom::http_request_v, export_endpoint.endpoint_id,
               std::move(request));
    run();
    CHECK_EQUAL(response->error_, caf::error{});
    CHECK(!response->body_.empty());
    auto padded_string = simdjson::padded_string{response->body_};
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    auto error = parser.parse(padded_string).get(doc);
    REQUIRE(!error);
    CHECK_EQUAL(int64_t{doc["num-events"]}, 16);
    CHECK_EQUAL(doc["events"].get_array().size(), 16ull);
  }
  //
  { // GET with unnormalized expression
    auto const& export_endpoint = endpoints[0];
    auto handler = plugin->handler(self->system(), test_node);
    auto response = std::make_shared<test_response>();
    auto request = vast::http_request{
      .params = {
        // The expression is syntactically valid but semantically
        // invalid (field_extractor in field_extractor)
        {"expression", "net.src.ip in asdf"},
        {"limit", uint64_t{16}},
      },
      .response = response,
    };
    self->send(handler, vast::atom::http_request_v, export_endpoint.endpoint_id,
               std::move(request));
    run();
    CHECK_NOT_EQUAL(response->error_, caf::error{});
    CHECK(
      to_string(response->error_).starts_with("unspecified(\"http error 400"));
  }
  { // POST /export
    auto const& export_endpoint = endpoints[1];
    auto handler = plugin->handler(self->system(), test_node);
    auto response = std::make_shared<test_response>();
    auto request = vast::http_request{
      .params = {
        {"expression", ":ip in 192.168.0.0/16"},
        {"limit", uint64_t{16}},
      },
      .response = response,
    };
    self->send(handler, vast::atom::http_request_v, export_endpoint.endpoint_id,
               std::move(request));
    run();
    CHECK_EQUAL(response->error_, caf::error{});
    CHECK(!response->body_.empty());
    auto padded_string = simdjson::padded_string{response->body_};
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    auto error = parser.parse(padded_string).get(doc);
    REQUIRE(!error);
    CHECK_EQUAL(int64_t{doc["num-events"]}, 16);
    CHECK_EQUAL(doc["events"].get_array().size(), 16ull);
  }
  { // POST /export/with-schema
    auto const& export_endpoint = endpoints[2];
    auto handler = plugin->handler(self->system(), test_node);
    auto response = std::make_shared<test_response>();
    auto request = vast::http_request{
      .params = {
        {"expression", ":ip in 192.168.0.0/16"},
        {"limit", uint64_t{16}},
      },
      .response = response,
    };
    self->send(handler, vast::atom::http_request_v, export_endpoint.endpoint_id,
               std::move(request));
    run();
    CHECK_EQUAL(response->error_, caf::error{});
    CHECK(!response->body_.empty());
    auto padded_string = simdjson::padded_string{response->body_};
    simdjson::dom::parser parser;
    simdjson::dom::element doc;
    auto error = parser.parse(padded_string).get(doc);
    REQUIRE(!error);
    CHECK_EQUAL(int64_t{doc["num-events"]}, 16);
    auto first = doc["events"].get_array().at(0);
    CHECK(!first.error());
    CHECK_EQUAL(std::string_view{first["name"]}, "zeek.conn");
    CHECK_EQUAL(first["data"].get_array().size(), 16ull);
  }
}

TEST(query endpoint) {
  auto const* plugin
    = vast::plugins::find<vast::rest_endpoint_plugin>("api-query");
  REQUIRE(plugin);
  auto endpoints = plugin->rest_endpoints();
  REQUIRE_EQUAL(endpoints.size(), 2ull);
  auto const& query_new_endpoint = endpoints[0];
  auto const& query_next_endpoint = endpoints[1];
  auto handler = plugin->handler(self->system(), test_node);
  auto response_new = std::make_shared<test_response>();
  auto request_new = vast::http_request{
    .params = {
      {"expression", "#type == \"zeek.conn\""},
    },
    .response = response_new,
  };
  self->send(handler, vast::atom::http_request_v,
             query_new_endpoint.endpoint_id, std::move(request_new));
  run();
  CHECK_EQUAL(response_new->error_, caf::error{});
  CHECK(!response_new->body_.empty());
  auto padded_string = simdjson::padded_string{response_new->body_};
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  auto error = parser.parse(padded_string).get(doc);
  CHECK(!error);
  CHECK(!doc.is_null());
  CHECK(!doc["id"].is_null());
  auto const* id = doc["id"].get<const char*>().value();
  auto response_next = std::make_shared<test_response>();
  const auto NUM_EVENTS = uint64_t{16};
  auto request_next = vast::http_request{
    .params = {
      {"id", std::string{id}},
      {"n", NUM_EVENTS},
    },
    .response = response_next,
  };
  self->send(handler, vast::atom::http_request_v,
             query_next_endpoint.endpoint_id, std::move(request_next));
  run();
  CHECK_EQUAL(response_next->error_, caf::error{});
  CHECK(!response_next->body_.empty());
  VAST_INFO("{}", response_next->body_);
  auto padded_string2 = simdjson::padded_string{response_next->body_};
  simdjson::dom::parser parser2;
  simdjson::dom::element doc2;
  auto error2 = parser2.parse(padded_string2).get(doc2);
  CHECK(!error2);
  CHECK(!doc2.is_null());
  CHECK(!doc2["events"].is_null());
}

FIXTURE_SCOPE_END()
