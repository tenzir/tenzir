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
  void
  abort(uint16_t error_code, std::string message, caf::error detail) override {
    body_ = "";
    error_ = caf::make_error(vast::ec::unspecified,
                             fmt::format("http error {}: {}{}", error_code,
                                         message, detail));
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

FIXTURE_SCOPE_END()

namespace {

struct query_fixture : public fixture {
  query_fixture() {
    auto const* plugin
      = vast::plugins::find<vast::rest_endpoint_plugin>("api-query");
    REQUIRE(plugin);
    auto endpoints = plugin->rest_endpoints();
    REQUIRE_EQUAL(endpoints.size(), 2ull);

    new_query_endpoint = endpoints[0];
    query_next_endpoint = endpoints[1];
    handler = plugin->handler(self->system(), test_node);
  }

  simdjson::dom::element parse(std::string response_body) {
    auto padded_string = simdjson::padded_string{response_body};
    simdjson::dom::element doc;
    // Parser must live as long as doc lives. Reusing the same parser seems to
    // not work. Just create a new parser and keep it in a std::list so it
    // doesn't get invalidated on emplace_back()
    auto& parser = parsers_.emplace_back();
    auto error = parser.parse(padded_string).get(doc);
    if (error)
      FAIL("failed to parse '" << response_body << "'");
    return doc;
  }

  auto send_new_query(std::string query) {
    auto response = std::make_shared<test_response>();
    auto request = vast::http_request{
      .params
      = {{"query", query}, {"flatten", true}, {"ttl", vast::duration::zero()}},
      .response = response};
    self->send(handler, vast::atom::http_request_v,
               new_query_endpoint.endpoint_id, std::move(request));
    run();
    CHECK_EQUAL(response->error_, caf::error{});
    return parse(response->body_);
  }

  auto send_query_next(std::string query_id, uint64_t events_count) {
    auto response = std::make_shared<test_response>();
    auto request = vast::http_request{.params = {{"id", std::string{query_id}},
                                                 {"n", events_count}},
                                      .response = response};
    self->send(handler, vast::atom::http_request_v,
               query_next_endpoint.endpoint_id, std::move(request));
    run();
    CHECK_EQUAL(response->error_, caf::error{});
    return parse(response->body_);
  }

  vast::rest_endpoint new_query_endpoint;
  vast::rest_endpoint query_next_endpoint;
  vast::system::rest_handler_actor handler;

private:
  std::list<simdjson::dom::parser> parsers_;
};

} // namespace

FIXTURE_SCOPE(query_rest_api_tests, query_fixture)

TEST(query endpoint) {
  auto response = send_new_query("where #type == \"zeek.conn\"");
  REQUIRE(!response.is_null());
  REQUIRE(!response["id"].is_null());
  auto const* id = response["id"].get<const char*>().value();
  constexpr auto NUM_EVENTS = uint64_t{16};
  auto next_response = send_query_next(id, NUM_EVENTS);
  REQUIRE(!next_response.is_null());
  REQUIRE(!next_response["events"].is_null());
  CHECK_EQUAL(next_response["events"].get_array().size(), NUM_EVENTS);
}

TEST(query endpoint outputs 0 events after all were already shipped) {
  auto response = send_new_query("where #type == \"zeek.conn\"");
  REQUIRE(!response.is_null());
  REQUIRE(!response["id"].is_null());
  auto const* id = response["id"].get<const char*>().value();
  auto next_response
    = send_query_next(id, std::numeric_limits<uint64_t>::max());
  REQUIRE(!next_response.is_null());
  REQUIRE(!next_response["events"].is_null());
  CHECK(next_response["events"].get_array().size() > 0);
  // After requesting uint64_t max events we expect to have 0 events.
  auto next_response2 = send_query_next(id, 1);
  REQUIRE(!next_response2.is_null());
  REQUIRE(!next_response2["events"].is_null());
  CHECK_EQUAL(next_response2["events"].get_array().size(), 0u);
}

TEST(query endpoint with pipeline) {
  auto response = send_new_query("where #type == \"zeek.conn\" | head 1");
  REQUIRE(!response.is_null());
  REQUIRE(!response["id"].is_null());
  auto const* id = response["id"].get<const char*>().value();
  auto next_response = send_query_next(id, 20);
  REQUIRE(!next_response.is_null());
  REQUIRE(!next_response["events"].is_null());
  CHECK_EQUAL(next_response["events"].get_array().size(), 1u);
}

TEST(query endpoint can handle head 0) {
  auto response = send_new_query("head 0");
  REQUIRE(!response.is_null());
  REQUIRE(!response["id"].is_null());
  auto const* id = response["id"].get<const char*>().value();
  auto next_response = send_query_next(id, 20);
  REQUIRE(!next_response.is_null());
  REQUIRE(!next_response["events"].is_null());
  CHECK_EQUAL(next_response["events"].get_array().size(), 0u);
  next_response = send_query_next(id, 20);
  REQUIRE(!next_response.is_null());
  REQUIRE(!next_response["events"].is_null());
  CHECK_EQUAL(next_response["events"].get_array().size(), 0u);
}

FIXTURE_SCOPE_END()
