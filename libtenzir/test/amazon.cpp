//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/amazon.hpp"

#include "tenzir/detail/env.hpp"
#include "tenzir/test/test.hpp"

#include <aws/core/Aws.h>

#include <algorithm>
#include <optional>
#include <ranges>
#include <string>
#include <vector>

using namespace tenzir;

namespace {

struct env_fixture {
  env_fixture() {
    Aws::InitAPI(aws_options);
  }

  auto set(std::string_view key, std::string_view value) -> void {
    remember(key);
    REQUIRE_EQUAL(detail::setenv(key, value), caf::none);
  }

  auto unset(std::string_view key) -> void {
    remember(key);
    REQUIRE_EQUAL(detail::unsetenv(key), caf::none);
  }

  ~env_fixture() {
    for (auto const& [key, value] : previous) {
      auto error = value ? detail::setenv(key, *value) : detail::unsetenv(key);
      TENZIR_ASSERT(error == caf::none);
    }
    Aws::ShutdownAPI(aws_options);
  }

  auto remember(std::string_view key) -> void {
    auto name = std::string{key};
    if (std::ranges::any_of(previous, [&](auto const& entry) {
          return entry.first == name;
        })) {
      return;
    }
    previous.emplace_back(std::move(name), detail::getenv(key));
  }

  std::vector<std::pair<std::string, std::optional<std::string>>> previous;
  Aws::SDKOptions aws_options;
};

auto find_header(std::vector<http::Header> const& headers,
                 std::string_view name) -> std::string {
  auto it = std::ranges::find_if(headers, [&](auto const& header) {
    return header.name == name;
  });
  return it == headers.end() ? std::string{} : it->value;
}

} // namespace

WITH_FIXTURE(env_fixture) {
  TEST("endpoint override precedence") {
    unset("AWS_ENDPOINT_URL");
    unset("AWS_ENDPOINT_URL_SQS");
    CHECK(not amazon::endpoint_override("SQS"));
    set("AWS_ENDPOINT_URL", "http://generic.example");
    CHECK_EQUAL(*amazon::endpoint_override("SQS"), "http://generic.example");
    CHECK_EQUAL(amazon::service_endpoint_url("sqs", "eu-central-1", "SQS"),
                "http://generic.example");
    set("AWS_ENDPOINT_URL_SQS", "http://sqs.example");
    CHECK_EQUAL(*amazon::endpoint_override("SQS"), "http://sqs.example");
    CHECK_EQUAL(amazon::service_endpoint_url("sqs", "eu-central-1", "SQS"),
                "http://sqs.example");
  }

  TEST("partition-aware endpoints") {
    unset("AWS_ENDPOINT_URL");
    unset("AWS_ENDPOINT_URL_LOGS");
    CHECK_EQUAL(amazon::service_endpoint_url("logs", "eu-central-1", "LOGS"),
                "https://logs.eu-central-1.amazonaws.com");
    CHECK_EQUAL(amazon::service_endpoint_url("logs", "cn-north-1", "LOGS"),
                "https://logs.cn-north-1.amazonaws.com.cn");
    CHECK_EQUAL(amazon::service_endpoint_url("logs", "eusc-de-east-1", "LOGS"),
                "https://logs.eusc-de-east-1.amazonaws.eu");
    CHECK_EQUAL(amazon::service_endpoint_url("logs", "us-iso-east-1", "LOGS"),
                "https://logs.us-iso-east-1.c2s.ic.gov");
    CHECK_EQUAL(amazon::service_endpoint_url("logs", "us-isob-east-1", "LOGS"),
                "https://logs.us-isob-east-1.sc2s.sgov.gov");
    CHECK_EQUAL(amazon::service_endpoint_url("logs", "eu-isoe-west-1", "LOGS"),
                "https://logs.eu-isoe-west-1.cloud.adc-e.uk");
    CHECK_EQUAL(amazon::service_endpoint_url("logs", "us-isof-south-1", "LOGS"),
                "https://logs.us-isof-south-1.csp.hci.ic.gov");
  }

  TEST("region fallback precedence") {
    unset("AWS_REGION");
    unset("AWS_DEFAULT_REGION");
    auto credentials = resolved_aws_credentials{.region = "us-west-2"};
    CHECK_EQUAL(amazon::resolve_region(Option<std::string>{"eu-central-1"},
                                       Option{credentials}),
                "eu-central-1");
    CHECK_EQUAL(amazon::resolve_region(None{}, Option{credentials}),
                "us-west-2");
    credentials.region.clear();
    set("AWS_DEFAULT_REGION", "ap-south-1");
    CHECK_EQUAL(amazon::resolve_region(None{}, Option{credentials}),
                "ap-south-1");
    set("AWS_REGION", "sa-east-1");
    CHECK_EQUAL(amazon::resolve_region(None{}, Option{credentials}),
                "sa-east-1");
    unset("AWS_REGION");
    unset("AWS_DEFAULT_REGION");
    CHECK(not amazon::resolve_region(None{}, Option{credentials}).empty());
  }
}

TEST("URL origin and relative path") {
  auto url = "https://logs.example:9443/base/path?x=1#frag";
  CHECK_EQUAL(amazon::url_origin(url), "https://logs.example:9443");
  CHECK_EQUAL(amazon::url_relative(url), "/base/path?x=1");
  CHECK_EQUAL(amazon::url_relative("https://logs.example"), "/");
}

TEST("AWS error extraction") {
  CHECK_EQUAL(amazon::extract_aws_error_message(R"({"message":"lower"})"),
              "lower");
  CHECK_EQUAL(amazon::extract_aws_error_message(R"({"Message":"upper"})"),
              "upper");
  CHECK_EQUAL(amazon::extract_aws_error_message(R"({"__type":"typed"})"),
              "typed");
  CHECK_EQUAL(amazon::extract_aws_error_message("plain error"), "plain error");
}

TEST("header conversion") {
  auto headers = std::vector<http::Header>{
    {"content-type", "application/json"},
    {"x-amz-target", "AmazonSQS.ReceiveMessage"},
  };
  auto aws = amazon::aws_headers(headers);
  CHECK_EQUAL(amazon::from_aws_string(aws["content-type"]), "application/json");
  CHECK_EQUAL(amazon::from_aws_string(aws["x-amz-target"]),
              "AmazonSQS.ReceiveMessage");
  auto http = amazon::http_headers(aws);
  CHECK_EQUAL(find_header(http, "content-type"), "application/json");
  CHECK_EQUAL(find_header(http, "x-amz-target"), "AmazonSQS.ReceiveMessage");
}

TEST("SDK result conversion") {
  auto response = HttpResponse{
    .status_code = 202,
    .headers = {{"x-amzn-requestid", "abc"}},
    .body = R"({"message":"ok"})",
  };
  auto result = amazon::to_aws_json_result(std::move(response));
  CHECK_EQUAL(result.GetResponseCode(), Aws::Http::HttpResponseCode::ACCEPTED);
  CHECK_EQUAL(
    amazon::from_aws_string(result.GetPayload().View().GetString("message")),
    "ok");
  auto const& headers = result.GetHeaderValueCollection();
  CHECK_EQUAL(amazon::from_aws_string(headers.at("x-amzn-requestid")), "abc");
}
