//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/node.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::api {

namespace {

class api_operator final : public crtp_operator<api_operator> {
public:
  api_operator() = default;

  explicit api_operator(std::string endpoint, std::string request_body)
    : endpoint_{std::move(endpoint)}, request_body_{std::move(request_body)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    const auto request = http_request_description{
      .canonical_path = fmt::format("POST {} (v0)", endpoint_),
      .json_body = request_body_,
    };
    auto response = std::optional<rest_response>{};
    const auto request_id = std::string{};
    ctrl.self()
      .request(ctrl.node(), caf::infinite, atom::proxy_v, request, request_id)
      .await(
        [&](rest_response& value) {
          response = std::move(value);
        },
        [&](const caf::error& error) {
          diagnostic::error("internal server error: {}", error)
            .note("endpoint: {}", endpoint_)
            .note("request body: {}", request_body_)
            .docs("https://docs.tenzir.com/operators/api")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    TENZIR_ASSERT(response.has_value());
    if (response->is_error()) {
      diagnostic::error("request failed with code {}", response->code())
        .note("body: {}", response->body())
        .hint("{}", response->error_detail())
        .docs("https://docs.tenzir.com/operators/api")
        .emit(ctrl.diagnostics());
      co_return;
    }
    const auto parsed_response = from_json(response->body());
    if (not parsed_response) {
      diagnostic::error("failed to parse response: {}", parsed_response.error())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto builder = series_builder{};
    builder.data(*parsed_response);
    for (auto&& slice : builder.finish_as_table_slice("tenzir.api")) {
      co_yield std::move(slice);
    }
  }

  auto name() const -> std::string override {
    return "api";
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, api_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.api.api_operator")
      .fields(f.field("endpoint", x.endpoint_),
              f.field("request-body", x.request_body_));
  }

private:
  std::string endpoint_ = {};
  std::string request_body_ = {};
};

class plugin final : public virtual operator_plugin<api_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto endpoint = std::string{};
    auto request_body = std::optional<std::string>{};
    auto parser
      = argument_parser{"api", "https://docs.tenzir.com/operators/api"};
    parser.add(endpoint, "<command>");
    parser.add(request_body, "<request-body>");
    parser.parse(p);
    return std::make_unique<api_operator>(
      std::move(endpoint), request_body ? std::move(*request_body) : "{}");
  }
};

} // namespace

} // namespace tenzir::plugins::api

TENZIR_REGISTER_PLUGIN(tenzir::plugins::api::plugin)
