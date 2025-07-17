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
#include <tenzir/tql2/plugin.hpp>

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
      .mail(atom::proxy_v, request, request_id)
      .request(ctrl.node(), caf::infinite)
      .then(
        [&](rest_response& value) {
          response = std::move(value);
          ctrl.set_waiting(false);
        },
        [&](caf::error error) {
          if (error == ec::no_error) {
            error = ec::unspecified;
          }
          diagnostic::error(std::move(error))
            .note("internal server error")
            .note("endpoint: {}", endpoint_)
            .note("request body: {}", request_body_)
            .emit(ctrl.diagnostics());
        });
    ctrl.set_waiting(true);
    co_yield {};
    TENZIR_ASSERT(response.has_value());
    if (response->is_error()) {
      auto detail = response->error_detail();
      if (detail == ec::no_error) {
        detail = ec::unspecified;
      }
      diagnostic::error(std::move(detail))
        .note("request failed with code {}", response->code())
        .note("body: {}", response->body())
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

class plugin final : public virtual operator_plugin<api_operator>,
                     public virtual operator_factory_plugin {
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

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto endpoint = located<std::string>{};
    auto request_body = std::optional<located<record>>{};
    argument_parser2::operator_("api")
      .positional("endpoint", endpoint)
      .positional("request_body", request_body)
      .parse(inv, ctx)
      .ignore();
    if (not request_body) {
      return std::make_unique<api_operator>(std::move(endpoint.inner), "{}");
    }
    auto request_body_json = check(to_json(request_body->inner));
    return std::make_unique<api_operator>(std::move(endpoint.inner),
                                          std::move(request_body_json));
  }
};

} // namespace

} // namespace tenzir::plugins::api

TENZIR_REGISTER_PLUGIN(tenzir::plugins::api::plugin)
