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
    ctrl.self()
      .request(ctrl.node(), caf::infinite, atom::proxy_v, request)
      .await(
        [&](rest_response& value) {
          response = std::move(value);
        },
        [&](const caf::error& error) {
          diagnostic::error("internal server error: {}", error)
            .note("endpoint: {}", endpoint_)
            .note("request body: {}", request_body_)
            .docs("https://docs.tenzir.com/next/operators/sources/api")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
    TENZIR_ASSERT_CHEAP(response.has_value());
    if (response->is_error()) {
      diagnostic::error("request failed with code {}", response->code())
        .note("body: {}", response->body())
        .note("detail: {}", response->error_detail())
        .docs("https://docs.tenzir.com/next/operators/sources/api")
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
    if (auto arg = p.accept_shell_arg()) {
      endpoint = std::move(arg->inner);
    } else {
      auto endpoints = std::unordered_set<std::string>{};
      for (const auto& plugin : plugins::get<rest_endpoint_plugin>()) {
        for (const auto& endpoint : plugin->rest_endpoints()) {
          endpoints.insert(endpoint.path);
        }
      }
      diagnostic::error("endpoint must be specified")
        .note("must be one of {}", fmt::join(endpoints, ", "))
        .docs("https://docs.tenzir.com/next/operators/sources/api")
        .throw_();
    }
    // TODO: We're using a `caf::settings` here over `record` because that
    // supports inserting `autostart.created=true` as a nested value directly.
    // Since we're just working with strings here there is no loss of precision,
    // but it's still not ideal to have to do this roundtrip.
    auto request_body = caf::settings{};
    while (auto arg = p.accept_shell_arg()) {
      auto kvp = detail::split_escaped(arg->inner, "=", "\\", 1);
      TENZIR_ASSERT(kvp.size() == 2);
      caf::put(request_body, kvp[0], std::move(kvp[1]));
    }
    auto request_body_json
      = to_json(to_data(request_body), {.style = no_style(), .oneline = true});
    if (not request_body_json) {
      diagnostic::error("failed to format request body: {}",
                        request_body_json.error())
        .docs("https://docs.tenzir.com/next/operators/sources/api")
        .throw_();
    }
    return std::make_unique<api_operator>(std::move(endpoint),
                                          std::move(*request_body_json));
  }
};

} // namespace

} // namespace tenzir::plugins::api

TENZIR_REGISTER_PLUGIN(tenzir::plugins::api::plugin)
