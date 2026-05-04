//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/async/fetch_node.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/node.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::api {

namespace {

struct ApiArgs {
  located<std::string> endpoint = {};
  Option<located<record>> request_body = {};
};

using ApiResult = caf::expected<rest_response>;

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

class Api final : public Operator<void, table_slice> {
public:
  explicit Api(ApiArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.request_body) {
      auto request_body_json = to_json(args_.request_body->inner);
      if (not request_body_json) {
        diagnostic::error(request_body_json.error())
          .primary(args_.request_body->source)
          .emit(ctx);
        done_ = true;
        co_return;
      }
      request_body_ = std::move(*request_body_json);
    }
    auto node = co_await fetch_node(ctx.actor_system(), ctx.dh());
    if (not node) {
      diagnostic::error("failed to connect to node")
        .primary(args_.endpoint.source)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    node_ = std::move(*node);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    const auto request = http_request_description{
      .canonical_path = fmt::format("POST {} (v0)", args_.endpoint.inner),
      .json_body = request_body_,
    };
    auto result = co_await async_mail(atom::proxy_v, request, std::string{})
                    .request(node_);
    co_return result;
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto response = std::move(result).as<ApiResult>();
    done_ = true;
    if (not response) {
      auto error = std::move(response.error());
      if (error == ec::no_error) {
        error = ec::unspecified;
      }
      diagnostic::error(std::move(error))
        .note("internal server error")
        .note("endpoint: {}", args_.endpoint.inner)
        .note("request body: {}", request_body_)
        .emit(ctx);
      co_return;
    }
    if (response->is_error()) {
      auto detail = response->error_detail();
      if (detail == ec::no_error) {
        detail = ec::unspecified;
      }
      diagnostic::error(std::move(detail))
        .note("request failed with code {}", response->code())
        .note("body: {}", response->body())
        .emit(ctx);
      co_return;
    }
    auto parsed_response = from_json(response->body());
    if (not parsed_response) {
      diagnostic::error("failed to parse response: {}", parsed_response.error())
        .emit(ctx);
      co_return;
    }
    auto builder = series_builder{};
    builder.data(*parsed_response);
    for (auto&& slice : builder.finish_as_table_slice("tenzir.api")) {
      co_await push(std::move(slice));
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  ApiArgs args_;
  std::string request_body_ = "{}";
  node_actor node_ = {};
  bool done_ = false;
};

class plugin final : public virtual operator_plugin<api_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
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

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto endpoint = located<std::string>{};
    auto request_body = std::optional<located<record>>{};
    TRY(argument_parser2::operator_("api")
          .positional("endpoint", endpoint)
          .positional("request_body", request_body)
          .parse(inv, ctx));
    if (not request_body) {
      return std::make_unique<api_operator>(std::move(endpoint.inner), "{}");
    }
    auto request_body_json = check(to_json(request_body->inner));
    return std::make_unique<api_operator>(std::move(endpoint.inner),
                                          std::move(request_body_json));
  }

  auto describe() const -> Description override {
    auto d = Describer<ApiArgs, Api>{"https://docs.tenzir.com/operators/api"};
    d.positional("endpoint", &ApiArgs::endpoint);
    d.positional("request_body", &ApiArgs::request_body);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::api

TENZIR_REGISTER_PLUGIN(tenzir::plugins::api::plugin)
