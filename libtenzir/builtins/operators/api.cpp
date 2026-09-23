//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/fetch_node.hpp>
#include <tenzir/async/mail.hpp>
#include <tenzir/node.hpp>
#include <tenzir/nova/data_array_builder.hpp>
#include <tenzir/nova/events.hpp>
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
  Option<located<record>> request_body = None{};
};

using ApiResult = caf::expected<rest_response>;

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

class ApiNova final : public Operator<void, nova::Events> {
public:
  explicit ApiNova(ApiArgs args) : args_{std::move(args)} {
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

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
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
    auto builder = nova::ArrayBuilder<nova::Data>{};
    nova::append_legacy_data(builder, *parsed_response, ctx.dh());
    const auto array = builder.finish();
    auto record = array.get_alternative<nova::Record>();
    if (not record or not record->present.get(0)) {
      diagnostic::error("expected the response to be a record")
        .primary(args_.endpoint.source)
        .note("got: {}", *parsed_response)
        .emit(ctx);
      co_return;
    }
    const auto length = record->data.length();
    co_await push(nova::Events{
      std::move(record->data),
      std::move(record->present),
      nova::Events::Meta::make_empty(length, "tenzir.api"),
    });
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

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "api";
  }

  auto describe() const -> Description override {
    auto d = Describer<ApiArgs, Api, ApiNova>{
      "https://tenzir.com/docs/operators/api"};
    d.positional("endpoint", &ApiArgs::endpoint);
    d.positional("request_body", &ApiArgs::request_body);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::api

TENZIR_REGISTER_PLUGIN(tenzir::plugins::api::plugin)
