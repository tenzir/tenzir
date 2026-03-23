//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/arc.hpp"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/box.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/pipeline_executor.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <boost/url/parse.hpp>
#include <caf/action.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/detail/pp.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/flow/observable_builder.hpp>
#include <caf/net/http/client.hpp>
#include <caf/net/http/method.hpp>
#include <caf/net/http/server.hpp>
#include <caf/net/http/with.hpp>
#include <caf/net/ssl/context.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/timespan.hpp>
#include <openssl/ssl.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Sleep.h>

#include <charconv>
#include <deque>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>

constexpr auto max_response_size = std::numeric_limits<int32_t>::max();

namespace tenzir::plugins::http {
namespace {

namespace caf_http = caf::net::http;
namespace ssl = caf::net::ssl;
using namespace std::literals;

template <typename T>
constexpr auto inner(const std::optional<located<T>>& x) -> std::optional<T> {
  return x.transform([](auto&& x) {
    return x.inner;
  });
};

auto try_decompress_body(const std::string_view encoding,
                         const std::span<const std::byte> body,
                         diagnostic_handler& dh) -> std::optional<blob> {
  return tenzir::http::try_decompress_body(encoding, body, dh);
}

struct http_actor_traits final {
  using signatures = caf::type_list<
    /// Push events from subpipeline into self.
    auto(atom::internal, atom::push, table_slice output)->caf::result<void>,
    /// Get events from self to parent.
    auto(atom::pull)->caf::result<table_slice>>
    /// Support the diagnostic receiver interface.
    ::append_from<receiver_actor<diagnostic>::signatures>
    /// Support the metrics receiver interface for the branch pipelines.
    ::append_from<metrics_receiver_actor::signatures>;
};

using http_actor = caf::typed_actor<http_actor_traits>;

struct internal_source final : public crtp_operator<internal_source> {
  internal_source() = default;

  internal_source(chunk_ptr ptr) : ptr_{std::move(ptr)} {
  }

  auto operator()(operator_control_plane&) const -> generator<chunk_ptr> {
    co_yield {};
    co_yield ptr_;
  }

  auto name() const -> std::string override {
    return "internal-http-source";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, internal_source& x) {
    return f.apply(x.ptr_);
  }

private:
  chunk_ptr ptr_;
};

struct internal_sink final : public crtp_operator<internal_sink> {
  internal_sink() = default;

  internal_sink(http_actor actor, std::optional<expression> filter,
                tenzir::location op)
    : actor_{std::move(actor)}, op_{op}, filter_{std::move(filter)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    for (auto slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      TENZIR_TRACE("[internal-http-sink] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::push_v, std::move(slice))
        .request(actor_, caf::infinite)
        .then(
          [&] {
            TENZIR_TRACE("[internal-http-sink] pushed slice");
            ctrl.set_waiting(false);
          },
          [&](const caf::error& e) {
            diagnostic::error("failed to push events: {}", e)
              .primary(op_)
              .emit(ctrl.diagnostics());
          });
      co_yield {};
    }
    ctrl.set_waiting(true);
    ctrl.self()
      .mail(atom::internal_v, atom::push_v, table_slice{})
      .request(actor_, caf::infinite)
      .then(
        [&] {
          TENZIR_TRACE("[internal-http-sink] pushed final slice");
          ctrl.set_waiting(false);
        },
        [&](const caf::error& e) {
          diagnostic::error("failed to push events: {}", e)
            .primary(op_)
            .emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto name() const -> std::string override {
    return "internal-http-sink";
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return {filter_, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, internal_sink& x) {
    return f.object(x).fields(f.field("actor_", x.actor_),
                              f.field("op_", x.op_),
                              f.field("filter_", x.filter_));
  }

private:
  http_actor actor_;
  tenzir::location op_;
  std::optional<expression> filter_;
};

struct http_state {
  [[maybe_unused]] static constexpr auto name = "http";

  http_state(http_actor::pointer self, shared_diagnostic_handler dh,
             metrics_receiver_actor metrics, uint64_t operator_index,
             exec_node_actor spawner)
    : self_{self},
      dh_{std::move(dh)},
      metrics_receiver_{std::move(metrics)},
      operator_index_{operator_index} {
    self_->monitor(std::move(spawner), [&](const caf::error& e) {
      self_->quit(e);
    });
  }

  ~http_state() {
    if (slice_rp_.pending()) {
      slice_rp_.deliver(table_slice{});
    }
  }

  auto make_behavior() -> http_actor::behavior_type {
    return {
      [&](atom::internal, atom::push, table_slice output) {
        if (slice_rp_.pending()) {
          slice_rp_.deliver(std::move(output));
          return;
        }
        slices_.push_back(std::move(output));
      },
      [&](atom::pull) -> caf::result<table_slice> {
        TENZIR_ASSERT(not slice_rp_.pending());
        if (slices_.empty()) {
          slice_rp_ = self_->make_response_promise<table_slice>();
          return slice_rp_;
        }
        auto x = std::move(slices_.front());
        slices_.pop_front();
        if (slices_.empty() and exited) {
          self_->schedule_fn([this] {
            self_->quit();
          });
        }
        return x;
      },
      [this](diagnostic diag) {
        dh_.emit(std::move(diag));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             type schema) {
        auto& id
          = registered_metrics_[nested_operator_index][nested_metrics_id];
        id = uuid::random();
        return self_->mail(operator_index_, id, std::move(schema))
          .delegate(metrics_receiver_);
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             record metrics) {
        const auto& id
          = registered_metrics_[nested_operator_index][nested_metrics_id];
        return self_->mail(operator_index_, id, std::move(metrics))
          .delegate(metrics_receiver_);
      },
      [](const operator_metric& metrics) {
        // We deliberately ignore operator metrics. There's no good way to
        // forward them from nested pipelines, and nowadays operator metrics
        // are really only relevant for generating pipeline metrics. If there's
        // a sink in the then-branch we're unfortunately losing its egress
        // metrics at the moment.
        TENZIR_UNUSED(metrics);
      },
      [this](const caf::exit_msg& msg) {
        exited = true;
        if (slices_.empty() or msg.reason.valid()) {
          self_->quit(msg.reason);
        }
      },
    };
  }

private:
  http_actor::pointer self_;
  shared_diagnostic_handler dh_;
  metrics_receiver_actor metrics_receiver_;
  uint64_t operator_index_ = 0;
  detail::flat_map<uint64_t, detail::flat_map<uuid, uuid>> registered_metrics_;
  caf::typed_response_promise<table_slice> slice_rp_;
  std::deque<table_slice> slices_;
  bool exited = false;
};

auto find_decompression_plugin_name(std::string_view ext)
  -> std::optional<std::string> {
  for (const auto& plugin : plugins::get<operator_factory_plugin>()) {
    if (std::ranges::contains(plugin->decompress_properties().extensions,
                              ext)) {
      TENZIR_TRACE("[http] inferred plugin `{}` for extension `{}`",
                   plugin->name(), ext);
      return plugin->name();
    }
  }
  return std::nullopt;
}

auto find_parser_plugin_name(std::string_view ext) -> std::optional<std::string> {
  for (const auto& plugin : plugins::get<operator_factory_plugin>()) {
    if (std::ranges::contains(plugin->read_properties().extensions, ext)) {
      TENZIR_TRACE("[http] inferred plugin `{}` for extension `{}`",
                   plugin->name(), ext);
      return plugin->name();
    }
  }
  return std::nullopt;
}

auto find_parser_plugin_name_for_mime(std::string_view mime)
  -> std::optional<std::string> {
  mime = mime.substr(0, mime.find(';'));
  for (const auto& plugin : plugins::get<operator_factory_plugin>()) {
    if (std::ranges::contains(plugin->read_properties().mime_types, mime)) {
      return plugin->name();
    }
  }
  return std::nullopt;
}

auto make_http_plugin_invocation(std::string_view plugin, location op)
  -> ast::invocation {
  return ast::invocation{
    ast::entity{
      std::vector{ast::identifier{std::string{plugin}, op}},
    },
    {},
  };
}

auto infer_http_pipeline_plugin_names(const caf::uri& uri,
                                      const caf_http::response& r,
                                      location oploc,
                                      diagnostic_handler& dh)
  -> failure_or<std::vector<std::string>> {
  if (auto parsed = boost::urls::parse_uri_reference(uri.str())) {
    if (not parsed->segments().empty()) {
      auto segment = parsed->segments().back();
      if (const auto last_dot = segment.rfind('.');
          last_dot != std::string::npos and last_dot + 1 != segment.size()
          and last_dot != 0) {
        auto names = std::vector<std::string>{};
        auto extension = segment.substr(last_dot + 1);
        TENZIR_TRACE("[http] finding decompression plugin for extension `{}`",
                     extension);
        if (auto decompressor = find_decompression_plugin_name(extension)) {
          names.push_back(*decompressor);
          if (const auto first_dot = segment.rfind('.', last_dot - 1);
              first_dot != std::string::npos and first_dot != 0) {
            extension = segment.substr(first_dot + 1, last_dot - first_dot - 1);
          }
        }
        TENZIR_TRACE("[http] finding parser plugin for extension `{}`",
                     extension);
        if (auto parser = find_parser_plugin_name(extension)) {
          names.push_back(*parser);
          return names;
        }
      }
    }
  } else {
    diagnostic::error("invalid URI `{}`", uri.str()).primary(oploc).emit(dh);
    return failure::promise();
  }
  const auto& headers = r.header_fields();
  const auto mit = std::ranges::find_if(headers, [](const auto& x) {
    return detail::ascii_icase_equal(x.first, "content-type");
  });
  if (mit == std::ranges::end(headers) or mit->second.empty()) {
    diagnostic::error(
      "cannot deduce a parser without a valid `Content-Type` header")
      .primary(oploc)
      .hint("consider specifying a parsing pipeline if the format is known")
      .emit(dh);
    return failure::promise();
  }
  if (auto parser = find_parser_plugin_name_for_mime(mit->second)) {
    return std::vector<std::string>{*parser};
  }
  auto mime = mit->second.substr(0, mit->second.find(';'));
  diagnostic::error("could not find a parser for mime-type `{}`", mime)
    .primary(oploc)
    .hint("consider specifying a parsing pipeline if the format is known")
    .emit(dh);
  return failure::promise();
}

auto infer_http_pipeline_plugin_names(const caf::uri& uri,
                                      const tenzir::http::ResponseData& r,
                                      location oploc,
                                      diagnostic_handler& dh)
  -> failure_or<std::vector<std::string>> {
  if (auto parsed = boost::urls::parse_uri_reference(uri.str())) {
    if (not parsed->segments().empty()) {
      auto segment = parsed->segments().back();
      if (const auto last_dot = segment.rfind('.');
          last_dot != std::string::npos and last_dot + 1 != segment.size()
          and last_dot != 0) {
        auto names = std::vector<std::string>{};
        auto extension = segment.substr(last_dot + 1);
        TENZIR_TRACE("[http] finding decompression plugin for extension `{}`",
                     extension);
        if (auto decompressor = find_decompression_plugin_name(extension)) {
          names.push_back(*decompressor);
          if (const auto first_dot = segment.rfind('.', last_dot - 1);
              first_dot != std::string::npos and first_dot != 0) {
            extension = segment.substr(first_dot + 1, last_dot - first_dot - 1);
          }
        }
        TENZIR_TRACE("[http] finding parser plugin for extension `{}`",
                     extension);
        if (auto parser = find_parser_plugin_name(extension)) {
          names.push_back(*parser);
          return names;
        }
      }
    }
  } else {
    diagnostic::error("invalid URI `{}`", uri.str()).primary(oploc).emit(dh);
    return failure::promise();
  }
  auto content_type = tenzir::http::find_header_value(r.headers, "content-type");
  if (content_type.empty()) {
    diagnostic::error(
      "cannot deduce a parser without a valid `Content-Type` header")
      .primary(oploc)
      .hint("consider specifying a parsing pipeline if the format is known")
      .emit(dh);
    return failure::promise();
  }
  if (auto parser = find_parser_plugin_name_for_mime(content_type)) {
    return std::vector<std::string>{*parser};
  }
  auto mime = std::string{content_type.substr(0, content_type.find(';'))};
  diagnostic::error("could not find a parser for mime-type `{}`", mime)
    .primary(oploc)
    .hint("consider specifying a parsing pipeline if the format is known")
    .emit(dh);
  return failure::promise();
}

auto make_http_pipeline_from_names(std::vector<std::string> plugin_names,
                                   location oploc, diagnostic_handler& dh)
  -> failure_or<located<pipeline>> {
  auto ops = std::vector<operator_ptr>{};
  auto sp = session_provider::make(dh);
  for (auto const& name : plugin_names) {
    auto* plugin = plugins::find<operator_factory_plugin>(name);
    TENZIR_ASSERT(plugin);
    auto inv = operator_factory_plugin::invocation{
      ast::entity{
        std::vector{ast::identifier{name, oploc}},
      },
      {},
    };
    TRY(auto opptr, plugin->make(std::move(inv), sp.as_session()));
    ops.push_back(std::move(opptr));
  }
  return located{pipeline{std::move(ops)}, oploc};
}

auto make_http_ir_pipeline_from_names(std::vector<std::string> plugin_names,
                                      location oploc, diagnostic_handler& dh)
  -> failure_or<located<ir::pipeline>> {
  auto reg = global_registry();
  auto ctx = compile_ctx::make_root(base_ctx{dh, *reg});
  auto ops = std::vector<Box<ir::Operator>>{};
  for (auto const& name : plugin_names) {
    auto* plugin = plugins::find<operator_compiler_plugin>(name);
    TENZIR_ASSERT(plugin);
    TRY(auto op, plugin->compile(make_http_plugin_invocation(name, oploc), ctx));
    ops.push_back(std::move(op));
  }
  return located{ir::pipeline{{}, std::move(ops)}, oploc};
}

auto make_pipeline(const std::optional<located<pipeline>>& pipe,
                   const caf::uri& uri, const caf_http::response& r,
                   location oploc, diagnostic_handler& dh)
  -> failure_or<located<pipeline>> {
  if (pipe) {
    return *pipe;
  }
  TRY(auto names, infer_http_pipeline_plugin_names(uri, r, oploc, dh));
  return make_http_pipeline_from_names(std::move(names), oploc, dh);
}

auto make_ir_pipeline(const std::optional<located<ir::pipeline>>& pipe,
                      const caf::uri& uri,
                      const tenzir::http::ResponseData& r,
                      location oploc, diagnostic_handler& dh)
  -> failure_or<located<ir::pipeline>> {
  if (pipe) {
    return *pipe;
  }
  TRY(auto names, infer_http_pipeline_plugin_names(uri, r, oploc, dh));
  return make_http_ir_pipeline_from_names(std::move(names), oploc, dh);
}

auto make_pipeline(const std::optional<located<pipeline>>& pipe,
                   const caf_http::request& r, location oploc,
                   diagnostic_handler& dh) -> failure_or<located<pipeline>> {
  if (pipe) {
    return *pipe;
  }
  auto mime = r.header().field("content-type");
  if (mime.empty()) {
    diagnostic::error(
      "cannot deduce a parser without a valid `Content-Type` header")
      .primary(oploc)
      .hint("consider specifying a parsing pipeline if the format is known")
      .emit(dh);
    return failure::promise();
  }
  auto parser = find_parser_plugin_name_for_mime(mime);
  if (not parser) {
    diagnostic::error("could not find a parser for mime-type `{}`", mime)
      .primary(oploc)
      .hint("consider specifying a parsing pipeline if the format is known")
      .emit(dh);
    return failure::promise();
  }
  return make_http_pipeline_from_names({*parser}, oploc, dh);
}

auto spawn_pipeline(operator_control_plane& ctrl, located<pipeline> pipe,
                    std::optional<expression> filter, chunk_ptr ptr,
                    bool is_warning) -> http_actor {
  TENZIR_TRACE("[http] spawning http_actor");
  // TODO: Figure out why only `from_http` shuts down when spawned as linked
  auto ha = ctrl.self().spawn(caf::actor_from_state<http_state>,
                              ctrl.shared_diagnostics(),
                              ctrl.metrics_receiver(), ctrl.operator_index(),
                              static_cast<exec_node_actor>(&ctrl.self()));
  ctrl.self().monitor(ha, [&ctrl, is_warning,
                           loc = pipe.source](const caf::error& e) {
    if (e.valid()) {
      diagnostic::error(e)
        .primary(loc)
        .severity(is_warning ? severity::warning : severity::error)
        .emit(ctrl.diagnostics());
    }
  });
  pipe.inner.prepend(std::make_unique<internal_source>(std::move(ptr)));
  // Only append internal_sink if the pipeline doesn't already end with a sink
  auto output_type = pipe.inner.infer_type<void>();
  TENZIR_ASSERT(output_type);
  if (output_type->is_not<void>()) {
    pipe.inner.append(
      std::make_unique<internal_sink>(ha, std::move(filter), pipe.source));
  }
  TENZIR_TRACE("[http] spawning subpipeline");
  const auto handle = ctrl.self().spawn(
    pipeline_executor, std::move(pipe.inner).optimize_if_closed(),
    std::string{ctrl.definition()}, ha, ha, ctrl.node(), ctrl.has_terminal(),
    ctrl.is_hidden(), std::string{ctrl.pipeline_id()});
  handle->link_to(ha);
  ha->attach_functor([handle] {});
  TENZIR_TRACE("[http] requesting subpipeline start");
  ctrl.self()
    .mail(atom::start_v)
    .request(handle, caf::infinite)
    .then(
      [] {
        TENZIR_TRACE("[http] subpipeline started");
      },
      [&ctrl, is_warning, loc = pipe.source](const caf::error& e) {
        TENZIR_TRACE("[http] failed to start subpipeline: {}", e);
        diagnostic::error(e)
          .primary(loc)
          .severity(is_warning ? severity::warning : severity::error)
          .emit(ctrl.diagnostics());
      });
  return ha;
}

using pagination_spec = located<tenzir::variant<ast::lambda_expr, std::string>>;

auto validate_paginate(std::optional<ast::expression> expr,
                       diagnostic_handler& dh)
  -> failure_or<std::optional<pagination_spec>> {
  if (not expr) {
    return std::nullopt;
  }
  if (const auto* lambda = try_as<ast::lambda_expr>(*expr)) {
    if (not lambda->is_unary()) {
        diagnostic::error("expected unary lambda")
          .primary(*lambda)
          .hint("binary lambdas are only supported for `sort(..., cmp=...)`")
          .emit(dh);
      return failure::promise();
    }
    return std::optional<pagination_spec>{
      {tenzir::variant<ast::lambda_expr, std::string>{*lambda},
       expr->get_location()}};
  }
  TRY(auto value, const_eval(*expr, dh));
  return match(
    value,
    [&](const std::string& mode) -> failure_or<std::optional<pagination_spec>> {
      if (mode != "link") {
          diagnostic::error("unsupported pagination mode: `{}`", mode)
            .primary(*expr)
            .hint("`paginate` must be `\"link\"` or a lambda")
            .emit(dh);
          return failure::promise();
        }
      return std::optional<pagination_spec>{
        {tenzir::variant<ast::lambda_expr, std::string>{mode},
         expr->get_location()}};
    },
    [&](const auto&) -> failure_or<std::optional<pagination_spec>> {
      const auto ty = type::infer(value);
      diagnostic::error("expected `paginate` to be `string` or `lambda`")
        .primary(*expr, "got `{}`", ty ? ty->kind() : type_kind{})
        .hint("`paginate` must be `\"link\"` or a lambda")
        .emit(dh);
      return failure::promise();
    });
}

auto field_paths_overlap(ast::field_path const& lhs, ast::field_path const& rhs)
  -> bool {
  auto lhs_path = lhs.path();
  auto rhs_path = rhs.path();
  auto i = size_t{};
  for (; i < lhs_path.size() and i < rhs_path.size(); ++i) {
    if (lhs_path[i].id.name != rhs_path[i].id.name) {
      return false;
    }
  }
  return i == lhs_path.size() or i == rhs_path.size();
}

auto to_chrono(duration d) -> std::chrono::milliseconds {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

struct header_evaluation {
  std::unordered_map<std::string, std::string> values;
  bool has_content_type = false;
  bool has_accept_header = false;
};

auto evaluate_http_urls(ast::expression const& url_expr, table_slice const& slice,
                        std::vector<secret_request>& requests,
                        std::vector<std::string>& urls,
                        diagnostic_handler& dh) -> void {
  urls.clear();
  urls.reserve(slice.rows());
  auto url_warned = false;
  auto url_ms = eval(url_expr, slice, dh);
  for (auto const& part : url_ms.parts()) {
    if (part.type.kind().is<string_type>()) {
      for (auto value : part.values<string_type>()) {
        if (value) {
          urls.emplace_back(*value);
        } else {
          url_warned = true;
          urls.emplace_back();
        }
      }
      continue;
    }
    if (part.type.kind().is<secret_type>()) {
      for (auto const& value : part.values<secret_type>()) {
        if (value) {
          requests.emplace_back(make_secret_request(
            "url", materialize(*value), url_expr.get_location(),
            urls.emplace_back(), dh));
        } else {
          url_warned = true;
          urls.emplace_back();
        }
      }
      continue;
    }
    diagnostic::warning("expected `string`, got `{}`", part.type.kind())
      .primary(url_expr)
      .note("skipping request")
      .emit(dh);
    urls.insert(urls.end(), part.length(), {});
  }
  if (url_warned) {
    diagnostic::warning("`url` must not be null")
      .primary(url_expr)
      .note("skipping request")
      .emit(dh);
  }
}

auto evaluate_http_headers(
  std::optional<ast::expression> const& headers_expr, bool has_body,
  table_slice const& input, std::vector<secret_request>& requests,
  std::vector<header_evaluation>& headers, diagnostic_handler& dh) -> void {
  headers.clear();
  headers.reserve(input.rows());
  if (not headers_expr) {
    headers.resize(input.rows());
    return;
  }
  auto header_warned = false;
  auto location = headers_expr->get_location();
  auto header_ms = eval(*headers_expr, input, dh);
  for (auto const& part : header_ms.parts()) {
    if (part.type.kind().is_not<record_type>()) {
      headers.insert(headers.end(), part.length(), header_evaluation{});
      diagnostic::warning("expected `record`, got `{}`", part.type.kind())
        .primary(*headers_expr)
        .note("skipping headers")
        .emit(dh);
      continue;
    }
    for (auto const& value : part.values<record_type>()) {
      auto& row_headers = headers.emplace_back();
      if (not value) {
        diagnostic::warning("expected `record`, got `null`")
          .primary(*headers_expr)
          .note("skipping headers")
          .emit(dh);
        continue;
      }
      for (auto const& [name, header_value] : *value) {
        row_headers.has_content_type
          |= has_body and detail::ascii_icase_equal(name, "content-type");
        row_headers.has_accept_header
          |= detail::ascii_icase_equal(name, "accept");
        match(
          header_value,
          [&](std::string_view x) {
            row_headers.values.emplace(name, x);
          },
          [&](secret_view x) {
            auto key = std::string{name};
            requests.emplace_back(make_secret_request(
              key, materialize(x), location, row_headers.values[key], dh));
          },
          [&](auto const&) {
            if (not header_warned) {
              header_warned = true;
              diagnostic::warning(
                "`headers` must be `{{ string: string|secret }}`")
                .primary(*headers_expr)
                .note("skipping invalid header values")
                .emit(dh);
            }
          });
      }
    }
  }
}

auto eval_http_body(std::optional<ast::expression> const& body_expr,
                    std::optional<located<std::string>> const& encode,
                    table_slice const& slice, diagnostic_handler& dh)
  -> generator<std::pair<std::string_view, bool>> {
  if (not body_expr) {
    for (auto i = size_t{}; i < slice.rows(); ++i) {
      co_yield {};
    }
    co_return;
  }
  auto ms = eval(*body_expr, slice, dh);
  for (auto const& part : ms.parts()) {
    if (part.type.kind().is<null_type>()) {
      for (auto i = int64_t{}; i < part.length(); ++i) {
        co_yield {};
      }
      continue;
    }
    if (part.type.kind().is<blob_type>()) {
      for (auto value : part.values<blob_type>()) {
        if (not value) {
          co_yield {};
          continue;
        }
        co_yield {
          {reinterpret_cast<char const*>(value->data()), value->size()},
          false,
        };
      }
      continue;
    }
    if (part.type.kind().is<string_type>()) {
      for (auto value : part.values<string_type>()) {
        if (not value) {
          co_yield {};
          continue;
        }
        co_yield {value.value(), false};
      }
      continue;
    }
    if (part.type.kind().is<record_type>()) {
      auto buffer = std::string{};
      auto const form = encode and encode->inner == "form";
      for (auto value : part.values<record_type>()) {
        if (not value) {
          co_yield {};
          continue;
        }
        if (form) {
          buffer = curl::escape(flatten(materialize(value.value())));
          co_yield {buffer, true};
          buffer.clear();
          continue;
        }
        auto printer = json_printer{{}};
        auto it = std::back_inserter(buffer);
        printer.print(it, value.value());
        co_yield {buffer, true};
        buffer.clear();
      }
      continue;
    }
    diagnostic::warning("expected `blob`, `record` or `string`, got `{}`",
                        part.type.kind())
      .primary(*body_expr)
      .emit(dh);
    for (auto i = int64_t{}; i < part.length(); ++i) {
      co_yield {};
    }
  }
}

auto eval_http_optional_string(std::optional<ast::expression> const& expr,
                               table_slice const& slice,
                               diagnostic_handler& dh)
  -> generator<std::string_view> {
  if (not expr) {
    for (auto i = size_t{}; i < slice.rows(); ++i) {
      co_yield {};
    }
    co_return;
  }
  auto ms = eval(*expr, slice, dh);
  for (auto const& part : ms.parts()) {
    if (part.type.kind().is<null_type>()) {
      for (auto i = int64_t{}; i < part.length(); ++i) {
        co_yield {};
      }
      continue;
    }
    if (part.type.kind().is<string_type>()) {
      for (auto value : part.values<string_type>()) {
        co_yield value.value_or("");
      }
      continue;
    }
    diagnostic::warning("expected `string`, got `{}`", part.type.kind())
      .primary(*expr)
      .emit(dh);
    for (auto i = int64_t{}; i < part.length(); ++i) {
      co_yield {};
    }
  }
}

auto next_url_from_lambda(const std::optional<pagination_spec>& paginate,
                          const table_slice& slice, diagnostic_handler& dh)
  -> std::optional<std::string> {
  if (not paginate) {
    return std::nullopt;
  }
  const auto* lambda = try_as<ast::lambda_expr>(&paginate->inner);
  if (not lambda) {
    return std::nullopt;
  }
  if (slice.rows() != 1) {
    diagnostic::warning("cannot paginate over multiple events")
      .primary(*paginate)
      .note("stopping pagination")
      .emit(dh);
    return std::nullopt;
  }
  const auto ms = eval(*lambda, series{slice}, dh);
  const auto val = ms.value_at(0);
  return match(
    val,
    [](const caf::none_t&) -> std::optional<std::string> {
      TENZIR_TRACE("[http] finishing pagination");
      return std::nullopt;
    },
    [](const std::string_view& url) -> std::optional<std::string> {
      TENZIR_TRACE("[http] paginating: {}", url);
      return std::string{url};
    },
    [&](const auto&) -> std::optional<std::string> {
      diagnostic::error("expected `paginate` to be `string`, got `{}`",
                        ms.parts().front().type.kind())
        .primary(*paginate)
        .emit(dh);
      return std::nullopt;
    });
}

auto is_link_pagination(const std::optional<pagination_spec>& paginate)
  -> bool {
  if (not paginate) {
    return false;
  }
  const auto* mode = try_as<std::string>(&paginate->inner);
  if (not mode) {
    return false;
  }
  TENZIR_ASSERT(*mode == "link");
  return true;
}

auto next_url_from_link_headers(const std::optional<pagination_spec>& paginate,
                                const caf_http::response& response,
                                const caf::uri& request_uri,
                                diagnostic_handler& dh)
  -> std::optional<std::string> {
  const auto link_pagination = is_link_pagination(paginate);
  TENZIR_ASSERT(link_pagination);
  if (not link_pagination) {
    return std::nullopt;
  }
  auto response_data = tenzir::http::ResponseData{};
  response_data.headers.reserve(response.header_fields().size());
  for (const auto& [name, value] : response.header_fields()) {
    response_data.headers.emplace_back(name, value);
  }
  auto request_uri_str = request_uri.str();
  return tenzir::http::next_url_from_link_headers(
    response_data, request_uri_str, std::optional<location>{paginate->source},
    dh);
}

auto make_http_response_metadata_record(const caf_http::response& response)
  -> record {
  auto response_data = tenzir::http::ResponseData{};
  response_data.status_code
    = detail::narrow<uint16_t>(std::to_underlying(response.code()));
  response_data.headers.reserve(response.header_fields().size());
  for (auto const& [name, value] : response.header_fields()) {
    response_data.headers.emplace_back(name, value);
  }
  return tenzir::http::make_response_record(response_data);
}

auto make_http_response_metadata_record(
  tenzir::http::ResponseData const& response)
  -> record {
  return tenzir::http::make_response_record(response);
}

auto make_metadata(const caf_http::response& r, const uint64_t len)
  -> series_builder {
  auto metadata = make_http_response_metadata_record(r);
  auto sb = series_builder{};
  for (auto i = uint64_t{}; i < len; ++i) {
    sb.data(metadata);
  }
  return sb;
}

auto make_metadata(const caf_http::request& r, const uint64_t len) -> series {
  auto sb = series_builder{};
  for (auto i = uint64_t{}; i < len; ++i) {
    auto rb = sb.record();
    auto hb = rb.field("headers").record();
    r.header().for_each_field([&](std::string_view k, std::string_view v) {
      hb.field(k, v);
    });
    auto qb = rb.field("query").record();
    for (const auto& [k, v] : r.header().query()) {
      qb.field(k, v);
    }
    rb.field("path", r.header().path());
    rb.field("fragment", r.header().fragment());
    rb.field("method", to_string(r.header().method()));
    rb.field("version", r.header().version());
  }
  return sb.finish_assert_one_array();
}

auto add_http_metadata_field(std::optional<ast::field_path> const& metadata_field,
                             record const& metadata, table_slice slice,
                             diagnostic_handler& dh) -> table_slice {
  if (not metadata_field) {
    return slice;
  }
  auto values = series_builder{};
  for (auto i = size_t{}; i < slice.rows(); ++i) {
    values.data(metadata);
  }
  return assign(*metadata_field, values.finish_assert_one_array(),
                std::move(slice), dh);
}

auto add_http_response_field(ast::field_path const& response_field,
                             record const& row, table_slice response_slice,
                             diagnostic_handler& dh) -> table_slice {
  auto base = series_builder{};
  for (auto i = size_t{}; i < response_slice.rows(); ++i) {
    base.data(row);
  }
  return assign(response_field, series{response_slice},
                base.finish_assert_one_slice(), dh);
}

auto make_http_error_slice(record const& row,
                           ast::field_path const& error_field,
                           blob const& body,
                           std::optional<ast::field_path> const& metadata_field,
                           record const& metadata, diagnostic_handler& dh)
  -> table_slice {
  auto base = series_builder{};
  base.data(row);
  auto error = series_builder{};
  error.data(body);
  auto slice = assign(error_field, error.finish_assert_one_array(),
                      base.finish_assert_one_slice(), dh);
  return add_http_metadata_field(metadata_field, metadata, std::move(slice), dh);
}

struct from_http_args {
  tenzir::location op;
  std::optional<expression> filter;
  located<secret> url;
  std::optional<located<std::string>> method;
  std::optional<located<data>> body;
  std::optional<located<std::string>> encode;
  std::optional<located<record>> headers;
  std::optional<ast::field_path> metadata_field;
  std::optional<ast::field_path> error_field;
  std::optional<ast::expression> paginate_expr;
  std::optional<pagination_spec> paginate;
  std::optional<located<duration>> paginate_delay;
  std::optional<located<duration>> connection_timeout;
  std::optional<located<uint64_t>> max_retry_count;
  std::optional<located<duration>> retry_delay;
  std::optional<location> server;
  std::optional<located<record>> responses;
  std::optional<located<uint64_t>> max_request_size;
  std::optional<located<uint64_t>> max_connections;
  tls_options ssl{{.tls_default = false, .is_server = true}};
  std::optional<located<pipeline>> parse;

  auto add_to(argument_parser2& p) {
    p.positional("url", url);
    p.named("method", method);
    p.named("body|payload", body);
    p.named("encode", encode);
    p.named("headers", headers);
    p.named("metadata_field", metadata_field);
    p.named("error_field", error_field);
    p.named("paginate", paginate_expr, "record->string|string");
    p.named("paginate_delay", paginate_delay);
    p.named("connection_timeout", connection_timeout);
    p.named("max_retry_count", max_retry_count);
    p.named("retry_delay", retry_delay);
    p.named("server", server);
    p.named("responses", responses);
    p.named("max_request_size", max_request_size);
    p.named("max_connections", max_connections);
    ssl.add_tls_options(p);
    p.positional("{ … }", parse);
  }

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    if (parse) {
      auto ty = parse->inner.infer_type(tag_v<chunk_ptr>);
      if (not ty) {
        diagnostic::error(ty.error()).primary(*parse).emit(dh);
        return failure::promise();
      }
      if (not ty->is_any<void, table_slice>()) {
        diagnostic::error("pipeline must return events or be a sink")
          .primary(*parse)
          .emit(dh);
        return failure::promise();
      }
    }
    TRY(ssl.validate(dh));
    const auto check_option
      = [&](bool is_server, const auto& x) -> failure_or<void> {
      if (x) {
        if (is_server) {
          diagnostic::error("cannot set client options when using the server")
            .primary(*x)
            .primary(*server)
            .emit(dh);
          return failure::promise();
        }
        diagnostic::error("cannot set server options when using the client")
          .primary(*x)
          .emit(dh);
        return failure::promise();
      }
      return {};
    };
    const auto check_options = [&](bool is_server, const auto&... xs) {
      return (check_option(is_server, xs).is_success() && ...);
    };
    if (server) {
      check_options(true, method, body, encode, headers, error_field, paginate,
                    paginate_delay, connection_timeout, max_retry_count,
                    retry_delay);
      TRY(validate_server_opts(dh));
    } else {
      check_options(false, responses, max_request_size, max_connections);
      TRY(validate_client_opts(dh));
    }
    return {};
  }

  auto validate_client_opts(diagnostic_handler& dh) -> failure_or<void> {
    if (error_field and metadata_field) {
      auto ep = std::views::transform(error_field->path(),
                                      &ast::field_path::segment::id)
                | std::views::transform(&ast::identifier::name);
      auto mp = std::views::transform(metadata_field->path(),
                                      &ast::field_path::segment::id)
                | std::views::transform(&ast::identifier::name);
      auto [ei, mi] = std::ranges::mismatch(ep, mp);
      if (ei == end(ep) or mi == end(mp)) {
        diagnostic::error("`error_field` and `metadata_field` must not "
                          "point to same or nested field")
          .primary(*error_field)
          .primary(*metadata_field)
          .emit(dh);
        return failure::promise();
      }
    }
    if (headers) {
      for (const auto& [_, v] : headers->inner) {
        if (not is<std::string>(v) and not is<secret>(v)) {
          diagnostic::error("header values must be of type `string`")
            .primary(*headers)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    if (body) {
      TRY(match(
        body->inner,
        [](const concepts::one_of<blob, std::string, record> auto&)
          -> failure_or<void> {
          return {};
        },
        [&](const auto&) -> failure_or<void> {
          diagnostic::error("`body` must be `blob`, `record` or `string`")
            .primary(body->source)
            .emit(dh);
          return failure::promise();
        }));
    }
    if (encode) {
      if (not body) {
        diagnostic::error("encoding specified without a `body`")
          .primary(encode->source)
          .emit(dh);
        return failure::promise();
      }
      if (encode->inner != "json" and encode->inner != "form") {
        diagnostic::error("unsupported encoding: `{}`", encode->inner)
          .primary(encode->source)
          .hint("must be `json` or `form`")
          .emit(dh);
        return failure::promise();
      }
    }
    if (method and method->inner.empty()) {
      diagnostic::error("`method` must not be empty").primary(*method).emit(dh);
      return failure::promise();
    }
    if (not make_method()) {
      diagnostic::error("invalid http method: `{}`", method->inner)
        .primary(*method)
        .emit(dh);
      return failure::promise();
    }
    if (retry_delay and retry_delay->inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(*retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay and paginate_delay->inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(*paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout and connection_timeout->inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(*connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (not retry_delay) {
      retry_delay = {1s, location::unknown};
    }
    if (not paginate_delay) {
      paginate_delay = {0s, location::unknown};
    }
    if (not connection_timeout) {
      connection_timeout = {5s, location::unknown};
    }
    if (not max_retry_count) {
      max_retry_count = {0, location::unknown};
    }
    return {};
  }

  auto validate_server_opts(diagnostic_handler& dh) -> failure_or<void> {
    if (max_request_size and max_request_size->inner == 0) {
      diagnostic::error("`max_request_size` must not be zero")
        .primary(max_request_size->source)
        .emit(dh);
      return failure::promise();
    }
    if (max_connections and max_connections->inner == 0) {
      diagnostic::error("`max_connections` must not be zero")
        .primary(max_connections->source)
        .emit(dh);
      return failure::promise();
    }
    if (responses) {
      if (responses->inner.empty()) {
        diagnostic::error("`responses` must not be empty")
          .primary(*responses)
          .emit(dh);
        return failure::promise();
      }
      for (const auto& [k, v] : responses->inner) {
        const auto* rec = try_as<record>(v);
        if (not rec) {
          diagnostic::error("field must be `record`")
            .primary(*responses)
            .emit(dh);
          return failure::promise();
        }
        TRY(has_typed_key<uint64_t>(*rec, "code", responses->source, dh));
        TRY(has_typed_key<std::string>(*rec, "content_type", responses->source,
                                       dh));
        TRY(has_typed_key<std::string>(*rec, "body", responses->source, dh));
        const auto code = as<uint64_t>(rec->at("code"));
        const auto ucode
          = detail::narrow<std::underlying_type_t<caf_http::status>>(code);
        auto status = caf_http::status{};
        if (not caf_http::from_integer(ucode, status)) {
          diagnostic::error("got invalid http status code `{}`", code)
            .primary(responses->source)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    return {};
  }

  template <typename T>
  auto has_typed_key(const record& r, std::string_view name, location loc,
                     diagnostic_handler& dh) -> failure_or<void> {
    const auto key = r.find(name);
    if (key == r.end()) {
      diagnostic::error("`responses` must contain key `{}`", name)
        .primary(loc)
        .emit(dh);
      return failure::promise();
    }
    if (not is<T>(key->second)) {
      diagnostic::error("`{}` must be of type `{}`", name,
                        type::infer(T{})->kind())
        .primary(loc)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  auto make_method() const -> std::optional<caf_http::method> {
    if (not method) {
      return body ? caf_http::method::post : caf_http::method::get;
    }
    auto m = caf_http::method{};
    if (caf_http::from_string(method->inner, m)) {
      return m;
    }
    return std::nullopt;
  }

  auto make_headers() const
    -> std::pair<std::unordered_map<std::string, std::string>,
                 detail::stable_map<std::string, secret>> {
    auto hdrs = std::unordered_map<std::string, std::string>{};
    auto secrets = detail::stable_map<std::string, secret>{};
    auto insert_accept_header = true;
    auto insert_content_type = body and is<record>(body->inner);
    if (headers) {
      for (const auto& [k, v] : headers->inner) {
        if (detail::ascii_icase_equal(k, "accept")) {
          insert_accept_header = false;
        }
        if (detail::ascii_icase_equal(k, "content-type")) {
          insert_content_type = false;
        }
        match(
          v,
          [&](const std::string& x) {
            hdrs.emplace(k, x);
          },
          [&](const secret& x) {
            secrets.emplace(k, x);
          },
          [](const auto&) {
            TENZIR_UNREACHABLE();
          });
      }
    }
    if (insert_content_type) {
      hdrs.emplace("Content-Type", encode and encode->inner == "form"
                                     ? "application/x-www-form-urlencoded"
                                     : "application/json");
    }
    if (insert_accept_header) {
      hdrs.emplace("Accept", "application/json, */*;q=0.5");
    }
    return std::pair{hdrs, secrets};
  }

  auto make_ssl_context(operator_control_plane& ctrl) const
    -> caf::expected<ssl::context> {
    return ssl.make_caf_context(ctrl, std::nullopt);
  }

  auto make_ssl_context(caf::uri uri, operator_control_plane& ctrl) const
    -> caf::expected<ssl::context> {
    return ssl.make_caf_context(ctrl, std::move(uri));
  }

  friend auto inspect(auto& f, from_http_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("filter", x.filter), f.field("url", x.url),
      f.field("method", x.method), f.field("body", x.body),
      f.field("encode", x.encode), f.field("headers", x.headers),
      f.field("metadata_field", x.metadata_field),
      f.field("error_field", x.error_field),
      f.field("paginate_expr", x.paginate_expr),
      f.field("paginate", x.paginate),
      f.field("paginate_delay", x.paginate_delay),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay), f.field("parse", x.parse),
      f.field("server", x.server), f.field("responses", x.responses),
      f.field("ssl", x.ssl), f.field("max_request_size", x.max_request_size),
      f.field("max_connections", x.max_connections));
  }
};

struct inreq_queue_item {
  http_actor actor;
  caf_http::request req;
  table_slice slice;
};

class from_http_server_operator final
  : public crtp_operator<from_http_server_operator> {
public:
  from_http_server_operator() = default;

  from_http_server_operator(from_http_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    auto tdh = transforming_diagnostic_handler{
      dh,
      [](diagnostic diag) {
        return std::move(diag).modify().severity(severity::warning).done();
      },
    };
    auto pull
      = std::optional<caf::async::consumer_resource<caf_http::request>>{};
    auto url = std::string{};
    auto port = uint16_t{};
    auto req = make_secret_request("url", args_.url, url, dh);
    co_yield ctrl.resolve_secrets_must_yield({std::move(req)});
    if (url.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      co_return;
    }
    const auto col = url.rfind(':');
    if (col == 0 or col == std::string::npos) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      co_return;
    }
    const auto* end = url.data() + url.size();
    const auto [ptr, err] = std::from_chars(url.data() + col + 1, end, port);
    if (err != std::errc{}) {
      diagnostic::error("failed to parse port").primary(args_.url).emit(dh);
      co_return;
    }
    if (ptr != end) {
      diagnostic::error("`url` must have the form `<host>:<port>`")
        .primary(args_.url)
        .emit(dh);
      co_return;
    }
    url.resize(col);
    auto server
      = caf_http::with(ctrl.self().system())
          .context(args_.make_ssl_context(ctrl))
          .accept(port, url)
          .monitor(static_cast<exec_node_actor>(&ctrl.self()))
          .max_connections(inner(args_.max_connections).value_or(10))
          .max_request_size(
            inner(args_.max_request_size).value_or(10 * 1024 * 1024))
          .start([&](caf::async::consumer_resource<caf_http::request> cr) {
            TENZIR_ASSERT(not pull);
            pull = std::move(cr);
          });
    if (not server) {
      diagnostic::error("failed to setup http server: {}", server.error())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
      co_return;
    }
    TENZIR_ASSERT(pull);
    auto active = std::deque<inreq_queue_item>{};
    const auto respond = [&](const caf_http::request& r) {
      if (args_.responses) {
        const auto it = args_.responses->inner.find(r.header().path());
        if (it != args_.responses->inner.end()) {
          auto rec = as<record>(it->second);
          auto code = as<uint64_t>(rec["code"]);
          auto ty = as<std::string>(rec["content_type"]);
          auto body = as<std::string>(rec["body"]);
          r.respond(static_cast<caf_http::status>(code), ty, body);
        }
      } else {
        r.respond(caf_http::status::ok, "", "");
      }
    };
    const auto request_slice
      = [&](const http_actor& actor, const caf_http::request& r) {
          ctrl.self()
            .mail(atom::pull_v)
            .request(actor, caf::infinite)
            .then(
              [&, r, actor](table_slice slice) {
                TENZIR_TRACE("[http] pulled slice");
                if (slice.rows() == 0) {
                  respond(r);
                  return;
                }
                if (args_.metadata_field) {
                  slice = assign(*args_.metadata_field,
                                 make_metadata(r, slice.rows()), slice, dh);
                }
                ctrl.set_waiting(false);
                active.emplace_back(actor, r, std::move(slice));
              },
              [&, r](const caf::error& e) {
                TENZIR_TRACE("[http] failed to get slice: {}", e);
                ctrl.set_waiting(false);
                respond(r);
              });
        };
    ctrl.self()
      .make_observable()
      .from_resource(std::move(*pull))
      .for_each([&](const caf_http::request& r) mutable {
        TENZIR_TRACE("[http] handling request with size: {}B",
                     r.body().size_bytes());
        if (r.body().empty()) {
          respond(r);
          return;
        }
        const auto make_chunk = [&] {
          if (auto body = try_decompress_body(
                r.header().field("content-encoding"), r.body(), dh)) {
            return chunk::make(std::move(*body));
          }
          return chunk::copy(r.body());
        };
        auto pipe = make_pipeline(args_.parse, r, args_.op, tdh);
        if (not pipe) {
          return;
        }
        const auto actor = spawn_pipeline(ctrl, std::move(pipe).unwrap(),
                                          args_.filter, make_chunk(), true);
        if (not actor) {
          return;
        }
        request_slice(actor, r);
      });
    while (true) {
      ctrl.set_waiting(true);
      co_yield {};
      // NOTE: We need to handle all, else we might hold onto requests which
      // have already yielded and will not signal `set_waiting(false)` again
      // unless we poll them again.
      while (not active.empty()) {
        auto& front = active.front();
        TENZIR_ASSERT(front.slice.rows() != 0);
        request_slice(front.actor, front.req);
        co_yield std::move(front.slice);
        active.pop_front();
      }
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "from_http_server";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, from_http_server_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  from_http_args args_;
};

struct pagination_request {
  caf::uri uri;
  std::unordered_map<std::string, std::string> headers;
};

auto validate_http_pagination_url(std::string next_url, bool tls_enabled,
                                  location const& op, diagnostic_handler& dh,
                                  severity diag_severity,
                                  std::string_view note = {})
  -> std::optional<caf::uri> {
  tenzir::http::normalize_http_url(next_url, tls_enabled);
  auto next_uri = caf::make_uri(next_url);
  if (next_uri) {
    return *next_uri;
  }
  if (diag_severity == severity::warning) {
    if (note.empty()) {
      diagnostic::warning("failed to parse uri: {}", next_uri.error())
        .primary(op)
        .emit(dh);
    } else {
      diagnostic::warning("failed to parse uri: {}", next_uri.error())
        .primary(op)
        .note("{}", note)
        .emit(dh);
    }
  } else {
    if (note.empty()) {
      diagnostic::error("failed to parse uri: {}", next_uri.error())
        .primary(op)
        .emit(dh);
    } else {
      diagnostic::error("failed to parse uri: {}", next_uri.error())
        .primary(op)
        .note("{}", note)
        .emit(dh);
    }
  }
  return std::nullopt;
}

auto queue_pagination_request(
  std::vector<pagination_request>& queue,
  const std::unordered_map<std::string, std::string>& headers,
  std::string next_url, bool tls_enabled, const location& op,
  diagnostic_handler& dh, severity diag_severity, std::string_view note = {})
  -> bool {
  auto next_uri = validate_http_pagination_url(
    std::move(next_url), tls_enabled, op, dh, diag_severity, note);
  if (not next_uri) {
    return false;
  }
  queue.emplace_back(std::move(*next_uri), headers);
  return true;
}

class from_http_client_operator final
  : public crtp_operator<from_http_client_operator> {
public:
  from_http_client_operator() = default;

  from_http_client_operator(from_http_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    const auto tls_enabled = args_.ssl.get_tls(&ctrl).inner;
    auto& dh = ctrl.diagnostics();
    auto awaiting = uint64_t{};
    auto slices = std::vector<table_slice>{};
    auto paginate_queue = std::vector<pagination_request>{};
    auto reqs = std::vector<secret_request>{};
    auto url = std::string{};
    auto [headers, secrets] = args_.make_headers();
    reqs.emplace_back(make_secret_request("url", args_.url, url, dh));
    if (not secrets.empty()) {
      const auto& loc = args_.headers->source;
      for (auto& [name, secret] : secrets) {
        auto req = secret_request{
          std::move(secret),
          loc,
          [&, name](const resolved_secret_value& x) -> failure_or<void> {
            TRY(auto str, x.utf8_view(name, loc, dh));
            headers.emplace(name, std::string{str});
            return {};
          },
        };
        reqs.emplace_back(std::move(req));
      }
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    if (url.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      co_return;
    }
    tenzir::http::normalize_http_url(url, tls_enabled);
    const auto handle_response = [&](caf::uri uri) {
      return [&, hdrs = headers,
              uri = std::move(uri)](const caf_http::response& r) {
        ctrl.set_waiting(false);
        TENZIR_TRACE("[http] handling response with size: {}B",
                     r.body().size_bytes());
        const auto& headers = r.header_fields();
        const auto eit = std::ranges::find_if(headers, [](const auto& x) {
          return detail::ascii_icase_equal(x.first, "content-encoding");
        });
        const auto encoding
          = eit != std::ranges::end(headers) ? eit->first : "";
        const auto make_chunk = [&] -> chunk_ptr {
          if (auto body = try_decompress_body(encoding, r.body(), dh)) {
            return chunk::make(std::move(*body));
          }
          return chunk::copy(r.body());
        };
        const auto make_blob = [&] -> blob {
          if (auto body = try_decompress_body(encoding, r.body(), dh)) {
            return std::move(*body);
          }
          return blob{r.body()};
        };
        const auto queue_paginate = [&](std::string next_url) {
          std::ignore = queue_pagination_request(
            paginate_queue, hdrs, std::move(next_url), tls_enabled, args_.op,
            ctrl.diagnostics(), severity::error);
        };
        if (const auto code = std::to_underlying(r.code());
            code < 200 or 399 < code) {
          if (not args_.error_field) {
            diagnostic::error("received erroneous http status code: `{}`", code)
              .primary(args_.op)
              .hint("specify `error_field` to keep the event")
              .emit(dh);
            return;
          }
          auto sb = series_builder{};
          std::ignore = sb.record();
          auto error = series_builder{};
          error.data(make_blob());
          auto slice
            = assign(*args_.error_field, error.finish_assert_one_array(),
                     sb.finish_assert_one_slice(), dh);
          if (args_.metadata_field) {
            auto sb = make_metadata(r, slice.rows());
            slice = assign(*args_.metadata_field, sb.finish_assert_one_array(),
                           slice, ctrl.diagnostics());
          }
          slices.push_back(std::move(slice));
          return;
        }
        if (is_link_pagination(args_.paginate)) {
          if (auto url
              = next_url_from_link_headers(args_.paginate, r, uri, dh)) {
            queue_paginate(std::move(*url));
          }
        }
        if (r.body().empty()) {
          --awaiting;
          return;
        }
        auto pipe = make_pipeline(args_.parse, uri, r, args_.op, dh);
        if (not pipe) {
          return;
        }
        const auto actor = spawn_pipeline(ctrl, std::move(pipe).unwrap(),
                                          args_.filter, make_chunk(), false);
        std::invoke(
          [&, &args_ = args_, r, actor, hdrs](this const auto& pull) -> void {
            TENZIR_TRACE("[http] requesting slice");
            ctrl.self()
              .mail(atom::pull_v)
              .request(actor, caf::infinite)
              .then(
                [&, r, pull, actor, hdrs](table_slice slice) {
                  TENZIR_TRACE("[http] pulled slice");
                  ctrl.set_waiting(false);
                  if (slice.rows() == 0) {
                    TENZIR_TRACE("[http] finishing subpipeline");
                    --awaiting;
                    return;
                  }
                  pull();
                  if (args_.metadata_field) {
                    auto sb = make_metadata(r, slice.rows());
                    slice = assign(*args_.metadata_field,
                                   sb.finish_assert_one_array(), slice,
                                   ctrl.diagnostics());
                  }
                  if (auto url
                      = next_url_from_lambda(args_.paginate, slice, dh)) {
                    queue_paginate(std::move(*url));
                  } else {
                    TENZIR_TRACE("[http] done paginating");
                  }
                  slices.push_back(std::move(slice));
                },
                [&](const caf::error& err) {
                  diagnostic::error(err)
                    .note("failed to parse response")
                    .primary(args_.op)
                    .emit(ctrl.diagnostics());
                });
          });
        TENZIR_TRACE("[http] handled response");
      };
    };
    auto uri = caf::make_uri(url);
    if (not uri) {
      diagnostic::error("failed to parse uri: {}", uri.error())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
    }
    auto body = std::string{};
    if (args_.body) {
      match(
        args_.body->inner,
        [&](const blob& x) {
          body.append(reinterpret_cast<const char*>(x.data()), x.size());
        },
        [&](const std::string& x) {
          body = x;
        },
        [&](const record& x) {
          if (args_.encode and args_.encode->inner == "form") {
            body = curl::escape(flatten(x));
            return;
          }
          auto p = json_printer{{}};
          auto it = std::back_inserter(body);
          p.print(it, x);
        },
        [](const auto&) {
          TENZIR_UNREACHABLE();
        });
    }
    caf_http::with(ctrl.self().system())
      .context(args_.make_ssl_context(*uri, ctrl))
      .connect(*uri)
      .max_response_size(max_response_size)
      .connection_timeout(args_.connection_timeout->inner)
      .max_retry_count(args_.max_retry_count->inner)
      .retry_delay(args_.retry_delay->inner)
      .add_header_fields(headers)
      .request(args_.make_method().value(), body)
      .or_else([&](const caf::error& e) {
        diagnostic::error("failed to make http request: {}", e)
          .primary(args_.op)
          .emit(dh);
      })
      .transform([](auto&& x) {
        return std::move(x.first);
      })
      .transform([&](const caf::async::future<caf_http::response>& fut) {
        ++awaiting;
        fut.bind_to(ctrl.self())
          .then(handle_response(std::move(*uri)), [&](const caf::error& e) {
            diagnostic::error("request failed: `{}`", e)
              .primary(args_.op)
              .emit(dh);
          });
      });
    do {
      ctrl.set_waiting(awaiting != 0);
      co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to
      // the observe loop after yielding here, causing the vector's
      // iterator to be invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
      for (auto i = size_t{}; i < paginate_queue.size(); ++i) {
        ++awaiting;
        ctrl.self().run_delayed(
          args_.paginate_delay->inner,
          [&, preq = std::move(paginate_queue[i])] mutable {
            auto& [uri, hdrs] = preq;
            caf_http::with(ctrl.self().system())
              .context(args_.make_ssl_context(uri, ctrl))
              .connect(uri)
              .max_response_size(max_response_size)
              .connection_timeout(args_.connection_timeout->inner)
              .max_retry_count(args_.max_retry_count->inner)
              .retry_delay(args_.retry_delay->inner)
              .add_header_fields(hdrs)
              .get()
              .or_else([&](const caf::error& e) {
                --awaiting;
                ctrl.set_waiting(false);
                diagnostic::error("failed to make http request: {}", e)
                  .primary(args_.op)
                  .emit(dh);
              })
              .transform([](auto&& x) {
                return x.first;
              })
              .transform(
                [&](const caf::async::future<caf_http::response>& fut) {
                  fut.bind_to(ctrl.self())
                    .then(handle_response(std::move(uri)),
                          [&](const caf::error& e) {
                            --awaiting;
                            ctrl.set_waiting(false);
                            diagnostic::error("request failed: `{}`", e)
                              .primary(args_.op)
                              .emit(dh);
                          });
                });
          });
      }
      paginate_queue.clear();
    } while (awaiting != 0);
  }

  auto optimize(expression const& expr, event_order) const
    -> optimize_result override {
    const auto make_copy = [&] -> operator_ptr {
      if (args_.paginate) {
        return copy();
      }
      auto args = args_;
      args.filter = expr;
      return std::make_unique<from_http_client_operator>(std::move(args));
    };
    return {std::nullopt, event_order::ordered, make_copy()};
  }

  auto name() const -> std::string override {
    return "from_http_client";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, from_http_client_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  from_http_args args_;
};

void warn_deprecated_payload(const operator_factory_invocation& inv,
                             session ctx) {
  for (const auto& arg : inv.args) {
    match(
      arg,
      [&](const ast::assignment& arg) {
        const auto* name = try_as<ast::field_path>(arg.left);
        if (name and name->path().size() == 1
            and name->path()[0].id.name == "payload") {
          diagnostic::warning(
            "parameter `payload` is deprecated, use `body` instead")
            .primary(arg.left)
            .emit(ctx);
        }
      },
      [](const auto&) {});
  }
}

struct from_http_compat final : public virtual operator_factory_plugin {
  auto name() const -> std::string override {
    return "tql2.from_http_compat";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.paginate, validate_paginate(std::move(args.paginate_expr), ctx));
    TRY(args.validate(ctx));
    warn_deprecated_payload(inv, ctx);
    // Check if the subpipeline is a sink
    bool subpipeline_is_sink = false;
    if (args.parse) {
      auto ty = args.parse->inner.infer_type(tag_v<chunk_ptr>);
      if (ty and ty->is<void>()) {
        subpipeline_is_sink = true;
      }
    }
    auto op = operator_ptr{};
    if (args.server) {
      op = std::make_unique<from_http_server_operator>(std::move(args));
    } else {
      op = std::make_unique<from_http_client_operator>(std::move(args));
    }
    // If the subpipeline is a sink, the `from_http` parent operator would not
    // never produce any events. It would be effectively a sink. To remain
    // consistent with other parts in the codebase, we explicitly append a
    // discard operator for such scenarios.
    if (subpipeline_is_sink) {
      auto pipe = std::make_unique<pipeline>();
      pipe->append(std::move(op));
      const auto* discard_plugin
        = plugins::find<operator_factory_plugin>("discard");
      TENZIR_ASSERT(discard_plugin);
      TRY(auto discard_op, discard_plugin->make({inv.self, {}}, ctx));
      pipe->append(std::move(discard_op));
      return pipe;
    }
    return op;
  }

  auto load_properties() const -> load_properties_t override {
    return {
      .schemes = {"http", "https"},
      .default_format = plugins::find<operator_factory_plugin>("read_json"),
      .accepts_pipeline = true,
      .events = true,
    };
  }
};

//------------------------------------ http ------------------------------------

struct http_args {
  tenzir::location op;
  ast::expression url;
  std::optional<ast::expression> method;
  std::optional<ast::expression> body;
  std::optional<located<std::string>> encode;
  std::optional<ast::expression> headers;
  std::optional<ast::field_path> response_field;
  std::optional<ast::field_path> metadata_field;
  std::optional<ast::field_path> error_field;
  std::optional<ast::expression> paginate_expr;
  std::optional<pagination_spec> paginate;
  located<duration> paginate_delay{0s, location::unknown};
  located<uint64_t> parallel{1, location::unknown};
  tls_options ssl{{.tls_default = false}};
  located<duration> connection_timeout{5s, location::unknown};
  uint64_t max_retry_count{};
  located<duration> retry_delay{1s, location::unknown};
  std::optional<located<pipeline>> parse;
  std::optional<expression> filter;

  auto add_to(argument_parser2& p) {
    p.positional("url", url, "string");
    p.named("method", method, "string");
    p.named("body|payload", body, "record|string|blob");
    p.named("encode", encode);
    p.named("headers", headers, "record");
    p.named("response_field", response_field);
    p.named("metadata_field", metadata_field);
    p.named("error_field", error_field);
    p.named("paginate", paginate_expr, "record->string|string");
    p.named_optional("paginate_delay", paginate_delay);
    p.named_optional("parallel", parallel);
    ssl.add_tls_options(p);
    p.named_optional("connection_timeout", connection_timeout);
    p.named_optional("max_retry_count", max_retry_count);
    p.named_optional("retry_delay", retry_delay);
    p.positional("{ … }", parse);
  }

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    TENZIR_ASSERT(op);
    if (encode) {
      if (not body) {
        diagnostic::error("encoding specified without a `body`")
          .primary(encode->source)
          .emit(dh);
        return failure::promise();
      }
      if (encode->inner != "json" and encode->inner != "form") {
        diagnostic::error("unsupported encoding: `{}`", encode->inner)
          .primary(encode->source)
          .hint("must be `json` or `form`")
          .emit(dh);
        return failure::promise();
      }
    }
    if (response_field and metadata_field
        and field_paths_overlap(*response_field, *metadata_field)) {
      diagnostic::error("`response_field` and `metadata_field` must not point "
                        "to same or nested field")
        .primary(*response_field)
        .primary(*metadata_field)
        .emit(dh);
      return failure::promise();
    }
    if (error_field and metadata_field
        and field_paths_overlap(*error_field, *metadata_field)) {
      diagnostic::error("`error_field` and `metadata_field` must not point to "
                        "same or nested field")
        .primary(*error_field)
        .primary(*metadata_field)
        .emit(dh);
      return failure::promise();
    }
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (parallel.inner == 0) {
      diagnostic::error("`parallel` must be not be zero")
        .primary(parallel)
        .emit(dh);
      return failure::promise();
    }
    if (parse) {
      auto ty = parse->inner.infer_type(tag_v<chunk_ptr>);
      if (not ty) {
        diagnostic::error(ty.error()).primary(*parse).emit(dh);
        return failure::promise();
      }
      if (not ty->is_any<void, table_slice>()) {
        diagnostic::error("pipeline must return events or be a sink")
          .primary(*parse)
          .emit(dh);
        return failure::promise();
      }
    }
    return {};
  }

  auto make_method(const std::string_view method) const
    -> std::optional<caf_http::method> {
    if (method.empty()) {
      if (not this->method and body) {
        return caf_http::method::post;
      }
      return caf_http::method::get;
    }
    auto m = caf_http::method{};
    if (caf_http::from_string(method, m)) {
      return m;
    }
    return std::nullopt;
  }

  auto make_ssl_context(caf::uri uri, operator_control_plane& ctrl) const
    -> caf::expected<ssl::context> {
    return ssl.make_caf_context(ctrl, std::move(uri));
  }

  friend auto inspect(auto& f, http_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("url", x.url), f.field("method", x.method),
      f.field("body", x.body), f.field("encode", x.encode),
      f.field("headers", x.headers),
      f.field("response_field", x.response_field),
      f.field("metadata_field", x.metadata_field),
      f.field("error_field", x.error_field),
      f.field("paginate_expr", x.paginate_expr),
      f.field("paginate", x.paginate),
      f.field("paginate_delay", x.paginate_delay),
      f.field("parallel", x.parallel), f.field("ssl", x.ssl),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay), f.field("parse", x.parse),
      f.field("filter", x.filter));
  }
};

class http_operator final : public crtp_operator<http_operator> {
public:
  http_operator() = default;

  http_operator(http_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    const auto tls_enabled = args_.ssl.get_tls(&ctrl).inner;
    auto& dh = ctrl.diagnostics();
    auto tdh = transforming_diagnostic_handler{
      dh,
      [](diagnostic diag) {
        return std::move(diag).modify().severity(severity::warning).done();
      },
    };
    // TODO: consider if this should also be extractable
    // NOTE: lambda because context has a deleted copy constructor
    auto awaiting = uint64_t{};
    auto slices = std::vector<table_slice>{};
    auto pagination_queue = std::vector<pagination_request>{};
    const auto handle_response =
      [&](view<record> og, caf::uri uri,
          std::unordered_map<std::string, std::string> hdrs) {
        return [&, hdrs = std::move(hdrs), uri = std::move(uri),
                og = materialize(std::move(og))](const caf_http::response& r) {
          TENZIR_TRACE("[http] handling response with size: {}B",
                       r.body().size_bytes());
          ctrl.set_waiting(false);
          const auto& headers = r.header_fields();
          const auto it = std::ranges::find_if(headers, [](const auto& x) {
            return detail::ascii_icase_equal(x.first, "content-encoding");
          });
          const auto encoding
            = it != std::ranges::end(headers) ? it->first : "";
          const auto make_chunk = [&] -> chunk_ptr {
            if (auto body = try_decompress_body(encoding, r.body(), tdh)) {
              return chunk::make(std::move(*body));
            }
            return chunk::copy(r.body());
          };
          const auto make_blob = [&] -> blob {
            if (auto body = try_decompress_body(encoding, r.body(), tdh)) {
              return std::move(*body);
            }
            return blob{r.body()};
          };
          const auto queue_paginate = [&](std::string next_url) {
            std::ignore = queue_pagination_request(
              pagination_queue, hdrs, std::move(next_url), tls_enabled,
              args_.op, dh, severity::warning, "skipping request");
          };
          if (const auto code = std::to_underlying(r.code());
              code < 200 or 399 < code) {
            --awaiting;
            if (not args_.error_field) {
              diagnostic::warning("received erroneous http status code: `{}`",
                                  code)
                .primary(args_.op)
                .note("skipping response handling")
                .hint("specify `error_field` to keep the event")
                .emit(dh);
              return;
            }
            auto metadata = make_http_response_metadata_record(r);
            auto slice = make_http_error_slice(
              og, *args_.error_field, make_blob(), args_.metadata_field,
              metadata, ctrl.diagnostics());
            slices.push_back(std::move(slice));
            return;
          }
          if (is_link_pagination(args_.paginate)) {
            if (auto url
                = next_url_from_link_headers(args_.paginate, r, uri, tdh)) {
              queue_paginate(std::move(*url));
            }
          }
          if (r.body().empty()) {
            --awaiting;
            return;
          }
          auto p = make_pipeline(args_.parse, uri, r, args_.op, tdh);
          if (not p) {
            --awaiting;
            return;
          }
          const auto actor
            = spawn_pipeline(ctrl, *p, args_.filter, make_chunk(), true);
          std::invoke([&, &args_ = args_, r, og, hdrs,
                       actor](this const auto& pull) -> void {
            TENZIR_TRACE("[http] requesting slice");
            ctrl.self()
              .mail(atom::pull_v)
              .request(actor, caf::infinite)
              .then(
                [&, r, hdrs, pull, og, actor](table_slice slice) {
                  TENZIR_TRACE("[http] pulled slice");
                  ctrl.set_waiting(false);
                  if (slice.rows() == 0) {
                    TENZIR_TRACE("[http] finishing subpipeline");
                    --awaiting;
                    return;
                  }
                  pull();
                  if (args_.response_field) {
                    slice = add_http_response_field(*args_.response_field, og,
                                                    std::move(slice), tdh);
                  }
                  if (args_.metadata_field) {
                    auto metadata = make_http_response_metadata_record(r);
                    slice = add_http_metadata_field(args_.metadata_field,
                                                    metadata, std::move(slice),
                                                    ctrl.diagnostics());
                  }
                  if (auto url
                      = next_url_from_lambda(args_.paginate, slice, tdh)) {
                    queue_paginate(std::move(*url));
                  } else {
                    TENZIR_TRACE("[http] done paginating");
                  }
                  slices.push_back(std::move(slice));
                },
                [&](const caf::error& err) {
                  --awaiting;
                  ctrl.set_waiting(false);
                  diagnostic::warning(err)
                    .note("failed to parse response")
                    .primary(args_.op)
                    .emit(ctrl.diagnostics());
                });
          });
          TENZIR_TRACE("[http] handled response");
        };
      };
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto urls = std::vector<std::string>{};
      auto reqs = std::vector<secret_request>{};
      evaluate_http_urls(args_.url, slice, reqs, urls, dh);
      auto hdrs = std::vector<header_evaluation>{};
      evaluate_http_headers(args_.headers, args_.body.has_value(), slice, reqs,
                            hdrs, dh);
      if (not reqs.empty()) {
        co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
      }
      TENZIR_ASSERT(urls.size() == slice.rows());
      TENZIR_ASSERT(hdrs.size() == slice.rows());
      auto url_it = urls.begin();
      auto hdr_it = hdrs.begin();
      auto methods = eval_http_optional_string(args_.method, slice, dh);
      auto bodies = eval_http_body(args_.body, args_.encode, slice, dh);
      for (auto row : slice.values()) {
        auto& url = *url_it++;
        auto& hdr = *hdr_it++;
        auto& headers = hdr.values;
        const auto method = methods.next().value();
        const auto [body, insert_content_type] = bodies.next().value();
        if (url.empty()) {
          diagnostic::warning("`url` must not be empty")
            .primary(args_.url)
            .note("skipping request")
            .emit(dh);
          continue;
        }
        tenzir::http::normalize_http_url(url, tls_enabled);
        const auto m = args_.make_method(method);
        if (not m) {
          diagnostic::warning("invalid http method: `{}`", method)
            .primary(args_.method.value())
            .emit(dh);
          continue;
        }
        auto caf_uri = caf::make_uri(url);
        if (not caf_uri) {
          diagnostic::warning("failed to parse uri: {}", caf_uri.error())
            .primary(args_.op)
            .emit(dh);
          continue;
        }
        if (insert_content_type and not hdr.has_content_type) {
          headers.emplace("Content-Type",
                          args_.encode and args_.encode->inner == "form"
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
        }
        if (not hdr.has_accept_header) {
          headers.emplace("Accept", "application/json, */*;q=0.5");
        }
        caf_http::with(ctrl.self().system())
          .context(args_.make_ssl_context(*caf_uri, ctrl))
          .connect(*caf_uri)
          .max_response_size(max_response_size)
          .connection_timeout(args_.connection_timeout.inner)
          .max_retry_count(args_.max_retry_count)
          .retry_delay(args_.retry_delay.inner)
          .add_header_fields(headers)
          .request(*m, body)
          .or_else([&](const caf::error& e) {
            diagnostic::warning("failed to make http request: {}", e)
              .primary(args_.op)
              .emit(dh);
          })
          .transform([](auto&& x) {
            return std::move(x.first);
          })
          .transform([&](const caf::async::future<caf_http::response>& fut) {
            ++awaiting;
            fut.bind_to(ctrl.self())
              .then(handle_response(row, std::move(*caf_uri),
                                    std::move(headers)),
                    [&](const caf::error& e) {
                      --awaiting;
                      ctrl.set_waiting(false);
                      diagnostic::warning("request failed: `{}`", e)
                        .primary(args_.op)
                        .emit(dh);
                    });
          });
        while (awaiting >= args_.parallel.inner) {
          // NOTE: Must be an index-based loop. The thread can go back to the
          // observe loop after yielding here, causing the vector's iterator to
          // be invalidated.
          for (auto i = size_t{}; i < slices.size(); ++i) {
            co_yield slices[i];
          }
          slices.clear();
          ctrl.set_waiting(awaiting != 0);
          co_yield {};
        }
        // NOTE: Must be an index-based loop. The thread can go back to the
        // observe loop after yielding here, causing the vector's iterator to
        // be invalidated.
        for (auto i = size_t{}; i < pagination_queue.size(); ++i) {
          ++awaiting;
          ctrl.self().run_delayed(
            args_.paginate_delay.inner,
            [&, preq = std::move(pagination_queue[i])] mutable {
              auto& [uri, hdrs] = preq;
              caf_http::with(ctrl.self().system())
                .context(args_.make_ssl_context(uri, ctrl))
                .connect(uri)
                .max_response_size(max_response_size)
                .connection_timeout(args_.connection_timeout.inner)
                .max_retry_count(args_.max_retry_count)
                .retry_delay(args_.retry_delay.inner)
                .add_header_fields(hdrs)
                .get()
                .or_else([&](const caf::error& e) {
                  --awaiting;
                  diagnostic::warning("failed to make http request: {}", e)
                    .primary(args_.op)
                    .emit(dh);
                })
                .transform([](auto&& x) {
                  return x.first;
                })
                .transform(
                  [&](const caf::async::future<caf_http::response>& fut) {
                    fut.bind_to(ctrl.self())
                      .then(handle_response(row, std::move(uri),
                                            std::move(hdrs)),
                            [&](const caf::error& e) {
                              --awaiting;
                              ctrl.set_waiting(false);
                              diagnostic::warning("request failed: `{}`", e)
                                .primary(args_.op)
                                .emit(dh);
                            });
                  });
            });
          while (awaiting >= args_.parallel.inner) {
            // NOTE: Must be an index-based loop. The thread can go back to the
            // observe loop after yielding here, causing the vector's iterator
            // to be invalidated.
            for (auto i = size_t{}; i < slices.size(); ++i) {
              co_yield slices[i];
            }
            slices.clear();
            ctrl.set_waiting(true);
            co_yield {};
          }
        }
        pagination_queue.clear();
      }
    }
    do {
      ctrl.set_waiting(awaiting != 0);
      co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to the
      // observe loop after yielding here, causing the vector's iterator to
      // be invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
    } while (awaiting != 0);
    TENZIR_ASSERT(pagination_queue.empty());
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& expr, event_order) const
    -> optimize_result override {
    const auto make_copy = [&] -> operator_ptr {
      if (not args_.paginate) {
        auto args = args_;
        args.filter = expr;
        return std::make_unique<http_operator>(std::move(args));
      }
      return copy();
    };
    return {
      std::nullopt,
      args_.parallel.inner == 1 ? event_order::ordered : event_order::unordered,
      make_copy(),
    };
  }

  auto name() const -> std::string override {
    return "tql2.http";
  }

  friend auto inspect(auto& f, http_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  http_args args_;
};

constexpr auto http_message_queue_capacity = uint32_t{1024};

struct http_executor_args {
  location op = location::unknown;
  ast::expression url;
  std::optional<ast::expression> method;
  std::optional<ast::expression> body;
  std::optional<located<std::string>> encode;
  std::optional<ast::expression> headers;
  std::optional<ast::field_path> response_field;
  std::optional<ast::field_path> metadata_field;
  std::optional<ast::field_path> error_field;
  std::optional<ast::expression> paginate_expr;
  std::optional<pagination_spec> paginate;
  located<duration> paginate_delay{0s, location::unknown};
  located<uint64_t> parallel{1, location::unknown};
  std::optional<located<data>> tls;
  located<duration> connection_timeout{5s, location::unknown};
  located<uint64_t> max_retry_count{0, location::unknown};
  located<duration> retry_delay{1s, location::unknown};
  std::optional<located<ir::pipeline>> parse;
  let_id request_let;
  let_id response_let;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (encode) {
      if (not body) {
        diagnostic::error("encoding specified without a `body`")
          .primary(encode->source)
          .emit(dh);
        return failure::promise();
      }
      if (encode->inner != "json" and encode->inner != "form") {
        diagnostic::error("unsupported encoding: `{}`", encode->inner)
          .primary(encode->source)
          .hint("must be `json` or `form`")
          .emit(dh);
        return failure::promise();
      }
    }
    if (response_field and metadata_field
        and field_paths_overlap(*response_field, *metadata_field)) {
      diagnostic::error("`response_field` and `metadata_field` must not point "
                        "to same or nested field")
        .primary(*response_field)
        .primary(*metadata_field)
        .emit(dh);
      return failure::promise();
    }
    if (error_field and metadata_field
        and field_paths_overlap(*error_field, *metadata_field)) {
      diagnostic::error("`error_field` and `metadata_field` must not point to "
                        "same or nested field")
        .primary(*error_field)
        .primary(*metadata_field)
        .emit(dh);
      return failure::promise();
    }
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (parallel.inner == 0) {
      diagnostic::error("`parallel` must be not be zero")
        .primary(parallel)
        .emit(dh);
      return failure::promise();
    }
    if (parse) {
      auto output = parse->inner.infer_type(tag_v<chunk_ptr>, dh);
      if (not output) {
        return failure::promise();
      }
      if (*output and not(*output)->is_any<void, table_slice>()) {
        diagnostic::error("pipeline must return events or be a sink")
          .primary(*parse)
          .emit(dh);
        return failure::promise();
      }
    }
    auto tls_opts = tls ? tls_options{*tls, {.tls_default = false}}
                        : tls_options{{.tls_default = false}};
    TRY(tls_opts.validate(dh));
    return {};
  }

  auto make_method(std::string_view method_name) const
    -> std::optional<std::string> {
    if (method_name.empty()) {
      if (not method and body) {
        return std::string{"POST"};
      }
      return std::string{"GET"};
    }
    return tenzir::http::normalize_http_method(method_name);
  }

  friend auto inspect(auto& f, http_executor_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("url", x.url), f.field("method", x.method),
      f.field("body", x.body), f.field("encode", x.encode),
      f.field("headers", x.headers),
      f.field("response_field", x.response_field),
      f.field("metadata_field", x.metadata_field),
      f.field("error_field", x.error_field),
      f.field("paginate_expr", x.paginate_expr),
      f.field("paginate", x.paginate),
      f.field("paginate_delay", x.paginate_delay),
      f.field("parallel", x.parallel), f.field("tls", x.tls),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay), f.field("parse", x.parse),
      f.field("request_let", x.request_let),
      f.field("response_let", x.response_let));
  }
};

class http_executor_operator final : public Operator<table_slice, table_slice> {
public:
  struct http_response_message {
    uint64_t request_id;
    tenzir::http::HttpResult<tenzir::http::ResponseData> result;
  };

  struct sub_slice_message {
    uint64_t sub_key;
    table_slice slice;
  };

  struct sub_finished_message {
    uint64_t sub_key;
  };

  using message
    = variant<http_response_message, sub_slice_message, sub_finished_message>;
  using queued_message = Box<message>;
  using message_queue = folly::coro::BoundedQueue<queued_message>;

  explicit http_executor_operator(http_executor_args args)
    : args_{std::move(args)},
      request_let_id_{args_.request_let},
      response_let_id_{args_.response_let} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    if (lifecycle_ != lifecycle::running or input.rows() == 0) {
      co_return;
    }
    auto& dh = ctx.dh();
    if (not ensure_paginate_spec(dh)) {
      lifecycle_ = lifecycle::done;
      co_return;
    }
    auto urls = std::vector<std::string>{};
    auto requests = std::vector<secret_request>{};
    evaluate_http_urls(args_.url, input, requests, urls, dh);
    auto headers = std::vector<header_evaluation>{};
    evaluate_http_headers(args_.headers, args_.body.has_value(), input,
                          requests, headers, dh);
    if (not requests.empty()) {
      auto resolved = co_await ctx.resolve_secrets(std::move(requests));
      if (not resolved) {
        lifecycle_ = lifecycle::done;
        co_return;
      }
    }
    TENZIR_ASSERT(urls.size() == input.rows());
    TENZIR_ASSERT(headers.size() == input.rows());
    auto url_it = urls.begin();
    auto header_it = headers.begin();
    auto methods = eval_http_optional_string(args_.method, input, dh);
    auto bodies = eval_http_body(args_.body, args_.encode, input, dh);
    for (auto row_view : input.values()) {
      auto url = std::move(*url_it++);
      auto header = std::move(*header_it++);
      auto method_name = methods.next().value();
      auto [body_view, insert_content_type] = bodies.next().value();
      if (url.empty()) {
        diagnostic::warning("`url` must not be empty")
          .primary(args_.url)
          .note("skipping request")
          .emit(dh);
        continue;
      }
      auto method = args_.make_method(method_name);
      if (not method) {
        auto method_location = args_.method ? args_.method->get_location()
                                            : args_.url.get_location();
        diagnostic::warning("invalid http method: `{}`", method_name)
          .primary(method_location)
          .note("skipping request")
          .emit(dh);
        continue;
      }
      if (insert_content_type and not header.has_content_type) {
        header.values.emplace(
          "Content-Type", args_.encode and args_.encode->inner == "form"
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
      }
      if (not header.has_accept_header) {
        header.values.emplace("Accept", "application/json, */*;q=0.5");
      }
      auto tls_default = tenzir::http::infer_tls_default(url);
      auto tls_opts = args_.tls
                        ? tls_options{*args_.tls, {.tls_default = tls_default}}
                        : tls_options{{.tls_default = tls_default}};
      tenzir::http::normalize_http_url(url, tls_opts.get_tls(nullptr).inner);
      if (not tls_opts.validate(url, args_.url.get_location(), dh)) {
        continue;
      }
      auto ssl_result = tls_opts.make_folly_ssl_context(dh);
      if (not ssl_result) {
        continue;
      }
      auto request = request_state{
        .id = next_request_id_++,
        .row = materialize(row_view),
        .url = std::move(url),
        .headers = std::move(header.values),
        .method = std::move(*method),
        .body = std::string{body_view},
        .ssl_context = std::move(*ssl_result),
        .tls_enabled = tls_opts.get_tls(nullptr).inner,
      };
      pending_.push_back(std::move(request));
    }
    launch_ready(ctx);
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto msg = std::move(result).as<queued_message>();
    co_await std::move(*msg).template match<Task<void>>(
      [&](http_response_message response) -> Task<void> {
        co_await handle_http_response(std::move(response), push, ctx);
      },
      [&](sub_slice_message slice) -> Task<void> {
        co_await handle_sub_slice(std::move(slice), push, ctx);
      },
      [&](sub_finished_message finished) -> Task<void> {
        handle_sub_finished(std::move(finished), ctx);
        co_return;
      });
    maybe_finish_draining();
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(push, ctx);
    if (slice.rows() == 0) {
      co_return;
    }
    auto data = materialize(key);
    auto* sub_key = try_as<int64_t>(&data);
    TENZIR_ASSERT(sub_key);
    co_await message_queue_->enqueue(queued_message{message{sub_slice_message{
      detail::narrow<uint64_t>(*sub_key),
      std::move(slice),
    }}});
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto data = materialize(key);
    auto* sub_key = try_as<int64_t>(&data);
    TENZIR_ASSERT(sub_key);
    co_await message_queue_->enqueue(
      queued_message{message{sub_finished_message{
        detail::narrow<uint64_t>(*sub_key),
      }}});
  }

  auto finalize(Push<table_slice>&, OpCtx&) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    lifecycle_ = lifecycle::draining;
    maybe_finish_draining();
    co_return lifecycle_ == lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  enum class lifecycle { running, draining, done };

  struct request_state {
    uint64_t id = 0;
    record row;
    std::string url;
    std::unordered_map<std::string, std::string> headers;
    std::string method;
    std::string body;
    std::shared_ptr<folly::SSLContext> ssl_context;
    bool tls_enabled = false;
    bool is_pagination = false;
  };

  struct active_subpipeline {
    record row;
    record response_metadata;
    std::unordered_map<std::string, std::string> headers;
    std::shared_ptr<folly::SSLContext> ssl_context;
    bool tls_enabled = false;
  };

  auto ensure_paginate_spec(diagnostic_handler& dh) -> bool {
    if (paginate_initialized_) {
      return true;
    }
    paginate_initialized_ = true;
    if (args_.paginate or not args_.paginate_expr) {
      return true;
    }
    auto paginate = validate_paginate(std::move(args_.paginate_expr), dh);
    if (not paginate) {
      return false;
    }
    args_.paginate = std::move(*paginate);
    return true;
  }

  static auto run_request_task(Arc<message_queue> queue,
                               std::chrono::milliseconds connect_timeout,
                               std::chrono::milliseconds retry_delay,
                               uint64_t retry_count,
                               std::chrono::milliseconds paginate_delay,
                               request_state request) -> Task<void> {
    if (request.is_pagination
        and paginate_delay > std::chrono::milliseconds::zero()) {
      co_await folly::coro::sleep(
        std::chrono::duration_cast<folly::HighResDuration>(paginate_delay));
    }
    auto config = tenzir::http::ClientRequestConfig{
      .url = request.url,
      .method = request.method,
      .body = request.body,
      .headers = request.headers,
      .connect_timeout = connect_timeout,
      .ssl_context = request.ssl_context,
    };
    auto result = co_await tenzir::http::send_request_with_retries(
      std::move(config), retry_count, retry_delay);
    co_await queue->enqueue(queued_message{message{http_response_message{
      request.id,
      std::move(result),
    }}});
  }

  auto launch_ready(OpCtx& ctx) -> void {
    while (active_count_ < args_.parallel.inner and not pending_.empty()) {
      auto request = std::move(pending_.front());
      pending_.pop_front();
      auto request_id = request.id;
      auto inserted = active_requests_.emplace(request_id, std::move(request));
      TENZIR_ASSERT(inserted.second);
      auto task_request = inserted.first->second;
      auto queue = message_queue_;
      auto connect_timeout = to_chrono(args_.connection_timeout.inner);
      auto retry_delay = to_chrono(args_.retry_delay.inner);
      auto retry_count = args_.max_retry_count.inner;
      auto paginate_delay = to_chrono(args_.paginate_delay.inner);
      ++active_count_;
      ctx.spawn_task(run_request_task(queue, connect_timeout, retry_delay,
                                      retry_count, paginate_delay,
                                      std::move(task_request)));
    }
  }

  auto finish_request(uint64_t request_id, OpCtx& ctx) -> void {
    auto erased = active_requests_.erase(request_id);
    TENZIR_ASSERT(erased == 1);
    TENZIR_ASSERT(active_count_ > 0);
    --active_count_;
    launch_ready(ctx);
  }

  auto queue_paginate(request_state const& request, std::string next_url,
                      diagnostic_handler& dh) -> void {
    auto next_uri = validate_http_pagination_url(
      std::move(next_url), request.tls_enabled, args_.op, dh, severity::warning,
      "skipping request");
    if (not next_uri) {
      return;
    }
    pending_.push_back(request_state{
      .id = next_request_id_++,
      .row = request.row,
      .url = std::string{next_uri->str()},
      .headers = request.headers,
      .method = "GET",
      .body = {},
      .ssl_context = request.ssl_context,
      .tls_enabled = request.tls_enabled,
      .is_pagination = true,
    });
  }

  auto handle_http_response(http_response_message response,
                            Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> {
    auto request_it = active_requests_.find(response.request_id);
    if (request_it == active_requests_.end()) {
      co_return;
    }
    auto request = request_it->second;
    if (response.result.is_err()) {
      diagnostic::warning("{}", std::move(response.result).unwrap_err())
        .primary(args_.op)
        .emit(ctx);
      finish_request(request.id, ctx);
      co_return;
    }
    auto response_data = std::move(response.result).unwrap();
    auto response_metadata = make_http_response_metadata_record(response_data);
    if (auto const code = response_data.status_code; code < 200 or code > 399) {
      if (not args_.error_field) {
        diagnostic::warning("received erroneous http status code: `{}`", code)
          .primary(args_.op)
          .note("skipping response handling")
          .hint("specify `error_field` to keep the event")
          .emit(ctx);
      } else {
        auto slice = make_http_error_slice(
          request.row, *args_.error_field, response_data.body,
          args_.metadata_field, response_metadata, ctx.dh());
        co_await push(std::move(slice));
      }
      finish_request(request.id, ctx);
      co_return;
    }
    if (is_link_pagination(args_.paginate)) {
      auto request_uri = caf::make_uri(request.url);
      if (request_uri) {
        if (auto next_url = tenzir::http::next_url_from_link_headers(
              response_data, request.url,
              std::optional<location>{args_.paginate->source}, ctx.dh())) {
          queue_paginate(request, std::move(*next_url), ctx.dh());
        }
      }
    }
    if (response_data.body.empty()) {
      finish_request(request.id, ctx);
      co_return;
    }
    auto request_uri = caf::make_uri(request.url);
    if (not request_uri) {
      diagnostic::warning("failed to parse uri: {}", request_uri.error())
        .primary(args_.op)
        .emit(ctx);
      finish_request(request.id, ctx);
      co_return;
    }
    auto pipeline_result
      = make_ir_pipeline(args_.parse, *request_uri, response_data, args_.op,
                         ctx.dh());
    if (not pipeline_result) {
      finish_request(request.id, ctx);
      co_return;
    }
    auto payload = chunk_ptr{};
    auto body = std::move(response_data.body);
    auto body_view = std::span<std::byte const>{body.data(), body.size()};
    auto encoding = tenzir::http::find_header_value(response_data.headers,
                                                    "content-encoding");
    if (auto decompressed = try_decompress_body(encoding, body_view, ctx.dh())) {
      payload = chunk::make(std::move(*decompressed));
    } else {
      payload = chunk::make(std::move(body));
    }
    auto pipeline = std::move(*pipeline_result);
    if (args_.parse) {
      auto env = substitute_ctx::env_t{};
      env[request_let_id_] = request.row;
      env[response_let_id_] = response_metadata;
      auto reg = global_registry();
      auto b_ctx = base_ctx{ctx, *reg};
      if (not pipeline.inner.substitute(substitute_ctx{b_ctx, &env}, true)) {
        finish_request(request.id, ctx);
        co_return;
      }
    }
    auto sub_key = next_sub_key_++;
    active_subpipelines_.emplace(sub_key, active_subpipeline{
                                          .row = request.row,
                                          .response_metadata
                                          = std::move(response_metadata),
                                          .headers = std::move(request.headers),
                                          .ssl_context = request.ssl_context,
                                          .tls_enabled = request.tls_enabled,
                                        });
    active_requests_.erase(request_it);
    auto sub
      = co_await ctx.spawn_sub(data{int64_t{static_cast<int64_t>(sub_key)}},
                               std::move(pipeline.inner),
                               tag_v<chunk_ptr>);
    auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
    auto push_result = co_await open_pipeline.push(std::move(payload));
    if (push_result.is_err()) {
      active_subpipelines_.erase(sub_key);
      --active_count_;
      launch_ready(ctx);
      co_return;
    }
    co_await open_pipeline.close();
    co_return;
  }

  auto handle_sub_slice(sub_slice_message slice_msg, Push<table_slice>& push,
                        OpCtx& ctx) -> Task<void> {
    auto sub_it = active_subpipelines_.find(slice_msg.sub_key);
    if (sub_it == active_subpipelines_.end()) {
      co_return;
    }
    auto slice = std::move(slice_msg.slice);
    if (args_.response_field) {
      slice = add_http_response_field(*args_.response_field, sub_it->second.row,
                                      std::move(slice), ctx.dh());
    }
    slice = add_http_metadata_field(args_.metadata_field,
                                    sub_it->second.response_metadata,
                                    std::move(slice), ctx.dh());
    if (auto next_url = next_url_from_lambda(args_.paginate, slice, ctx.dh())) {
      queue_paginate(sub_it->second, std::move(*next_url), ctx.dh());
    }
    co_await push(std::move(slice));
    co_return;
  }

  auto handle_sub_finished(sub_finished_message finished, OpCtx& ctx) -> void {
    auto sub_it = active_subpipelines_.find(finished.sub_key);
    if (sub_it == active_subpipelines_.end()) {
      return;
    }
    active_subpipelines_.erase(sub_it);
    TENZIR_ASSERT(active_count_ > 0);
    --active_count_;
    launch_ready(ctx);
  }

  auto queue_paginate(active_subpipeline const& sub, std::string next_url,
                      diagnostic_handler& dh) -> void {
    auto next_uri = validate_http_pagination_url(
      std::move(next_url), sub.tls_enabled, args_.op, dh, severity::warning,
      "skipping request");
    if (not next_uri) {
      return;
    }
    pending_.push_back(request_state{
      .id = next_request_id_++,
      .row = sub.row,
      .url = std::string{next_uri->str()},
      .headers = sub.headers,
      .method = "GET",
      .body = {},
      .ssl_context = sub.ssl_context,
      .tls_enabled = sub.tls_enabled,
      .is_pagination = true,
    });
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ == lifecycle::draining and active_count_ == 0
        and pending_.empty()) {
      lifecycle_ = lifecycle::done;
    }
  }

  http_executor_args args_;
  let_id request_let_id_;
  let_id response_let_id_;
  mutable Arc<message_queue> message_queue_{std::in_place,
                                            http_message_queue_capacity};
  std::deque<request_state> pending_;
  std::unordered_map<uint64_t, request_state> active_requests_;
  std::unordered_map<uint64_t, active_subpipeline> active_subpipelines_;
  lifecycle lifecycle_ = lifecycle::running;
  uint64_t next_request_id_ = 0;
  uint64_t next_sub_key_ = 0;
  uint64_t active_count_ = 0;
  bool paginate_initialized_ = false;
};

auto make_http_executor_description() -> Description {
  auto d = Describer<http_executor_args, http_executor_operator>{};
  d.operator_location(&http_executor_args::op);
  auto url = d.positional("url", &http_executor_args::url, "string");
  auto method = d.named("method", &http_executor_args::method, "string");
  auto body
    = d.named("body", &http_executor_args::body, "record|string|blob");
  auto payload
    = d.named("payload", &http_executor_args::body, "record|string|blob");
  auto encode = d.named("encode", &http_executor_args::encode);
  auto headers = d.named("headers", &http_executor_args::headers, "record");
  auto response_field
    = d.named("response_field", &http_executor_args::response_field);
  auto metadata_field
    = d.named("metadata_field", &http_executor_args::metadata_field);
  auto error_field
    = d.named("error_field", &http_executor_args::error_field);
  auto paginate = d.named("paginate", &http_executor_args::paginate_expr,
                          "record->string|string");
  auto paginate_delay
    = d.named_optional("paginate_delay", &http_executor_args::paginate_delay);
  auto parallel = d.named_optional("parallel", &http_executor_args::parallel);
  auto tls = d.named("tls", &http_executor_args::tls);
  auto connection_timeout = d.named_optional(
    "connection_timeout", &http_executor_args::connection_timeout);
  auto max_retry_count
    = d.named_optional("max_retry_count", &http_executor_args::max_retry_count);
  auto retry_delay
    = d.named_optional("retry_delay", &http_executor_args::retry_delay);
  auto parse = d.pipeline(&http_executor_args::parse,
                          {{"request", &http_executor_args::request_let},
                           {"response", &http_executor_args::response_let}});
  const auto build_args = [=](DescribeCtx& ctx)
    -> failure_or<http_executor_args> {
    auto args = http_executor_args{};
    args.op = ctx.get_location(url).value_or(location::unknown);
    if (auto x = ctx.get(url)) {
      args.url = *x;
    }
    if (auto x = ctx.get(method)) {
      args.method = *x;
    }
    if (auto x = ctx.get(body)) {
      args.body = *x;
    } else if (auto x = ctx.get(payload)) {
      args.body = *x;
    }
    if (auto x = ctx.get(encode)) {
      args.encode = *x;
    }
    if (auto x = ctx.get(headers)) {
      args.headers = *x;
    }
    if (auto x = ctx.get(response_field)) {
      args.response_field = *x;
    }
    if (auto x = ctx.get(metadata_field)) {
      args.metadata_field = *x;
    }
    if (auto x = ctx.get(error_field)) {
      args.error_field = *x;
    }
    if (auto x = ctx.get(paginate)) {
      args.paginate_expr = *x;
    }
    if (auto x = ctx.get(paginate_delay)) {
      args.paginate_delay = *x;
    }
    if (auto x = ctx.get(parallel)) {
      args.parallel = *x;
    }
    if (auto x = ctx.get(tls)) {
      args.tls = *x;
    }
    if (auto x = ctx.get(connection_timeout)) {
      args.connection_timeout = *x;
    }
    if (auto x = ctx.get(max_retry_count)) {
      args.max_retry_count = *x;
    }
    if (auto x = ctx.get(retry_delay)) {
      args.retry_delay = *x;
    }
    if (auto x = ctx.get(parse)) {
      args.parse = *x;
    }
    if (args.paginate_expr) {
      TRY(auto paginate, validate_paginate(std::move(args.paginate_expr), ctx));
      args.paginate = std::move(paginate);
    }
    TRY(args.validate(ctx));
    return args;
  };
  d.validate([build_args](DescribeCtx& ctx) -> Empty {
    std::ignore = build_args(ctx);
    return {};
  });
  d.spawner([build_args]<class Input>(DescribeCtx& ctx)
              -> failure_or<Option<SpawnWith<http_executor_args, Input>>> {
    if constexpr (not std::same_as<Input, table_slice>) {
      return {};
    } else {
      TRY(auto args, build_args(ctx));
      return [](http_executor_args args) {
        return http_executor_operator{std::move(args)};
      };
    }
  });
  return d.without_optimize();
}

struct HttpPlugin final : public virtual operator_plugin2<http_operator>,
                          public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.http";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.paginate, validate_paginate(std::move(args.paginate_expr), ctx));
    TRY(args.validate(ctx));
    warn_deprecated_payload(inv, ctx);
    return std::make_unique<http_operator>(std::move(args));
  }

  auto describe() const -> Description override {
    return make_http_executor_description();
  }
};

using internal_source_plugin = operator_inspection_plugin<internal_source>;
using internal_sink_plugin = operator_inspection_plugin<internal_sink>;
using from_http_client = operator_inspection_plugin<from_http_client_operator>;
using from_http_server = operator_inspection_plugin<from_http_server_operator>;

} // namespace
} // namespace tenzir::plugins::http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http_client)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http_server)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http_compat)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::HttpPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::internal_source_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::internal_sink_plugin)
