//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/pipeline_executor.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <arrow/util/compression.h>
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
#include <fmt/format.h>

#include <charconv>
#include <ranges>
#include <unordered_map>
#include <utility>

constexpr auto max_response_size = std::numeric_limits<int32_t>::max();

namespace tenzir::plugins::http {
namespace {

namespace http = caf::net::http;
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
  if (encoding.empty()) {
    return std::nullopt;
  }
  const auto compression_type
    = arrow::util::Codec::GetCompressionType(std::string{encoding});
  if (not compression_type.ok()) {
    diagnostic::warning("invalid compression type: {}", encoding)
      .hint("must be one of `brotli`, `bz2`, `gzip`, `lz4`, `zstd`")
      .note("skipping decompression")
      .emit(dh);
    return std::nullopt;
  }
  auto out = blob{};
  out.resize(body.size_bytes() * 2);
  const auto codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  TENZIR_ASSERT(codec.ok());
  if (not codec.ValueUnsafe()) {
    return std::nullopt;
  }
  const auto decompressor = check(codec.ValueUnsafe()->MakeDecompressor());
  auto written = size_t{};
  auto read = size_t{};
  while (read != body.size_bytes()) {
    const auto result = decompressor->Decompress(
      detail::narrow<int64_t>(body.size_bytes() - read),
      reinterpret_cast<const uint8_t*>(body.data() + read),
      detail::narrow<int64_t>(out.size() - written),
      reinterpret_cast<uint8_t*>(out.data() + written));
    if (not result.ok()) {
      diagnostic::warning("failed to decompress: {}",
                          result.status().ToString())
        .note("emitting compressed body")
        .emit(dh);
      return std::nullopt;
    }
    TENZIR_ASSERT(std::cmp_less_equal(result->bytes_written, out.size()));
    written += result->bytes_written;
    read += result->bytes_read;
    if (result->need_more_output) {
      if (out.size() == out.max_size()) [[unlikely]] {
        diagnostic::error("failed to resize buffer").emit(dh);
        return std::nullopt;
      }
      if (out.size() < out.max_size() / 2) {
        out.resize(out.size() * 2);
      } else [[unlikely]] {
        out.resize(out.max_size());
      }
    }
    // In case the input contains multiple concatenated compressed streams,
    // we gracefully reset the decompressor.
    if (decompressor->IsFinished()) {
      const auto result = decompressor->Reset();
      if (not result.ok()) {
        diagnostic::warning("failed to reset decompressor: {}",
                            result.ToString())
          .note("emitting compressed body")
          .emit(dh);
        return std::nullopt;
      }
    }
  }
  TENZIR_ASSERT(written != 0);
  out.resize(written);
  return out;
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
      TENZIR_DEBUG("[internal-http-sink] pushing slice");
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::internal_v, atom::push_v, std::move(slice))
        .request(actor_, caf::infinite)
        .then(
          [&] {
            TENZIR_DEBUG("[internal-http-sink] pushed slice");
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
          TENZIR_DEBUG("[internal-http-sink] pushed final slice");
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
          if (exited) {
            self_->quit();
            return {};
          }
          slice_rp_ = self_->make_response_promise<table_slice>();
          return slice_rp_;
        }
        auto x = std::move(slices_.front());
        slices_.pop_front();
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
        if (slices_.empty() or msg.reason) {
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

auto find_decompression_plugin(std::string_view ext, location op,
                               diagnostic_handler& dh)
  -> failure_or<operator_ptr> {
  for (const auto& plugin : plugins::get<operator_factory_plugin>()) {
    if (std::ranges::contains(plugin->decompress_properties().extensions,
                              ext)) {
      TENZIR_DEBUG("[http] inferred plugin `{}` for extension `{}`",
                   plugin->name(), ext);
      auto inv = operator_factory_plugin::invocation{
        ast::entity{
          std::vector{ast::identifier{plugin->name(), op}},
        },
        {},
      };
      auto sp = session_provider::make(dh);
      TRY(auto opptr, plugin->make(std::move(inv), sp.as_session()));
      return opptr;
    }
  }
  return nullptr;
}

auto find_parser_plugin(std::string_view ext, location op,
                        diagnostic_handler& dh) -> failure_or<operator_ptr> {
  for (const auto& plugin : plugins::get<operator_factory_plugin>()) {
    if (std::ranges::contains(plugin->read_properties().extensions, ext)) {
      TENZIR_DEBUG("[http] inferred plugin `{}` for extension `{}`",
                   plugin->name(), ext);
      auto inv = operator_factory_plugin::invocation{
        ast::entity{
          std::vector{ast::identifier{plugin->name(), op}},
        },
        {},
      };
      auto sp = session_provider::make(dh);
      TRY(auto opptr, plugin->make(std::move(inv), sp.as_session()));
      return opptr;
    }
  }
  return nullptr;
}

auto find_plugin_for_mime(std::string_view mime, location op,
                          diagnostic_handler& dh) -> failure_or<operator_ptr> {
  mime = mime.substr(0, mime.find(';'));
  for (const auto& plugin : plugins::get<operator_factory_plugin>()) {
    if (std::ranges::contains(plugin->read_properties().mime_types, mime)) {
      auto inv = operator_factory_plugin::invocation{
        ast::entity{
          std::vector{ast::identifier{plugin->name(), op}},
        },
        {},
      };
      auto sp = session_provider::make(dh);
      TRY(auto opptr, plugin->make(std::move(inv), sp.as_session()));
      return opptr;
    }
  }
  diagnostic::error("could not find a parser for mime-type `{}`", mime)
    .primary(op)
    .hint("consider specifying a parsing pipeline if the format is known")
    .emit(dh);
  return failure::promise();
}

auto make_pipeline(const std::optional<located<pipeline>>& pipe,
                   const caf::uri& uri, const http::response& r, location oploc,
                   diagnostic_handler& dh) -> failure_or<located<pipeline>> {
  if (pipe) {
    return *pipe;
  }
  auto parsed = boost::urls::parse_uri_reference(uri.str());
  if (not parsed) {
    diagnostic::error("invalid URI `{}`", uri.str()).primary(oploc).emit(dh);
    return failure::promise();
  }
  if (not parsed->segments().empty()) {
    auto segment = parsed->segments().back();
    if (const auto last_dot = segment.rfind('.');
        last_dot != std::string::npos and last_dot + 1 != segment.size()
        and last_dot != 0) {
      auto extension = segment.substr(last_dot + 1);
      auto v = std::vector<operator_ptr>{};
      TENZIR_DEBUG("[http] finding decompression plugin for extension `{}`",
                   extension);
      TRY(auto decompressor, find_decompression_plugin(extension, oploc, dh));
      if (decompressor) {
        v.push_back(std::move(decompressor));
        if (const auto first_dot = segment.rfind('.', last_dot - 1);
            first_dot != std::string::npos and first_dot != 0) {
          extension = segment.substr(first_dot + 1, last_dot - first_dot - 1);
        }
      }
      TENZIR_DEBUG("[http] finding parser plugin for extension `{}`",
                   extension);
      TRY(auto parser, find_parser_plugin(extension, oploc, dh));
      if (parser) {
        v.push_back(std::move(parser));
        return located{pipeline{std::move(v)}, oploc};
      }
    }
  }
  const auto& headers = r.header_fields();
  const auto* mit = std::ranges::find_if(headers, [](const auto& x) {
    return caf::icase_equal(x.first, "content-type");
  });
  if (mit == std::ranges::end(headers) or mit->second.empty()) {
    diagnostic::error(
      "cannot deduce a parser without a valid `Content-Type` header")
      .primary(oploc)
      .hint("consider specifying a parsing pipeline if the format is known")
      .emit(dh);
    return failure::promise();
  }
  TRY(auto ptr, find_plugin_for_mime(mit->second, oploc, dh));
  auto v = std::vector<operator_ptr>{};
  v.push_back(std::move(ptr));
  return located{pipeline{std::move(v)}, oploc};
}

auto make_pipeline(const std::optional<located<pipeline>>& pipe,
                   const http::request& r, location oploc,
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
  TRY(auto ptr, find_plugin_for_mime(mime, oploc, dh));
  auto v = std::vector<operator_ptr>{};
  v.push_back(std::move(ptr));
  return located{
    pipeline{std::move(v)},
    oploc,
  };
}

auto spawn_pipeline(operator_control_plane& ctrl, located<pipeline> pipe,
                    std::optional<expression> filter, chunk_ptr ptr,
                    bool is_warning) -> http_actor {
  TENZIR_DEBUG("[http] spawning http_actor");
  // TODO: Figure out why only `from_http` shuts down when spawned as linked
  auto ha = ctrl.self().spawn(caf::actor_from_state<http_state>,
                              ctrl.shared_diagnostics(),
                              ctrl.metrics_receiver(), ctrl.operator_index(),
                              static_cast<exec_node_actor>(&ctrl.self()));
  ctrl.self().monitor(ha, [&ctrl, is_warning,
                           loc = pipe.source](const caf::error& e) {
    if (e) {
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
  TENZIR_DEBUG("[http] spawning subpipeline");
  const auto handle
    = ctrl.self().spawn(pipeline_executor,
                        std::move(pipe.inner).optimize_if_closed(),
                        std::string{ctrl.definition()}, ha, ha, ctrl.node(),
                        ctrl.has_terminal(), ctrl.is_hidden());
  handle->link_to(ha);
  ctrl.self().attach_functor([handle] {});
  TENZIR_DEBUG("[http] requesting subpipeline start");
  ctrl.self()
    .mail(atom::start_v)
    .request(handle, caf::infinite)
    .then(
      [] {
        TENZIR_DEBUG("[http] subpipeline started");
      },
      [&ctrl, is_warning, loc = pipe.source](const caf::error& e) {
        TENZIR_DEBUG("[http] failed to start subpipeline: {}", e);
        diagnostic::error(e)
          .primary(loc)
          .severity(is_warning ? severity::warning : severity::error)
          .emit(ctrl.diagnostics());
      });
  return ha;
}

auto next_url(const std::optional<ast::lambda_expr>& paginate,
              const table_slice& slice, diagnostic_handler& dh)
  -> std::optional<std::string> {
  if (not paginate) {
    return std::nullopt;
  }
  if (slice.rows() != 1) {
    diagnostic::warning("cannot paginate over multiple events")
      .primary(*paginate)
      .note("stopping pagination")
      .emit(dh);
    return std::nullopt;
  }
  const auto ms = eval(*paginate, series{slice}, dh);
  const auto val = ms.value_at(0);
  return match(
    val,
    [](const caf::none_t&) -> std::optional<std::string> {
      TENZIR_DEBUG("[http] finishing pagination");
      return std::nullopt;
    },
    [](const std::string_view& url) -> std::optional<std::string> {
      TENZIR_DEBUG("[http] paginating: {}", url);
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

auto make_metadata(const http::response& r, const uint64_t len)
  -> series_builder {
  auto sb = series_builder{};
  for (auto i = uint64_t{}; i < len; ++i) {
    auto rb = sb.record();
    rb.field("code", static_cast<uint64_t>(r.code()));
    auto hb = rb.field("headers").record();
    for (const auto& [k, v] : r.header_fields()) {
      hb.field(k, v);
    }
  }
  return sb;
}

auto make_metadata(const http::request& r, const uint64_t len) -> series {
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
  std::optional<ast::lambda_expr> paginate;
  std::optional<located<duration>> paginate_delay;
  std::optional<located<duration>> connection_timeout;
  std::optional<located<uint64_t>> max_retry_count;
  std::optional<located<duration>> retry_delay;
  std::optional<location> server;
  std::optional<located<record>> responses;
  std::optional<located<uint64_t>> max_request_size;
  located<bool> tls{false, location::unknown};
  std::optional<located<std::string>> keyfile;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> password;
  std::optional<located<pipeline>> parse;

  auto add_to(argument_parser2& p) {
    p.positional("url", url);
    p.named("method", method);
    p.named("body|payload", body);
    p.named("encode", encode);
    p.named("headers", headers);
    p.named("metadata_field", metadata_field);
    p.named("error_field", error_field);
    p.named("paginate", paginate, "record->string");
    p.named("paginate_delay", paginate_delay);
    p.named("connection_timeout", connection_timeout);
    p.named("max_retry_count", max_retry_count);
    p.named("retry_delay", retry_delay);
    p.named("server", server);
    p.named("responses", responses);
    p.named("max_request_size", max_request_size);
    p.named_optional("tls", tls);
    p.named("certfile", certfile);
    p.named("keyfile", keyfile);
    p.named("password", password);
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
    const auto tls_logic
      = [&](const std::optional<located<std::string>>& opt,
            std::string_view name, bool required = false) -> failure_or<void> {
      if (opt) {
        if (not tls.inner and tls.source) {
          diagnostic::warning("`{}` is unused when `tls` is disabled", name)
            .primary(*opt)
            .emit(dh);
          return {};
        }
        tls.inner = true;
        if (opt->inner.empty()) {
          diagnostic::error("`{}` must not be empty", name)
            .primary(*opt)
            .emit(dh);
          return failure::promise();
        }
        return {};
      }
      if (tls.inner and required) {
        diagnostic::error("`{}` must be set when enabling `tls`", name)
          .primary(tls.source ? tls.source : op)
          .emit(dh);
        return failure::promise();
      }
      return {};
    };
    TRY(tls_logic(certfile, "certfile", server.has_value()));
    TRY(tls_logic(keyfile, "keyfile", server.has_value()));
    TRY(tls_logic(password, "password"));
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
      check_options(false, responses, max_request_size);
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
      diagnostic::error("request size must not be zero")
        .primary(max_request_size->source)
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
          = detail::narrow<std::underlying_type_t<http::status>>(code);
        auto status = http::status{};
        if (not http::from_integer(ucode, status)) {
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

  auto make_method() const -> std::optional<http::method> {
    if (not method) {
      return body ? http::method::post : http::method::get;
    }
    auto m = http::method{};
    if (http::from_string(method->inner, m)) {
      return m;
    }
    return std::nullopt;
  }

  auto make_headers() const
    -> std::pair<std::unordered_map<std::string, std::string>,
                 detail::stable_map<std::string, secret>> {
    auto hdrs = std::unordered_map<std::string, std::string>{};
    auto secrets = detail::stable_map<std::string, secret>{};
    auto insert_content_type = body and is<record>(body->inner);
    if (headers) {
      for (const auto& [k, v] : headers->inner) {
        if (caf::icase_equal(k, "content-type")) {
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
    return std::pair{hdrs, secrets};
  }

  auto make_ssl_context() const -> caf::expected<ssl::context> {
    return ssl::context::enable(tls.inner)
      .and_then(ssl::emplace_context(ssl::tls::any))
      .and_then(ssl::enable_default_verify_paths())
      .and_then(ssl::use_private_key_file_if(inner(keyfile), ssl::format::pem))
      .and_then(ssl::use_certificate_file_if(inner(certfile), ssl::format::pem))
      .and_then(ssl::use_password_if(inner(password)));
  }

  auto make_ssl_context(caf::uri uri) const -> caf::expected<ssl::context> {
    return ssl::context::enable(tls.inner or uri.scheme() == "https")
      .and_then(ssl::emplace_context(ssl::tls::any))
      .and_then(ssl::enable_default_verify_paths())
      .and_then(ssl::use_private_key_file_if(inner(keyfile), ssl::format::pem))
      .and_then(ssl::use_certificate_file_if(inner(certfile), ssl::format::pem))
      .and_then(ssl::use_password_if(inner(password)))
      .and_then(ssl::use_sni_hostname(std::move(uri)));
  }

  friend auto inspect(auto& f, from_http_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("filter", x.filter), f.field("url", x.url),
      f.field("method", x.method), f.field("body", x.body),
      f.field("encode", x.encode), f.field("headers", x.headers),
      f.field("metadata_field", x.metadata_field),
      f.field("error_field", x.error_field), f.field("paginate", x.paginate),
      f.field("paginate_delay", x.paginate_delay),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay), f.field("parse", x.parse),
      f.field("server", x.server), f.field("responses", x.responses),
      f.field("tls", x.tls), f.field("keyfile", x.keyfile),
      f.field("certfile", x.certfile), f.field("password", x.password),
      f.field("max_request_size", x.max_request_size));
  }
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
    auto pull = std::optional<caf::async::consumer_resource<http::request>>{};
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
      = http::with(ctrl.self().system())
          .context(args_.make_ssl_context())
          .accept(port, url)
          .monitor(static_cast<exec_node_actor>(&ctrl.self()))
          .max_request_size(
            inner(args_.max_request_size).value_or(10 * 1024 * 1024))
          .start([&](caf::async::consumer_resource<http::request> cr) {
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
    auto slices = std::vector<table_slice>{};
    ctrl.self()
      .make_observable()
      .from_resource(std::move(*pull))
      .for_each([&](const http::request& r) mutable {
        TENZIR_DEBUG("[http] handling request with size: {}B",
                     r.body().size_bytes());
        if (args_.responses) {
          const auto it = args_.responses->inner.find(r.header().path());
          if (it != args_.responses->inner.end()) {
            auto rec = as<record>(it->second);
            auto code = as<uint64_t>(rec["code"]);
            auto ty = as<std::string>(rec["content_type"]);
            auto body = as<std::string>(rec["body"]);
            r.respond(static_cast<http::status>(code), ty, body);
          }
        } else {
          r.respond(http::status::ok, "", "");
        }
        if (r.body().empty()) {
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
        std::invoke(
          [&, &args_ = args_, r, actor](this const auto& pull) -> void {
            TENZIR_DEBUG("[http] requesting slice");
            ctrl.self()
              .mail(atom::pull_v)
              .request(actor, caf::infinite)
              .then(
                [&, r, pull, actor](table_slice slice) {
                  TENZIR_DEBUG("[http] pulled slice");
                  if (slice.rows() == 0) {
                    TENZIR_DEBUG("[http] finishing subpipeline");
                    ctrl.set_waiting(false);
                    return;
                  }
                  pull();
                  if (args_.metadata_field) {
                    slice = assign(*args_.metadata_field,
                                   make_metadata(r, slice.rows()), slice, dh);
                  }
                  slices.push_back(std::move(slice));
                },
                [&](const caf::error& e) {
                  TENZIR_TRACE("[http] failed to get slice: {}", e);
                });
          });
        TENZIR_DEBUG("[http] handled request");
      });
    while (true) {
      ctrl.set_waiting(true);
      co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to the
      // observe loop after yielding here, causing the vector's iterator to be
      // invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
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

class from_http_client_operator final
  : public crtp_operator<from_http_client_operator> {
public:
  from_http_client_operator() = default;

  from_http_client_operator(from_http_args args) : args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
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
    if (not url.starts_with("http://") and not url.starts_with("https://")) {
      url.insert(0, args_.tls.inner ? "https://" : "http://");
    } else if (args_.tls.inner and url.starts_with("http://")) {
      url.insert(4, "s");
    }
    const auto handle_response = [&](caf::uri uri) {
      return [&, hdrs = headers,
              uri = std::move(uri)](const http::response& r) {
        ctrl.set_waiting(false);
        TENZIR_DEBUG("[http] handling response with size: {}B",
                     r.body().size_bytes());
        const auto& headers = r.header_fields();
        const auto* eit = std::ranges::find_if(headers, [](const auto& x) {
          return caf::icase_equal(x.first, "content-encoding");
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
            TENZIR_DEBUG("[http] requesting slice");
            ctrl.self()
              .mail(atom::pull_v)
              .request(actor, caf::infinite)
              .then(
                [&, r, pull, actor, hdrs](table_slice slice) {
                  TENZIR_DEBUG("[http] pulled slice");
                  ctrl.set_waiting(false);
                  if (slice.rows() == 0) {
                    TENZIR_DEBUG("[http] finishing subpipeline");
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
                  if (auto url = next_url(args_.paginate, slice, dh)) {
                    if (not url->starts_with("http://")
                        and not url->starts_with("https://")) {
                      url->insert(0, args_.tls.inner ? "https://" : "http://");
                    }
                    if (args_.tls.inner and url->starts_with("http://")) {
                      url->insert(4, "s");
                    }
                    auto uri = caf::make_uri(*url);
                    if (not uri) {
                      diagnostic::error("failed to parse uri: {}", uri.error())
                        .primary(args_.op)
                        .emit(ctrl.diagnostics());
                    }
                    paginate_queue.emplace_back(std::move(*uri), hdrs);
                  } else {
                    TENZIR_DEBUG("[http] done paginating");
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
        TENZIR_DEBUG("[http] handled response");
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
    http::with(ctrl.self().system())
      .context(args_.make_ssl_context(*uri))
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
      .transform([&](const caf::async::future<http::response>& fut) {
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
            http::with(ctrl.self().system())
              .context(args_.make_ssl_context(uri))
              .connect(uri)
              .max_response_size(max_response_size)
              .connection_timeout(args_.connection_timeout->inner)
              .max_retry_count(args_.max_retry_count->inner)
              .retry_delay(args_.retry_delay->inner)
              .add_header_fields(hdrs)
              .get()
              .or_else([&](const caf::error& e) {
                diagnostic::error("failed to make http request: {}", e)
                  .primary(args_.op)
                  .emit(dh);
              })
              .transform([](auto&& x) {
                return x.first;
              })
              .transform([&](const caf::async::future<http::response>& fut) {
                fut.bind_to(ctrl.self())
                  .then(handle_response(std::move(uri)),
                        [&](const caf::error& e) {
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

void warn_deprecated_payload(const operator_factory_plugin::invocation& inv,
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

struct from_http final : public virtual operator_factory_plugin {
  auto name() const -> std::string override {
    return "tql2.from_http";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
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

// Base structure for common HTTP request arguments shared between operators
// Common HTTP utility functions shared between http and to_http operators

// Evaluate body expression from table slice, handling different types and
// encodings
auto eval_http_body(const std::optional<ast::expression>& body_expr,
                    const std::optional<located<std::string>>& encode_setting,
                    const table_slice& slice, diagnostic_handler& dh)
  -> generator<std::pair<std::string_view, bool>> {
  if (not body_expr) {
    for (auto i = size_t{}; i < slice.rows(); ++i) {
      co_yield {};
    }
    co_return;
  }
  const auto ms = eval(body_expr.value(), slice, dh);
  for (const auto& s : ms.parts()) {
    if (s.type.kind().is<null_type>()) {
      for (auto i = int64_t{}; i < s.length(); ++i) {
        co_yield {};
      }
      continue;
    }
    if (s.type.kind().is<blob_type>()) {
      for (auto val : s.values<blob_type>()) {
        if (not val) {
          co_yield {};
          continue;
        }
        co_yield {
          {reinterpret_cast<const char*>(val->data()), val->size()},
          false,
        };
      }
      continue;
    }
    if (s.type.kind().is<string_type>()) {
      for (auto val : s.values<string_type>()) {
        if (not val) {
          co_yield {};
          continue;
        }
        co_yield {val.value(), false};
      }
      continue;
    }
    if (s.type.kind().is<record_type>()) {
      auto buf = std::string{};
      const auto form = encode_setting and encode_setting->inner == "form";
      for (auto val : s.values<record_type>()) {
        if (not val) {
          co_yield {};
          continue;
        }
        if (form) {
          co_yield {curl::escape(flatten(materialize(val.value()))), true};
          continue;
        }
        auto p = json_printer{{}};
        auto it = std::back_inserter(buf);
        p.print(it, val.value());
        co_yield {buf, true};
        buf.clear();
      }
      continue;
    }
    diagnostic::warning("expected `blob`, `record` or `string`, got `{}`",
                        s.type.kind())
      .primary(body_expr.value())
      .emit(dh);
    for (auto i = int64_t{}; i < s.length(); ++i) {
      co_yield {};
    }
  }
}

// Evaluate optional string expression from table slice
auto eval_http_optional_string(const std::optional<ast::expression>& expr,
                               const table_slice& slice, diagnostic_handler& dh)
  -> generator<std::string_view> {
  if (not expr) {
    for (auto i = size_t{}; i < slice.rows(); ++i) {
      co_yield {};
    }
    co_return;
  }
  const auto ms = eval(*expr, slice, dh);
  for (const auto& s : ms.parts()) {
    if (s.type.kind().is<null_type>()) {
      for (auto i = int64_t{}; i < s.length(); ++i) {
        co_yield {};
      }
      continue;
    }
    if (s.type.kind().is<string_type>()) {
      for (auto val : s.values<string_type>()) {
        co_yield val.value_or("");
      }
      continue;
    }
    diagnostic::warning("expected `string`, got `{}`", s.type.kind())
      .primary(*expr)
      .emit(dh);
    for (auto i = int64_t{}; i < s.length(); ++i) {
      co_yield {};
    }
  }
}

struct http_request_args {
  tenzir::location op;
  ast::expression url;
  std::optional<ast::expression> method;
  std::optional<ast::expression> body;
  std::optional<located<std::string>> encode;
  std::optional<ast::expression> headers;
  located<uint64_t> parallel{1, location::unknown};
  std::optional<tenzir::location> tls;
  std::optional<located<std::string>> keyfile;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> password;
  located<duration> connection_timeout{5s, location::unknown};
  uint64_t max_retry_count{};
  located<duration> retry_delay{1s, location::unknown};

  // Common validation for HTTP request fields
  auto validate_request_args(diagnostic_handler& dh) const -> failure_or<void> {
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
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
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
    return {};
  }

  // Common method parsing
  auto make_method(const std::string_view method) const
    -> std::optional<http::method> {
    if (method.empty()) {
      if (not this->method and body) {
        return http::method::post;
      }
      return http::method::get;
    }
    auto m = http::method{};
    if (http::from_string(method, m)) {
      return m;
    }
    return std::nullopt;
  }

  // Common SSL context creation
  auto make_ssl_context(caf::uri uri) const -> caf::expected<ssl::context> {
    return ssl::context::enable(tls.has_value() or uri.scheme() == "https")
      .and_then(ssl::emplace_context(ssl::tls::any))
      .and_then(ssl::enable_default_verify_paths())
      .and_then(ssl::use_private_key_file_if(inner(keyfile), ssl::format::pem))
      .and_then(ssl::use_certificate_file_if(inner(certfile), ssl::format::pem))
      .and_then(ssl::use_password_if(inner(password)))
      .and_then(ssl::use_sni_hostname(std::move(uri)));
  }

  friend auto inspect(auto& f, http_request_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("url", x.url), f.field("method", x.method),
      f.field("body", x.body), f.field("encode", x.encode),
      f.field("headers", x.headers), f.field("parallel", x.parallel),
      f.field("tls", x.tls), f.field("keyfile", x.keyfile),
      f.field("certfile", x.certfile), f.field("password", x.password),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay));
  }
};

struct http_args : http_request_args {
  // Response-specific fields (not in base)
  std::optional<ast::field_path> response_field;
  std::optional<ast::field_path> metadata_field;
  std::optional<ast::field_path> error_field;
  std::optional<ast::lambda_expr> paginate;
  located<duration> paginate_delay{0s, location::unknown};
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
    p.named("paginate", paginate, "record->string");
    p.named_optional("paginate_delay", paginate_delay);
    p.named_optional("parallel", parallel);
    p.named("tls", tls);
    p.named("certfile", certfile);
    p.named("keyfile", keyfile);
    p.named("password", password);
    p.named_optional("connection_timeout", connection_timeout);
    p.named_optional("max_retry_count", max_retry_count);
    p.named_optional("retry_delay", retry_delay);
    p.positional("{ … }", parse);
  }

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    // First validate common request arguments
    if (auto result = validate_request_args(dh); not result) {
      return result;
    }

    // Then validate response-specific fields
    if (response_field and metadata_field) {
      auto rp = std::views::transform(response_field->path(),
                                      &ast::field_path::segment::id)
                | std::views::transform(&ast::identifier::name);
      auto mp = std::views::transform(metadata_field->path(),
                                      &ast::field_path::segment::id)
                | std::views::transform(&ast::identifier::name);
      auto [ri, mi] = std::ranges::mismatch(rp, mp);
      if (ri == end(rp) or mi == end(mp)) {
        diagnostic::error("`response_field` and `metadata_field` must not "
                          "point to same or nested field")
          .primary(*response_field)
          .primary(*metadata_field)
          .emit(dh);
        return failure::promise();
      }
    }
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
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
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

  friend auto inspect(auto& f, http_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("url", x.url), f.field("method", x.method),
      f.field("body", x.body), f.field("encode", x.encode),
      f.field("headers", x.headers),
      f.field("response_field", x.response_field),
      f.field("metadata_field", x.metadata_field),
      f.field("error_field", x.error_field), f.field("paginate", x.paginate),
      f.field("paginate_delay", x.paginate_delay),
      f.field("parallel", x.parallel), f.field("tls", x.tls),
      f.field("keyfile", x.keyfile), f.field("certfile", x.certfile),
      f.field("password", x.password),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay), f.field("parse", x.parse),
      f.field("filter", x.filter));
  }
};

// Arguments for the to_http sink operator (subset of http_args without response
// handling)
struct to_http_args : http_request_args {
  std::optional<located<std::string>> on_error;

  auto add_to(argument_parser2& p) {
    p.positional("url", url, "string");
    p.named("method", method, "string");
    p.named("body|payload", body, "record|string|blob");
    p.named("encode", encode);
    p.named("headers", headers, "record");
    p.named_optional("parallel", parallel);
    p.named("on_error", on_error);
    p.named("tls", tls);
    p.named("certfile", certfile);
    p.named("keyfile", keyfile);
    p.named("password", password);
    p.named_optional("connection_timeout", connection_timeout);
    p.named_optional("max_retry_count", max_retry_count);
    p.named_optional("retry_delay", retry_delay);
  }
  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    // Validate on_error parameter
    if (on_error) {
      if (on_error->inner != "fail" and on_error->inner != "warn"
          and on_error->inner != "ignore") {
        diagnostic::error("invalid on_error value: `{}`", on_error->inner)
          .primary(on_error->source)
          .hint("must be `fail`, `warn`, or `ignore`")
          .emit(dh);
        return failure::promise();
      }
    }
    // Validate common request arguments
    return validate_request_args(dh);
  }
  friend auto inspect(auto& f, to_http_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("url", x.url), f.field("method", x.method),
      f.field("body", x.body), f.field("encode", x.encode),
      f.field("headers", x.headers), f.field("parallel", x.parallel),
      f.field("on_error", x.on_error), f.field("tls", x.tls),
      f.field("keyfile", x.keyfile), f.field("certfile", x.certfile),
      f.field("password", x.password),
      f.field("connection_timeout", x.connection_timeout),
      f.field("max_retry_count", x.max_retry_count),
      f.field("retry_delay", x.retry_delay));
  }
};
;

class http_operator final : public crtp_operator<http_operator> {
public:
  http_operator() = default;

  http_operator(http_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
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
    auto hdr_warned = false;
    const auto handle_response
      = [&](view<record> og, caf::uri uri,
            std::unordered_map<std::string, std::string> hdrs) {
          return [&, hdrs = std::move(hdrs), uri = std::move(uri),
                  og = materialize(std::move(og))](const http::response& r) {
            TENZIR_DEBUG("[http] handling response with size: {}B",
                         r.body().size_bytes());
            ctrl.set_waiting(false);
            const auto& headers = r.header_fields();
            const auto* it = std::ranges::find_if(headers, [](const auto& x) {
              return caf::icase_equal(x.first, "content-encoding");
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
              auto sb = series_builder{};
              sb.data(og);
              auto error = series_builder{};
              error.data(make_blob());
              auto slice
                = assign(*args_.error_field, error.finish_assert_one_array(),
                         sb.finish_assert_one_slice(), dh);
              if (args_.metadata_field) {
                auto sb = make_metadata(r, slice.rows());
                slice
                  = assign(*args_.metadata_field, sb.finish_assert_one_array(),
                           slice, ctrl.diagnostics());
              }
              slices.push_back(std::move(slice));
              return;
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
              TENZIR_DEBUG("[http] requesting slice");
              ctrl.self()
                .mail(atom::pull_v)
                .request(actor, caf::infinite)
                .then(
                  [&, r, hdrs, pull, og, actor](table_slice slice) {
                    TENZIR_DEBUG("[http] pulled slice");
                    ctrl.set_waiting(false);
                    if (slice.rows() == 0) {
                      TENZIR_DEBUG("[http] finishing subpipeline");
                      --awaiting;
                      return;
                    }
                    pull();
                    if (args_.response_field) {
                      auto sb = series_builder{};
                      for (auto i = size_t{}; i < slice.rows(); ++i) {
                        sb.data(og);
                      }
                      slice = assign(*args_.response_field, series{slice},
                                     sb.finish_assert_one_slice(), tdh);
                    }
                    if (args_.metadata_field) {
                      auto sb = make_metadata(r, slice.rows());
                      slice = assign(*args_.metadata_field,
                                     sb.finish_assert_one_array(), slice,
                                     ctrl.diagnostics());
                    }
                    if (auto url = next_url(args_.paginate, slice, tdh)) {
                      if (not url->starts_with("http://")
                          and not url->starts_with("https://")) {
                        url->insert(0, args_.tls ? "https://" : "http://");
                      }
                      if (args_.tls and url->starts_with("http://")) {
                        url->insert(4, "s");
                      }
                      auto caf_uri = caf::make_uri(*url);
                      if (not caf_uri) {
                        diagnostic::warning("failed to parse uri: {}",
                                            caf_uri.error())
                          .primary(args_.op)
                          .note("skipping request")
                          .emit(dh);
                      } else {
                        pagination_queue.emplace_back(pagination_request{
                          std::move(*caf_uri), std::move(hdrs)});
                      }
                    } else {
                      TENZIR_DEBUG("[http] done paginating");
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
            TENZIR_DEBUG("[http] handled response");
          };
        };
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto urls = std::vector<std::string>{};
      urls.reserve(slice.rows());
      auto reqs = std::vector<secret_request>{};
      auto url_warn = false;
      const auto url_ms = eval(args_.url, slice, dh);
      for (const auto& s : url_ms.parts()) {
        if (s.type.kind().is<string_type>()) {
          for (auto val : s.values<string_type>()) {
            if (val) {
              urls.emplace_back(*val);
            } else {
              url_warn = true;
              urls.emplace_back();
            }
          }
          continue;
        }
        if (s.type.kind().is<secret_type>()) {
          for (const auto& val : s.values<secret_type>()) {
            if (val) {
              auto req = make_secret_request("url", materialize(*val),
                                             args_.url.get_location(),
                                             urls.emplace_back(), dh);
              reqs.emplace_back(std::move(req));
            } else {
              url_warn = true;
              urls.emplace_back();
            }
          }
          continue;
        }
        diagnostic::warning("expected `string`, got `{}`", s.type.kind())
          .primary(args_.url)
          .emit(dh);
        urls.insert(urls.end(), s.length(), {});
      }
      if (url_warn) {
        diagnostic::warning("`url` must not be null")
          .primary(args_.url)
          .note("skipping request")
          .emit(dh);
      }
      auto hdrs = std::vector<
        std::pair<std::unordered_map<std::string, std::string>, bool>>{};
      if (args_.headers) {
        hdrs.reserve(slice.rows());
        auto hdr_ms = eval(*args_.headers, slice, dh);
        for (const auto& s : hdr_ms.parts()) {
          if (s.type.kind().is_not<record_type>()) {
            hdrs.insert(hdrs.end(), s.length(), {});
            diagnostic::warning("expected `record`, got `{}`", s.type.kind())
              .primary(*args_.headers)
              .note("skipping headers")
              .emit(dh);
            continue;
          }
          for (const auto& val : s.values<record_type>()) {
            if (not val) {
              hdrs.emplace_back();
              diagnostic::warning("expected `record`, got `null`")
                .primary(*args_.headers)
                .note("skipping headers")
                .emit(dh);
              continue;
            }
            auto& [h, has_content_type] = hdrs.emplace_back();
            const auto has_body = args_.body.has_value();
            for (const auto& [k, v] : *val) {
              has_content_type
                |= has_body and caf::icase_equal(k, "content-type");
              match(
                v,
                [&](const std::string_view& x) {
                  h.emplace(k, x);
                },
                [&](const secret_view& x) {
                  auto key = std::string{k};
                  auto req = make_secret_request(key, materialize(x),
                                                 args_.headers->get_location(),
                                                 h[key], dh);
                  reqs.emplace_back(std::move(req));
                },
                [&](const auto&) {
                  if (not hdr_warned) {
                    hdr_warned = true;
                    diagnostic::warning(
                      "`headers` must be `{{ string: string }}`")
                      .primary(*args_.headers)
                      .note("skipping headers")
                      .emit(dh);
                  }
                });
            }
          }
        }
      } else {
        hdrs.resize(slice.rows());
      }
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
        auto& [headers, has_content_type] = *hdr_it++;
        const auto method = methods.next().value();
        const auto [body, insert_content_type] = bodies.next().value();
        if (url.empty()) {
          diagnostic::warning("`url` must not be empty")
            .primary(args_.url)
            .note("skipping request")
            .emit(dh);
          continue;
        }
        if (not url.starts_with("http://")
            and not url.starts_with("https://")) {
          url.insert(0, args_.tls ? "https://" : "http://");
        }
        if (args_.tls and url.starts_with("http://")) {
          url.insert(4, "s");
        }
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
        if (insert_content_type and not has_content_type) {
          headers.emplace("Content-Type",
                          args_.encode and args_.encode->inner == "form"
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
        }
        http::with(ctrl.self().system())
          .context(args_.make_ssl_context(*caf_uri))
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
          .transform([&](const caf::async::future<http::response>& fut) {
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
          ctrl.set_waiting(true);
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
              http::with(ctrl.self().system())
                .context(args_.make_ssl_context(uri))
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
                .transform([&](const caf::async::future<http::response>& fut) {
                  fut.bind_to(ctrl.self())
                    .then(handle_response(row, std::move(uri), std::move(hdrs)),
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

// Sink operator that sends HTTP requests but discards responses
class to_http_operator final : public crtp_operator<to_http_operator> {
public:
  to_http_operator() = default;
  to_http_operator(to_http_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto& dh = ctrl.diagnostics();
    auto awaiting = uint64_t{};
    for (const auto& slice : input) {
      if (slice.rows() == 0) {
        co_yield std::monostate{};
        continue;
      }
      auto urls = std::vector<std::string>{};
      urls.reserve(slice.rows());
      auto reqs = std::vector<secret_request>{};
      auto url_warn = false;

      // Evaluate URLs (using the same logic as http operator)
      const auto url_ms = eval(args_.url, slice, dh);
      for (const auto& s : url_ms.parts()) {
        if (s.type.kind().is<string_type>()) {
          for (auto val : s.values<string_type>()) {
            if (val) {
              urls.emplace_back(*val);
            } else {
              url_warn = true;
              urls.emplace_back();
            }
          }
          continue;
        }
        if (s.type.kind().is<secret_type>()) {
          for (const auto& val : s.values<secret_type>()) {
            if (val) {
              auto req = make_secret_request("url", materialize(*val),
                                             args_.url.get_location(),
                                             urls.emplace_back(), dh);
              reqs.emplace_back(std::move(req));
            } else {
              url_warn = true;
              urls.emplace_back();
            }
          }
          continue;
        }
        diagnostic::warning("expected `string`, got `{}`", s.type.kind())
          .primary(args_.url)
          .emit(dh);
        urls.insert(urls.end(), s.length(), {});
      }
      if (url_warn) {
        diagnostic::warning("`url` must not be null")
          .primary(args_.url)
          .note("skipping request")
          .emit(dh);
      }
      // Evaluate headers
      auto hdrs = std::vector<
        std::pair<std::unordered_map<std::string, std::string>, bool>>{};
      if (args_.headers) {
        hdrs.reserve(slice.rows());
        auto hdr_ms = eval(*args_.headers, slice, dh);
        for (const auto& s : hdr_ms.parts()) {
          if (s.type.kind().is_not<record_type>()) {
            hdrs.insert(hdrs.end(), s.length(), {});
            diagnostic::warning("expected `record`, got `{}`", s.type.kind())
              .primary(*args_.headers)
              .note("skipping headers")
              .emit(dh);
            continue;
          }
          for (const auto& val : s.values<record_type>()) {
            if (not val) {
              hdrs.emplace_back();
              diagnostic::warning("expected `record`, got `null`")
                .primary(*args_.headers)
                .note("skipping headers")
                .emit(dh);
              continue;
            }
            auto& [h, has_content_type] = hdrs.emplace_back();
            const auto has_body = args_.body.has_value();
            for (const auto& [k, v] : *val) {
              has_content_type
                |= has_body and caf::icase_equal(k, "content-type");
              match(
                v,
                [&](const std::string_view& x) {
                  h.emplace(k, x);
                },
                [&](const secret_view& x) {
                  auto key = std::string{k};
                  auto req = make_secret_request(key, materialize(x),
                                                 args_.headers->get_location(),
                                                 h[key], dh);
                  reqs.emplace_back(std::move(req));
                },
                [&](const auto&) {
                  diagnostic::warning(
                    "`headers` must be `{{ string: string }}`")
                    .primary(*args_.headers)
                    .note("skipping headers")
                    .emit(dh);
                });
            }
          }
        }
      } else {
        hdrs.resize(slice.rows());
      }
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
        auto& [headers, has_content_type] = *hdr_it++;
        const auto method = methods.next().value();
        const auto [body, insert_content_type] = bodies.next().value();
        if (url.empty()) {
          diagnostic::warning("`url` must not be empty")
            .primary(args_.url)
            .note("skipping request")
            .emit(dh);
          continue;
        }
        if (not url.starts_with("http://")
            and not url.starts_with("https://")) {
          url.insert(0, args_.tls ? "https://" : "http://");
        }
        if (args_.tls and url.starts_with("http://")) {
          url.insert(4, "s");
        }
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
        if (insert_content_type and not has_content_type) {
          headers.emplace("Content-Type",
                          args_.encode and args_.encode->inner == "form"
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
        }
        // Send HTTP request but don't handle response (sink behavior)
        // Note: The HTTP client retries on network errors and 5xx responses,
        // but not on 4xx client errors (which are typically permanent failures)
        http::with(ctrl.self().system())
          .context(args_.make_ssl_context(*caf_uri))
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
          .transform([&](const caf::async::future<http::response>& fut) {
            ++awaiting;
            fut.bind_to(ctrl.self())
              .then(
                [&, url](const http::response& r) {
                  --awaiting;
                  ctrl.set_waiting(false);
                  const auto code = std::to_underlying(r.code());
                  const auto is_success = 200 <= code and code <= 299;
                  const auto is_client_error = 400 <= code and code <= 499;
                  const auto is_server_error = 500 <= code and code <= 599;

                  if (not is_success) {
                    const auto on_error_mode
                      = args_.on_error ? args_.on_error->inner : "warn";

                    if (on_error_mode == "fail") {
                      diagnostic::error(
                        "HTTP request to {} failed with status {}", url, code)
                        .primary(args_.op)
                        .note(is_client_error   ? "client error"
                              : is_server_error ? "server error"
                                                : "unexpected status code")
                        .emit(dh);
                    } else if (on_error_mode == "warn") {
                      diagnostic::warning(
                        "HTTP request to {} failed with status {}", url, code)
                        .primary(args_.op)
                        .note(is_client_error   ? "client error"
                              : is_server_error ? "server error"
                                                : "unexpected status code")
                        .emit(dh);
                    }
                    // If on_error_mode == "ignore", do nothing
                  } else {
                    TENZIR_TRACE("[to_http] request to {} succeeded with "
                                 "status {}",
                                 url, code);
                  }
                },
                [&, url](const caf::error& e) {
                  --awaiting;
                  ctrl.set_waiting(false);
                  const auto on_error_mode
                    = args_.on_error ? args_.on_error->inner : "warn";

                  if (on_error_mode == "fail") {
                    auto err = diagnostic::error(
                                 "HTTP request to {} failed: {}", url, e)
                                 .primary(args_.op);
                    if (args_.max_retry_count > 0) {
                      err = std::move(err).note(
                        fmt::format("failed after {} retry attempt(s)",
                                    args_.max_retry_count));
                    }
                    std::move(err).emit(dh);
                  } else if (on_error_mode == "warn") {
                    auto warn = diagnostic::warning(
                                  "HTTP request to {} failed: {}", url, e)
                                  .primary(args_.op);
                    if (args_.max_retry_count > 0) {
                      warn = std::move(warn).note(
                        fmt::format("failed after {} retry attempt(s)",
                                    args_.max_retry_count));
                    }
                    std::move(warn).emit(dh);
                  }
                  // If on_error_mode == "ignore", do nothing
                });
          });
        // Limit parallel requests
        while (awaiting >= args_.parallel.inner) {
          ctrl.set_waiting(true);
          co_yield std::monostate{};
        }
      }
    }
    // Wait for all pending requests to complete
    do {
      ctrl.set_waiting(awaiting != 0);
      co_yield std::monostate{};
    } while (awaiting != 0);
  }

  auto name() const -> std::string override {
    return "tql2.to_http";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(expression const& expr, event_order) const
    -> optimize_result override {
    auto args = args_;
    // No filter support for sink operators
    return {
      std::nullopt,
      args_.parallel.inner == 1 ? event_order::ordered : event_order::unordered,
      std::make_unique<to_http_operator>(std::move(args)),
    };
  }

  friend auto inspect(auto& f, to_http_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  to_http_args args_;
};

struct http_plugin final : public operator_plugin2<http_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    warn_deprecated_payload(inv, ctx);
    return std::make_unique<http_operator>(std::move(args));
  }
};

struct to_http_plugin final : public operator_plugin2<to_http_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = to_http_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<to_http_operator>(std::move(args));
  }
};

using internal_source_plugin = operator_inspection_plugin<internal_source>;
using internal_sink_plugin = operator_inspection_plugin<internal_sink>;
using from_http_client = operator_inspection_plugin<from_http_client_operator>;
using from_http_server = operator_inspection_plugin<from_http_server_operator>;

} // namespace
} // namespace tenzir::plugins::http

TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http_client)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::from_http_server)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::http_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::to_http_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::internal_source_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::http::internal_sink_plugin)
