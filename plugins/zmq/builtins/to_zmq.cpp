//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "transport.hpp"

#include <tenzir/chunk.hpp>
#include <tenzir/concept/printable/tenzir/json2.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <chrono>
#include <optional>
#include <span>
#include <string>
#include <utility>

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

enum class Encoding {
  json,
  ndjson,
};

struct SinkArgs {
  located<std::string> endpoint;
  located<std::string> encoding;
  Option<ast::expression> prefix;
  bool monitor = false;
};

auto parse_encoding(std::string_view encoding) -> std::optional<Encoding> {
  if (encoding == "json") {
    return Encoding::json;
  }
  if (encoding == "ndjson") {
    return Encoding::ndjson;
  }
  return std::nullopt;
}

auto make_json_printer(Encoding encoding) -> json_printer2 {
  TENZIR_UNUSED(encoding);
  return json_printer2{json_printer_options{
    .style = no_style(),
    .oneline = true,
  }};
}

auto evaluate_prefix(const ast::expression& expr, const table_slice& input,
                     diagnostic_handler& dh) -> std::optional<std::string> {
  auto result = eval(expr, input, dh);
  auto* series = [&]() -> const tenzir::series* {
    for (const auto& item : result) {
      return &item;
    }
    return nullptr;
  }();
  if (not series) {
    return std::string{};
  }
  if (auto strings = series->as<string_type>()) {
    if (strings->array->IsNull(0)) {
      diagnostic::warning("expected `string`, got `null`").primary(expr).emit(dh);
      return std::nullopt;
    }
    return std::string{strings->array->Value(0)};
  }
  diagnostic::warning("expected `string`, got `{}`", series->type.kind())
    .primary(expr)
    .emit(dh);
  return std::nullopt;
}

auto serialize_row(json_printer2& printer, const table_slice& input)
  -> caf::expected<chunk_ptr> {
  for (auto row : values3(input)) {
    printer.load_new(row);
    auto bytes = printer.bytes();
    return chunk::copy(bytes.data(), bytes.size());
  }
  return caf::make_error(ec::invalid_argument, "expected one event");
}

template <transport::ConnectionMode Mode>
class ZmqSink final : public Operator<table_slice, void> {
public:
  explicit ZmqSink(SinkArgs args)
    : args_{std::move(args)},
      encoding_{*parse_encoding(args_.encoding.inner)},
      printer_{make_json_printer(encoding_)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    endpoint_ = transport::normalize_endpoint(args_.endpoint.inner);
    if (args_.monitor) {
      if (auto err = socket_.enable_peer_monitoring(); not err) {
        diagnostic::error("failed to enable ZeroMQ peer monitoring")
          .primary(args_.endpoint.source)
          .note("{}", render(err.error()))
          .emit(ctx);
        done_ = true;
        co_return;
      }
    }
    if (auto err = socket_.open(Mode, endpoint_); not err) {
      diagnostic::error("failed to open ZeroMQ socket")
        .primary(args_.endpoint.source)
        .note("{}", render(err.error()))
        .emit(ctx);
      done_ = true;
      co_return;
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_) {
      co_return;
    }
    for (size_t row = 0; row < input.rows(); ++row) {
      auto row_slice = subslice(input, row, row + 1);
      auto payload = serialize_row(printer_, row_slice);
      if (not payload) {
        diagnostic::error("failed to serialize ZeroMQ message")
          .primary(args_.encoding.source)
          .note("{}", render(payload.error()))
          .emit(ctx);
        done_ = true;
        co_return;
      }
      auto framed = *payload;
      if (args_.prefix) {
        auto prefix = evaluate_prefix(*args_.prefix, row_slice, ctx.dh());
        if (not prefix) {
          continue;
        }
        auto with_prefix = transport::prepend_prefix(std::move(framed), *prefix);
        if (not with_prefix) {
          diagnostic::error("failed to prefix ZeroMQ message")
            .primary(args_.prefix->get_location())
            .note("{}", render(with_prefix.error()))
            .emit(ctx);
          done_ = true;
          co_return;
        }
        framed = std::move(*with_prefix);
      }
      while (args_.monitor and socket_.num_peers() == 0 and not done_) {
        socket_.poll_monitor(250ms);
      }
      socket_.poll_monitor(0ms);
      auto err = socket_.send(framed, 250ms);
      if (not err) {
        continue;
      }
      diagnostic::error("failed to send ZeroMQ message")
        .primary(args_.endpoint.source)
        .note("{}", render(err))
        .emit(ctx);
      done_ = true;
      co_return;
    }
  }

  auto finalize(OpCtx&) -> Task<FinalizeBehavior> override {
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  SinkArgs args_;
  std::string endpoint_;
  Encoding encoding_;
  json_printer2 printer_;
  transport::Socket socket_{transport::SocketRole::publisher};
  bool done_ = false;
};

template <transport::ConnectionMode Mode>
class ZmqSinkPlugin : public virtual OperatorPlugin {
public:
  explicit ZmqSinkPlugin(std::string name) : name_{std::move(name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto describe() const -> Description override {
    auto d = Describer<SinkArgs, ZmqSink<Mode>>{};
    auto endpoint_arg = d.positional("endpoint", &SinkArgs::endpoint);
    auto encoding_arg = d.named("encoding", &SinkArgs::encoding);
    auto prefix_arg = d.named("prefix", &SinkArgs::prefix, "string");
    auto monitor_arg = d.named("monitor", &SinkArgs::monitor);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint, ctx.get(endpoint_arg));
      if (endpoint.inner.empty()) {
        diagnostic::error("endpoint must not be empty")
          .primary(endpoint.source)
          .emit(ctx);
      }
      TRY(auto encoding, ctx.get(encoding_arg));
      if (not parse_encoding(encoding.inner)) {
        diagnostic::error("unsupported encoding `{}`", encoding.inner)
          .primary(encoding.source)
          .emit(ctx);
      }
      TRY(auto monitor, ctx.get(monitor_arg));
      if (monitor
          and not transport::is_tcp_endpoint(
            transport::normalize_endpoint(endpoint.inner))) {
        diagnostic::error("`monitor` requires a TCP endpoint")
          .primary(ctx.get_location(monitor_arg).value_or(location::unknown))
          .emit(ctx);
      }
      if (auto prefix = ctx.get(prefix_arg)) {
        TENZIR_UNUSED(prefix);
      }
      return {};
    });
    return d.without_optimize();
  }

private:
  std::string name_;
};

class ToZmqPlugin final
  : public ZmqSinkPlugin<transport::ConnectionMode::connect> {
public:
  ToZmqPlugin() : ZmqSinkPlugin{"to_zmq"} {
  }
};

class ServeZmqPlugin final
  : public ZmqSinkPlugin<transport::ConnectionMode::bind> {
public:
  ServeZmqPlugin() : ZmqSinkPlugin{"serve_zmq"} {
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::ToZmqPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::ServeZmqPlugin)
