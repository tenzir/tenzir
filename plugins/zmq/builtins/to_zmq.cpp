//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"
#include "zmq/transport.hpp"

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/concept/printable/tenzir/json2.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova_json_printer.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <chrono>
#include <span>
#include <string>
#include <type_traits>
#include <utility>

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

constexpr auto monitor_wait_poll_interval = 250ms;
constexpr auto monitor_wait_timeout = 5s;

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

auto parse_encoding(std::string_view encoding) -> Option<Encoding> {
  if (encoding == "json") {
    return Encoding::json;
  }
  if (encoding == "ndjson") {
    return Encoding::ndjson;
  }
  return None{};
}

namespace legacy {

auto evaluate_prefix(const ast::expression& expr, const table_slice& input,
                     diagnostic_handler& dh) -> Option<std::string> {
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
      diagnostic::warning("expected `string`, got `null`")
        .primary(expr)
        .emit(dh);
      return None{};
    }
    return std::string{strings->array->Value(0)};
  }
  diagnostic::warning("expected `string`, got `{}`", series->type.kind())
    .primary(expr)
    .emit(dh);
  return None{};
}

auto serialize_row(Encoding encoding, const table_slice& input)
  -> caf::expected<chunk_ptr> {
  for (auto row : values3(input)) {
    if (encoding == Encoding::ndjson) {
      auto compact_printer = json_printer2{json_printer_options{
        .style = no_style(),
        .oneline = true,
      }};
      compact_printer.load_new(row);
      auto bytes = compact_printer.bytes();
      return chunk::copy(bytes.data(), bytes.size());
    }
    auto pretty_printer = json_printer{json_printer_options{
      .style = no_style(),
    }};
    auto buffer = std::string{};
    auto it = std::back_inserter(buffer);
    if (not pretty_printer.print(it, row)) {
      return caf::make_error(ec::invalid_argument, "failed to print JSON");
    }
    return chunk::copy(buffer.data(), buffer.size());
  }
  return caf::make_error(ec::invalid_argument, "expected one event");
}

template <transport::ConnectionMode Mode>
class ZmqSink final : public Operator<table_slice, void> {
public:
  explicit ZmqSink(SinkArgs args)
    : args_{std::move(args)}, encoding_{*parse_encoding(args_.encoding.inner)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if constexpr (Mode == transport::ConnectionMode::connect) {
      bytes_write_counter_
        = ctx.make_counter(MetricsLabel{"operator", "to_zmq"},
                           MetricsDirection::write,
                           MetricsVisibility::external_, MetricsUnit::bytes);
      events_write_counter_
        = ctx.make_counter(MetricsLabel{"operator", "to_zmq"},
                           MetricsDirection::write,
                           MetricsVisibility::external_, MetricsUnit::events);
    } else {
      bytes_write_counter_
        = ctx.make_counter(MetricsLabel{"operator", "serve_zmq"},
                           MetricsDirection::write,
                           MetricsVisibility::external_, MetricsUnit::bytes);
      events_write_counter_
        = ctx.make_counter(MetricsLabel{"operator", "serve_zmq"},
                           MetricsDirection::write,
                           MetricsVisibility::external_, MetricsUnit::events);
    }
    endpoint_ = transport::normalize_endpoint(args_.endpoint.inner);
    if (args_.monitor) {
      if (auto err = socket_.enable_peer_monitoring(); not err) {
        diagnostic::error("failed to enable ZeroMQ peer monitoring")
          .primary(args_.endpoint.source)
          .note("{}", render(err.error()))
          .emit(ctx);
        co_return;
      }
    }
    if (auto err = socket_.open(Mode, endpoint_); not err) {
      diagnostic::error("failed to open ZeroMQ socket")
        .primary(args_.endpoint.source)
        .note("{}", render(err.error()))
        .emit(ctx);
      co_return;
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_) {
      co_return;
    }
    for (size_t row = 0; row < input.rows(); ++row) {
      auto row_slice = subslice(input, row, row + 1);
      auto payload = serialize_row(encoding_, row_slice);
      if (not payload) {
        diagnostic::error("failed to serialize ZeroMQ message")
          .primary(args_.encoding.source)
          .note("{}", render(payload.error()))
          .emit(ctx);
        co_return;
      }
      auto framed = *payload;
      if (args_.prefix) {
        auto prefix = evaluate_prefix(*args_.prefix, row_slice, ctx.dh());
        if (not prefix) {
          continue;
        }
        auto with_prefix
          = transport::prepend_prefix(std::move(framed), *prefix);
        if (not with_prefix) {
          diagnostic::error("failed to prefix ZeroMQ message")
            .primary(args_.prefix->get_location())
            .note("{}", render(with_prefix.error()))
            .emit(ctx);
          co_return;
        }
        framed = std::move(*with_prefix);
      }
      auto monitor_deadline
        = std::chrono::steady_clock::now() + monitor_wait_timeout;
      while (args_.monitor and socket_.num_peers() == 0 and not done_) {
        socket_.poll_monitor(0ms);
        if (socket_.num_peers() != 0) {
          break;
        }
        if (std::chrono::steady_clock::now() >= monitor_deadline) {
          diagnostic::error("timed out waiting for a ZeroMQ peer")
            .primary(args_.endpoint.source)
            .note("`monitor=true` requires a connected peer before sending")
            .emit(ctx);
          co_return;
        }
        co_await sleep_for(monitor_wait_poll_interval);
      }
      socket_.poll_monitor(0ms);
      auto deadline = std::chrono::steady_clock::now() + 250ms;
      auto err = socket_.send(framed, 0ms);
      while (err == ec::timeout and not done_) {
        if (std::chrono::steady_clock::now() >= deadline) {
          break;
        }
        co_await sleep_for(10ms);
        err = socket_.send(framed, 0ms);
      }
      if (not err) {
        if (framed->size() > 0) {
          bytes_write_counter_.add(framed->size());
        }
        events_write_counter_.add(1);
        continue;
      }
      diagnostic::error("failed to send ZeroMQ message")
        .primary(args_.endpoint.source)
        .note("{}", render(err))
        .emit(ctx);
      co_return;
    }
  }

  auto finalize(OpCtx&) -> Task<FinalizeBehavior> override {
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  SinkArgs args_;
  std::string endpoint_;
  Encoding encoding_;
  transport::Socket socket_{transport::SocketRole::publisher};
  bool done_ = false;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
};

} // namespace legacy

template <transport::ConnectionMode Mode>
class ZmqSink final : public Operator<nova::Events, void> {
public:
  explicit ZmqSink(SinkArgs args)
    : args_{std::move(args)}, encoding_{*parse_encoding(args_.encoding.inner)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (ctx.checkpoint_settings()) {
      diagnostic::error("ZeroMQ sinks do not support checkpointing")
        .primary(args_.endpoint.source)
        .emit(ctx);
      co_return;
    }
    auto label = Mode == transport::ConnectionMode::connect
                   ? MetricsLabel{"operator", "to_zmq"}
                   : MetricsLabel{"operator", "serve_zmq"};
    bytes_write_counter_
      = ctx.make_counter(label, MetricsDirection::write,
                         MetricsVisibility::external_, MetricsUnit::bytes);
    events_write_counter_
      = ctx.make_counter(label, MetricsDirection::write,
                         MetricsVisibility::external_, MetricsUnit::events);
    if (args_.prefix) {
      auto evaluator = co_await nova::Evaluator::make(*args_.prefix, ctx);
      if (not evaluator) {
        co_return;
      }
      prefix_.emplace(std::move(*evaluator));
    }
    auto opened = co_await with_socket(
      [endpoint = transport::normalize_endpoint(args_.endpoint.inner),
       monitor
       = args_.monitor](transport::Socket& socket) -> caf::expected<void> {
        if (monitor) {
          TRY(socket.enable_peer_monitoring());
        }
        return socket.open(Mode, endpoint);
      });
    if (not opened) {
      diagnostic::error("failed to open ZeroMQ socket")
        .primary(args_.endpoint.source)
        .note("{}", render(opened.error()))
        .emit(ctx);
      co_return;
    }
    ready_ = true;
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    if (not ready_) {
      co_return;
    }
    auto rows = input.mask;
    auto prefixes = Option<nova::MaskedArray<nova::Array<nova::String>>>{};
    if (prefix_) {
      prefixes = prefix_->eval(input, nova::EvalCtx{ctx.dh()})
                   .get_alternative<nova::String>();
      auto strings = prefixes ? input.mask & prefixes->present
                              : nova::storage::BitMap{input.length(), false};
      if (input.mask.and_not(strings).any()) {
        diagnostic::warning("expected `string` for ZeroMQ prefix")
          .primary(args_.prefix->get_location())
          .emit(ctx);
      }
      rows = std::move(strings);
    }
    auto printer = nova::json_printer{json_printer_options{
      .style = no_style(),
      .oneline = encoding_ == Encoding::ndjson,
    }};
    for (auto row : nova::storage::true_bits(rows)) {
      printer.print(input.data.get(row));
      auto payload = chunk::copy(printer.bytes());
      if (prefixes) {
        auto framed = transport::prepend_prefix(std::move(payload),
                                                *prefixes->data.get(row));
        if (not framed) {
          diagnostic::error("failed to prefix ZeroMQ message")
            .primary(args_.prefix->get_location())
            .note("{}", render(framed.error()))
            .emit(ctx);
          co_return;
        }
        payload = std::move(*framed);
      }
      if (not co_await send(payload, ctx)) {
        co_return;
      }
      bytes_write_counter_.add(payload->size());
      events_write_counter_.add(1);
    }
  }

  auto state() -> OperatorState override {
    return ready_ ? OperatorState::normal : OperatorState::done;
  }

private:
  template <class F>
  auto with_socket(F f) -> Task<std::invoke_result_t<F, transport::Socket&>> {
    // libzmq permits socket migration with synchronized ownership handoffs.
    // The pool queue publishes the socket to the worker, and the promise/future
    // publishes it back. Await each call before issuing another; the worker
    // retains the live handles if the coroutine is cancelled.
    auto result = co_await spawn_blocking(
      [socket = std::move(socket_), f = std::move(f)]() mutable {
        auto value = f(socket);
        return std::pair{std::move(socket), std::move(value)};
      });
    socket_ = std::move(result.first);
    co_return std::move(result.second);
  }

  auto send(chunk_ptr payload, OpCtx& ctx) -> Task<bool> {
    if (args_.monitor) {
      auto deadline = std::chrono::steady_clock::now() + monitor_wait_timeout;
      while (true) {
        auto peers = co_await with_socket([](transport::Socket& socket) {
          socket.poll_monitor(0ms);
          if (socket.num_peers() == 0) {
            socket.poll_monitor(monitor_wait_poll_interval);
          }
          return socket.num_peers();
        });
        if (peers != 0) {
          break;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
          diagnostic::error("timed out waiting for a ZeroMQ peer")
            .primary(args_.endpoint.source)
            .note("`monitor=true` requires a connected peer before sending")
            .emit(ctx);
          co_return false;
        }
      }
    }
    auto deadline = std::chrono::steady_clock::now() + 250ms;
    auto err = caf::error{};
    do {
      err = co_await with_socket([payload](transport::Socket& socket) {
        socket.poll_monitor(0ms);
        return socket.send(payload, 0ms);
      });
      if (not err) {
        co_return true;
      }
      if (err != ec::timeout) {
        break;
      }
      co_await sleep_for(10ms);
    } while (std::chrono::steady_clock::now() < deadline);
    diagnostic::error("failed to send ZeroMQ message")
      .primary(args_.endpoint.source)
      .note("{}", render(err))
      .emit(ctx);
    co_return false;
  }

  SinkArgs args_;
  Encoding encoding_;
  Option<nova::Evaluator> prefix_;
  transport::Socket socket_{transport::SocketRole::publisher};
  bool ready_ = false;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
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
    auto d = Describer<SinkArgs, legacy::ZmqSink<Mode>, ZmqSink<Mode>>{};
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
