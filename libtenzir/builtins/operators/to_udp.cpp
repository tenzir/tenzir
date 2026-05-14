//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/channel.hpp>
#include <tenzir/async/dns.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/result.hpp>
#include <tenzir/tql2/eval.hpp>

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncUDPSocket.h>

#include <cerrno>
#include <iterator>
#include <string>

namespace tenzir::plugins::to_udp {

namespace {

constexpr auto request_queue_capacity = uint32_t{1};

struct ToUdpArgs {
  location self;
  located<std::string> endpoint;
  Option<ast::expression> message;
};

struct SendBatchResult {
  std::vector<diagnostic> diagnostics;
};

struct SendBatch {
  std::vector<chunk_ptr> payloads;
  Sender<SendBatchResult> reply_to;
};

class ToUdp final : public Operator<table_slice, void> {
public:
  explicit ToUdp(ToUdpArgs args)
    : ToUdp{std::move(args), channel<SendBatch>(request_queue_capacity)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    evb_ = folly::getKeepAliveToken(ctx.io_executor()->getEventBase());
    auto endpoint = tenzir::Endpoint{};
    auto parsed = parsers::endpoint(args_.endpoint.inner, endpoint)
                  and endpoint.port and not endpoint.host.empty();
    TENZIR_ASSERT(parsed);
    auto address
      = co_await forward_dns_.resolve_socket_address(std::move(endpoint));
    if (address.is_err()) {
      diagnostic::error("failed to resolve remote endpoint")
        .primary(args_.endpoint)
        .note("reason: {}", std::move(address).unwrap_err())
        .emit(ctx);
      co_return;
    }
    // A truly label-free counter is not possible with the current metrics API:
    // `make_counter` always requires exactly one label pair.
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_udp"},
                         MetricsDirection::write, MetricsVisibility::external_);
    auto [startup_sender, startup_receiver]
      = channel<diagnostic>(request_queue_capacity);
    TENZIR_ASSERT(write_sender_);
    ctx.spawn_task(folly::coro::co_withExecutor(
      evb_, write_loop(*evb_, std::move(address).unwrap(), args_.self,
                       std::move(write_receiver_), std::move(startup_sender),
                       bytes_write_counter_)));
    // Successful startup is signaled by closing the channel without errors.
    while (auto diagnostic = co_await startup_receiver.recv()) {
      std::move(*diagnostic).modify().emit(ctx);
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    auto messages
      = eval(args_.message ? *args_.message : ast::expression{ast::this_{}},
             input, ctx);
    auto payloads = std::vector<chunk_ptr>{};
    for (auto const& part : messages.parts()) {
      if (part.type.kind().is<null_type>()) {
        diagnostic::warning("`message` evaluated to `null`, skipping event")
          .primary(args_.message.unwrap())
          .emit(ctx);
        continue;
      }
      if (part.type.kind().is<string_type>()) {
        for (auto value : part.values<string_type>()) {
          if (not value) {
            continue;
          }
          payloads.emplace_back(chunk::copy(value->data(), value->size()));
        }
        continue;
      }
      if (part.type.kind().is<blob_type>()) {
        for (auto value : part.values<blob_type>()) {
          if (not value) {
            continue;
          }
          payloads.emplace_back(chunk::copy(*value));
        }
        continue;
      }
      if (part.type.kind().is<record_type>()) {
        auto buffer = std::string{};
        auto printer = json_printer{json_printer_options{
          .style = no_style(),
          .oneline = true,
        }};
        for (auto value : part.values<record_type>()) {
          if (not value) {
            continue;
          }
          auto it = std::back_inserter(buffer);
          auto success = printer.print(it, value.value());
          TENZIR_ASSERT(success);
          payloads.emplace_back(chunk::copy(buffer.data(), buffer.size()));
          buffer.clear();
        }
        continue;
      }
      diagnostic::warning("expected `blob`, `record`, or `string`, got `{}`",
                          part.type.kind())
        .primary(args_.message.unwrap())
        .emit(ctx);
    }
    if (payloads.empty()) {
      co_return;
    }
    auto [reply_sender, reply_receiver] = channel<SendBatchResult>(1);
    // We currently use a one-shot channel to confirm that the data has been
    // completely sent. That might not be ideal, but it simplifies the code for
    // now that we don't have to wait for the queue to be drained. As a result,
    // the writer channel can be capacity one and writing always succeeds.
    auto write_result = write_sender_->try_send(
      SendBatch{std::move(payloads), std::move(reply_sender)});
    TENZIR_ASSERT(write_result.is_ok());
    auto result = co_await reply_receiver.recv();
    TENZIR_ASSERT(result);
    for (auto& diagnostic : result->diagnostics) {
      std::move(diagnostic).modify().emit(ctx);
    }
  }

  auto finalize(OpCtx&) -> Task<FinalizeBehavior> override {
    write_sender_ = None{};
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return write_sender_ ? OperatorState::normal : OperatorState::done;
  }

private:
  explicit ToUdp(ToUdpArgs args,
                 std::tuple<Sender<SendBatch>, Receiver<SendBatch>> ch)
    : args_{std::move(args)},
      write_sender_{std::move(std::get<0>(ch))},
      write_receiver_{std::move(std::get<1>(ch))} {
  }

  static auto write_loop(folly::EventBase& evb, folly::SocketAddress address,
                         location self, Receiver<SendBatch> write_receiver,
                         Sender<diagnostic> startup_sender,
                         MetricsCounter bytes_write_counter) -> Task<void> {
    auto socket = folly::AsyncUDPSocket{&evb};
    auto startup_diagnostics = collecting_diagnostic_handler{};
    try {
      socket.connect(address);
    } catch (std::exception const& ex) {
      diagnostic::error("failed to connect UDP socket")
        .primary(self)
        .note("{}", ex.what())
        .emit(startup_diagnostics);
    }
    auto startup_result = std::move(startup_diagnostics).collect();
    for (auto& diagnostic : startup_result) {
      co_await startup_sender.send(std::move(diagnostic));
    }
    {
      // Let the caller know that startup is complete by dropping the channel.
      auto _ = std::move(startup_sender);
    }
    if (not startup_result.empty()) {
      co_return;
    }
    while (true) {
      auto batch = co_await write_receiver.recv();
      if (not batch) {
        co_return;
      }
      auto diagnostics = collecting_diagnostic_handler{};
      for (auto const& payload : batch->payloads) {
        auto iov = iovec{
          const_cast<std::byte*>(payload->data()),
          payload->size(),
        };
        auto num_sent = socket.writev(address, &iov, 1);
        if (num_sent < 0) {
          auto builder = diagnostic::warning("failed to send UDP datagram")
                           .primary(self, detail::describe_errno());
          if (errno == EMSGSIZE) {
            builder = std::move(builder).hint(
              "ensure the payload fits into a single UDP datagram");
          }
          std::move(builder).emit(diagnostics);
          continue;
        }
        if (num_sent != detail::narrow_cast<ssize_t>(payload->size())) {
          diagnostic::warning("failed to send complete UDP datagram")
            .primary(self)
            .note("sent {} of {} bytes", num_sent, payload->size())
            .emit(diagnostics);
          continue;
        }
        bytes_write_counter.add(payload->size());
      }
      // This single-shot channel of capacity 1 can always be written to since
      // we don't reuse it.
      auto reply_result = batch->reply_to.try_send(
        SendBatchResult{std::move(diagnostics).collect()});
      TENZIR_ASSERT(reply_result.is_ok());
    }
  }

  ToUdpArgs args_;
  folly::Executor::KeepAlive<folly::EventBase> evb_;
  ForwardDnsResolver forward_dns_;
  Option<Sender<SendBatch>> write_sender_;
  Receiver<SendBatch> write_receiver_;
  MetricsCounter bytes_write_counter_;
};

class ToUdpPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_udp";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToUdpArgs, ToUdp>{};
    d.operator_location(&ToUdpArgs::self);
    auto endpoint_arg = d.positional("endpoint", &ToUdpArgs::endpoint);
    d.named("message", &ToUdpArgs::message, "any");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_str, ctx.get(endpoint_arg));
      auto location
        = ctx.get_location(endpoint_arg).value_or(location::unknown);
      auto endpoint = tenzir::Endpoint{};
      if (not parsers::endpoint(endpoint_str.inner, endpoint)
          or not endpoint.port or endpoint.host.empty()) {
        diagnostic::error("failed to parse endpoint")
          .primary(location)
          .emit(ctx);
      } else if (endpoint.port->type() != port_type::unknown
                 and endpoint.port->type() != port_type::udp) {
        diagnostic::error("expected a UDP endpoint").primary(location).emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::to_udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_udp::ToUdpPlugin)
