//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/endpoint.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/socket.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <algorithm>
#include <cerrno>
#include <iterator>
#include <span>
#include <string>

namespace tenzir::plugins::to_udp {

namespace {

struct ToUdpArgs {
  located<std::string> endpoint;
  Option<ast::expression> message;
};

struct ConnectResult {
  failure_or<tenzir::socket> socket;
  Option<diagnostic> diagnostic;
};

struct SendResult {
  std::vector<diagnostic> diagnostics;
  uint64_t bytes_sent = 0;
};

class ToUdp final : public Operator<table_slice, void> {
public:
  explicit ToUdp(ToUdpArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto result = co_await spawn_blocking(
      [endpoint = args_.endpoint.inner,
       source = args_.endpoint.source]() mutable -> ConnectResult {
        auto parsed = socket_endpoint::parse("udp://" + endpoint);
        if (not parsed) {
          return {
            .socket = failure::promise(),
            .diagnostic = diagnostic::error("invalid UDP endpoint")
                            .primary(source, "{}", parsed.error())
                            .done(),
          };
        }
        auto socket = tenzir::socket{*parsed};
        if (not socket) {
          return {
            .socket = failure::promise(),
            .diagnostic = diagnostic::error("failed to create UDP socket")
                            .primary(source, detail::describe_errno())
                            .note("endpoint: {}", parsed->addr)
                            .done(),
          };
        }
        if (socket.connect(*parsed) < 0) {
          return {
            .socket = failure::promise(),
            .diagnostic = diagnostic::error("failed to connect UDP socket")
                            .primary(source, detail::describe_errno())
                            .note("endpoint: {}", parsed->addr)
                            .done(),
          };
        }
        return {
          .socket = std::move(socket),
          .diagnostic = None{},
        };
      });
    if (result.socket) {
      socket_ = std::move(*result.socket);
    }
    if (result.diagnostic) {
      std::move(*result.diagnostic).modify().emit(ctx.dh());
    }
    bytes_write_counter_ = ctx.make_counter(
      MetricsLabel{"peer",
                   MetricsLabel::FixedString::truncate(args_.endpoint.inner)},
      MetricsDirection::write, MetricsVisibility::external_);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    auto messages = eval(args_.message ? *args_.message : ast::expression{ast::this_{}},
                         input, ctx.dh());
    auto result = co_await spawn_blocking(
      [this, messages = std::move(messages)]() mutable -> SendResult {
        auto diagnostics = collecting_diagnostic_handler{};
        auto bytes_sent = uint64_t{0};
        auto send_payload = [&](std::span<const std::byte> payload) -> bool {
          auto num_sent = socket_.send(payload);
          if (num_sent < 0) {
            auto builder = diagnostic::error("failed to send UDP datagram")
                             .primary(args_.endpoint.source,
                                      detail::describe_errno());
            if (errno == EMSGSIZE) {
              builder = std::move(builder).hint(
                "ensure the payload fits into a single UDP datagram");
            }
            std::move(builder).emit(diagnostics);
            return false;
          }
          if (num_sent != detail::narrow_cast<ssize_t>(payload.size())) {
            diagnostic::error("failed to send complete UDP datagram")
              .primary(args_.endpoint.source)
              .note("sent {} of {} bytes", num_sent, payload.size())
              .emit(diagnostics);
            return false;
          }
          bytes_sent += payload.size();
          return true;
        };
        for (auto const& part : messages.parts()) {
          if (part.type.kind().is<null_type>()) {
            diagnostic::warning("`message` evaluated to `null`, skipping event")
              .primary(args_.message ? *args_.message : ast::expression{ast::this_{}})
              .emit(diagnostics);
            continue;
          }
          if (part.type.kind().is<string_type>()) {
            for (auto value : part.values<string_type>()) {
              if (not value) {
                continue;
              }
              auto payload = std::span{
                reinterpret_cast<const std::byte*>(value->data()),
                value->size(),
              };
              if (not send_payload(payload)) {
                return {
                  .diagnostics = std::move(diagnostics).collect(),
                  .bytes_sent = bytes_sent,
                };
              }
            }
            continue;
          }
          if (part.type.kind().is<blob_type>()) {
            for (auto value : part.values<blob_type>()) {
              if (not value) {
                continue;
              }
              if (not send_payload(*value)) {
                return {
                  .diagnostics = std::move(diagnostics).collect(),
                  .bytes_sent = bytes_sent,
                };
              }
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
              if (not printer.print(it, value.value())) {
                diagnostic::error("failed to serialize `message` as JSON")
                  .primary(args_.message ? *args_.message : ast::expression{ast::this_{}})
                  .emit(diagnostics);
                return {
                  .diagnostics = std::move(diagnostics).collect(),
                  .bytes_sent = bytes_sent,
                };
              }
              auto payload = std::span{
                reinterpret_cast<const std::byte*>(buffer.data()),
                buffer.size(),
              };
              if (not send_payload(payload)) {
                return {
                  .diagnostics = std::move(diagnostics).collect(),
                  .bytes_sent = bytes_sent,
                };
              }
              buffer.clear();
            }
            continue;
          }
          diagnostic::warning(
            "expected `blob`, `null`, `record`, or `string`, got `{}`",
            part.type.kind())
            .primary(args_.message ? *args_.message : ast::expression{ast::this_{}})
            .emit(diagnostics);
        }
        return {
          .diagnostics = std::move(diagnostics).collect(),
          .bytes_sent = bytes_sent,
        };
      });
    auto stop = false;
    for (auto& diagnostic : result.diagnostics) {
      stop = stop or diagnostic.severity == severity::error;
      std::move(diagnostic).modify().emit(ctx.dh());
    }
    bytes_write_counter_.add(result.bytes_sent);
    if (stop) {
      co_return;
    }
  }

private:
  ToUdpArgs args_;
  tenzir::socket socket_;
  MetricsCounter bytes_write_counter_;
};

class ToUdpPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_udp";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToUdpArgs, ToUdp>{};
    auto endpoint_arg = d.positional("endpoint", &ToUdpArgs::endpoint);
    d.named("message", &ToUdpArgs::message, "any");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto endpoint_str, ctx.get(endpoint_arg));
      auto endpoint = to<struct endpoint>(endpoint_str.inner);
      auto location = ctx.get_location(endpoint_arg).value_or(location::unknown);
      if (not endpoint) {
        diagnostic::error("failed to parse endpoint").primary(location).emit(ctx);
      } else if (not endpoint->port) {
        diagnostic::error("port number is required").primary(location).emit(ctx);
      } else if (endpoint->host.empty()) {
        diagnostic::error("host is required").primary(location).emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::to_udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_udp::ToUdpPlugin)
