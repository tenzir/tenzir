//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <arpa/inet.h>
#include <caf/uri.hpp>
#include <sys/socket.h>

using namespace std::chrono_literals;

namespace tenzir::plugins::udp {

namespace {

struct loader_args {
  std::string host = {};
  uint16_t port = {};
  bool insert_newlines = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, loader_args& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.udp.loader_args")
      .fields(f.field("host", x.host), f.field("port", x.port),
              f.field("insert_newlines", x.insert_newlines));
  }
};

auto udp_loader_impl(operator_control_plane& ctrl, loader_args args)
  -> generator<chunk_ptr> {
  auto sockfd = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    // TODO: handle failrue
    co_return;
  };
  auto sockfd_guard = caf::detail::make_scope_guard([sockfd] {
    close(sockfd);
  });
  auto buffer = std::array<char, 65536>{};
  struct sockaddr_in server = {};
  struct sockaddr_in client = {};
  socklen_t client_length = sizeof(client);
  memset(&server, 0, sizeof(server));
  memset(&client, 0, sizeof(client));
  server.sin_family = AF_INET;
  server.sin_port = htons(args.port);
  // TODO: resolve host with getaddrinfo
  const auto inet_pton_result
    = inet_pton(AF_INET, args.host.c_str(), &server.sin_addr);
  if (inet_pton_result <= 0) {
    TENZIR_WARN("inet_pton failure {}", inet_pton_result);
    co_return;
  }
  if (bind(sockfd, (struct sockaddr*)&server, sizeof(server)) < 0) {
    TENZIR_WARN("bind failure");
    co_return;
  }
  while (true) {
    TENZIR_WARN("recvfrom");
    auto received_bytes
      = recvfrom(sockfd, buffer.data(), buffer.size() - 1, 0,
                 reinterpret_cast<sockaddr*>(&client), &client_length);
    if (received_bytes < 0) {
      TENZIR_WARN("received_bytes failure");
      co_return;
    }
    TENZIR_ASSERT(received_bytes
                  < detail::narrow_cast<ssize_t>(buffer.size()) - 1);
    if (args.insert_newlines) {
      buffer[received_bytes++] = '\n';
    }
    co_yield chunk::copy(as_bytes(buffer).subspan(0, received_bytes));
  }
}

class udp_loader final : public plugin_loader {
public:
  udp_loader() = default;

  explicit udp_loader(loader_args args) : args_{std::move(args)} {
    // nop
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return udp_loader_impl(ctrl, args_);
  }

  auto name() const -> std::string override {
    return "udp";
  }

  friend auto inspect(auto& f, udp_loader& x) -> bool {
    return f.object(x)
      .pretty_name("udp_loader")
      .fields(f.field("args", x.args_));
  }

private:
  loader_args args_;
};

class plugin final : public virtual loader_plugin<udp_loader> {
public:
  auto name() const -> std::string override {
    return "udp";
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/connectors/{}", name())};
    auto endpoint = located<std::string>{};
    auto args = loader_args{};
    parser.add(endpoint, "<endpoint>");
    parser.add("-n,--insert-newlines", args.insert_newlines);
    parser.parse(p);
    if (endpoint.inner.starts_with("udp://")) {
      endpoint.inner = std::move(endpoint.inner).substr(6);
    }
    auto split = detail::split(endpoint.inner, ":", 1);
    if (split.size() != 2) {
      diagnostic::error("malformed endpoint")
        .primary(endpoint.source)
        .hint("format must be 'udp://address:port'")
        .throw_();
    }
    args.host = std::string{split[0]};
    args.port = stoul(std::string{split[1]});
    return std::make_unique<udp_loader>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::udp

TENZIR_REGISTER_PLUGIN(tenzir::plugins::udp::plugin)
