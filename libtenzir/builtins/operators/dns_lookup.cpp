//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view.hpp>

#include <arpa/inet.h>
#include <arrow/type.h>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <ares.h>
#include <array>
#include <chrono>
#include <memory>
#include <netdb.h>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::dns_lookup {

namespace {

struct ares_init_raii {
  ares_init_raii() {
    const auto res = ares_library_init(ARES_LIB_INIT_ALL);
    if (res != ARES_SUCCESS) {
      TENZIR_WARN("failed to init libares: {}", res);
    }
  }

  ares_init_raii(const ares_init_raii&) = delete;

  ~ares_init_raii() {
    ares_library_cleanup();
  }
};

const static auto ares_init = ares_init_raii{};

class ares_channel_wrapper {
public:
  ares_channel_wrapper() {
    status_ = static_cast<ares_status_t>(ares_library_initialized());
    if (status_ != ARES_SUCCESS) {
      return;
    }
    auto options = ares_options{};
    options.timeout = 5000; // 5 second timeout
    options.tries = 2;
    options.evsys = ARES_EVSYS_DEFAULT;
    const auto optmask
      = ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES | ARES_OPT_EVENT_THREAD;
    status_ = static_cast<ares_status_t>(
      ares_init_options(&channel_, &options, optmask));
    if (status_ != ARES_SUCCESS) {
      channel_ = nullptr;
    }
  }

  ~ares_channel_wrapper() {
    if (channel_) {
      ares_destroy(channel_);
    }
  }

  ares_channel_wrapper(const ares_channel_wrapper&) = delete;
  ares_channel_wrapper& operator=(const ares_channel_wrapper&) = delete;

  auto get() const -> ares_channel {
    return channel_;
  }

  auto valid() const -> bool {
    return channel_ != nullptr;
  }

  auto status() const -> ares_status_t {
    return status_;
  }

private:
  ares_channel channel_ = nullptr;
  ares_status_t status_;
};

struct callback_manager_base {
  operator_control_plane& ctrl;
  const size_t N;
  std::atomic<size_t> finished = 0;

  callback_manager_base(operator_control_plane& ctrl, size_t N)
    : ctrl{ctrl}, N{N} {
  }

  auto done() {
    const auto r = finished.fetch_add(1);
    if (r == N - 1) {
      auto self = caf::actor_cast<caf::actor>(&ctrl.self());
      caf::anon_mail(caf::make_action([&ctrl = this->ctrl]() {
        ctrl.set_waiting(false);
      }))
        .send(self);
    }
  }

  virtual auto result() -> tenzir::series = 0;

  virtual ~callback_manager_base() = default;
};

struct reverse_result {
  struct part {
    ip address;
    std::string type;
    duration ttl;
  };

  reverse_result(callback_manager_base* self) : manager{self} {
  }

  callback_manager_base* manager;
  std::vector<part> parts;

  auto done() {
    return manager->done();
  }
};

struct callback_manager_reverse : callback_manager_base {
  static auto
  perform_must_yield_and_wait(operator_control_plane& ctrl,
                              const type_to_arrow_array_t<string_type>& arr,
                              ares_channel_wrapper& channel)
    -> std::unique_ptr<callback_manager_reverse> {
    auto res = std::make_unique<callback_manager_reverse>(ctrl, arr.length(),
                                                          ctor_token{});
    for (auto i = 0; i < arr.length(); ++i) {
      const auto str = view_at(arr, i);
      if (not str) {
        res->done();
        continue;
      }
      const auto host = std::string{*str};
      auto& result = res->results_[i];
      auto hints = ares_addrinfo_hints{};
      hints.ai_family = AF_UNSPEC;
      hints.ai_flags = ARES_AI_CANONNAME;
      ares_getaddrinfo(channel.get(), host.c_str(), nullptr, &hints, callback,
                       &result);
    }
    return res;
  }

  auto result() -> series override {
    auto builder = series_builder{};
    for (auto& v : results_) {
      if (v.parts.empty()) {
        builder.null();
      } else {
        auto l = builder.list();
        for (const auto& p : v.parts) {
          auto r = l.record();
          r.field("address", p.address);
          r.field("type", p.type);
          r.field("ttl", p.ttl);
        }
      }
    }
    return builder.finish_assert_one_array();
  };

private:
  struct ctor_token {};

public:
  callback_manager_reverse(operator_control_plane& ctrl, size_t N, ctor_token)
    : callback_manager_base{ctrl, N} {
    results_.resize(N, reverse_result{this});
  }

private:
  static auto callback(void* arg, int status, int /*timeouts*/,
                       struct ares_addrinfo* info) -> void {
    auto* res = static_cast<reverse_result*>(arg);
    if (status != ARES_SUCCESS) {
      res->done();
      return;
    }
    for (auto* node = info->nodes; node != nullptr; node = node->ai_next) {
      if (node->ai_family == AF_INET) {
        auto* addr = reinterpret_cast<struct sockaddr_in*>(node->ai_addr);
        auto ip_str = std::array<char, INET_ADDRSTRLEN>{};
        if (inet_ntop(AF_INET, &addr->sin_addr, ip_str.data(), ip_str.size())) {
          auto ip = to<tenzir::ip>(std::string_view{ip_str.data()});
          TENZIR_ASSERT(ip);
          res->parts.emplace_back(*ip, "A", std::chrono::seconds{node->ai_ttl});
        }
      } else if (node->ai_family == AF_INET6) {
        auto* addr = reinterpret_cast<struct sockaddr_in6*>(node->ai_addr);
        auto ip_str = std::array<char, INET6_ADDRSTRLEN>{};
        if (inet_ntop(AF_INET6, &addr->sin6_addr, ip_str.data(),
                      ip_str.size())) {
          auto ip = to<tenzir::ip>(std::string_view{ip_str.data()});
          TENZIR_ASSERT(ip);
          res->parts.emplace_back(*ip, "AAAA",
                                  std::chrono::seconds{node->ai_ttl});
        }
      }
    }
    ares_freeaddrinfo(info);
    res->done();
  }
  std::vector<reverse_result> results_;
};

struct forward_result {
  std::string hostname;

  callback_manager_base* manager;

  forward_result(callback_manager_base* self) : manager{self} {
  }

  auto done() {
    return manager->done();
  }
};

struct callback_manager_forward : callback_manager_base {
  static auto
  perform_must_yield_and_wait(operator_control_plane& ctrl,
                              const type_to_arrow_array_t<ip_type>& arr,
                              ares_channel_wrapper& channel)
    -> std::unique_ptr<callback_manager_forward> {
    auto res = std::make_unique<callback_manager_forward>(ctrl, arr.length(),
                                                          ctor_token{});
    for (auto i = 0; i < arr.length(); ++i) {
      const auto addr = view_at(arr, i);
      if (not addr) {
        res->done();
        continue;
      }
      auto& result = res->results_[i];
      if (addr->is_v4()) {
        const auto bytes = as_bytes(*addr);
        auto in4 = in_addr{};
        std::memcpy(&in4, bytes.data() + 12, 4);
        ares_gethostbyaddr(channel.get(), &in4, sizeof(in4), AF_INET, callback,
                           &result);
      } else {
        const auto bytes = as_bytes(*addr);
        auto in6 = in6_addr{};
        std::memcpy(&in6, bytes.data(), 16);
        ares_gethostbyaddr(channel.get(), &in6, sizeof(in6), AF_INET6, callback,
                           &result);
      }
    }
    return res;
  }

  auto result() -> series override {
    auto builder = series_builder{};
    for (auto& v : results_) {
      if (v.hostname.empty()) {
        builder.null();
      } else {
        builder.record().field("hostname", v.hostname);
      }
    }
    return builder.finish_assert_one_array();
  };

private:
  struct ctor_token {};

public:
  callback_manager_forward(operator_control_plane& ctrl, size_t N, ctor_token)
    : callback_manager_base{ctrl, N} {
    results_.resize(N, forward_result{this});
  }

private:
  static auto callback(void* arg, int status, int /*timeouts*/,
                       struct hostent* host) -> void {
    auto* res = static_cast<forward_result*>(arg);
    if (status == ARES_SUCCESS && host && host->h_name) {
      res->hostname = host->h_name;
    }
    res->done();
  }

  std::vector<forward_result> results_;
};

class dns_lookup_operator final : public crtp_operator<dns_lookup_operator> {
public:
  dns_lookup_operator() = default;
  explicit dns_lookup_operator(ast::expression field, ast::field_path result,
                               struct location operator_location)
    : field_{std::move(field)},
      result_{std::move(result)},
      operator_location_{operator_location} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto channel = std::make_shared<ares_channel_wrapper>();
    if (not channel->valid()) {
      diagnostic::error("failed to initialize DNS resolver")
        .primary(operator_location_, "cares status code: {}",
                 static_cast<int>(channel->status()))
        .emit(ctrl.diagnostics());
      co_return;
    }
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto input = eval(field_, slice, ctrl.diagnostics());
      auto slice_start = 0;
      for (const auto& [field_type, field_array] : input) {
        auto result_series = series{};
        if (is<ip_type>(field_type)) {
          const auto& ip_arr
            = static_cast<const type_to_arrow_array_t<ip_type>&>(*field_array);
          auto result = callback_manager_forward::perform_must_yield_and_wait(
            ctrl, ip_arr, *channel);
          ctrl.set_waiting(true);
          co_yield {};
          result_series = result->result();
        } else if (is<string_type>(field_type)) {
          const auto& host_arr
            = static_cast<const type_to_arrow_array_t<string_type>&>(
              *field_array);
          auto result = callback_manager_reverse::perform_must_yield_and_wait(
            ctrl, host_arr, *channel);
          ctrl.set_waiting(true);
          co_yield {};
          result_series = result->result();
        } else {
          if (auto kind = field_type.kind();
              kind != type_kind{tag_v<null_type>}) {
            diagnostic::warning("expected `ip` or `string`")
              .primary(field_, "got {}", kind)
              .emit(ctrl.diagnostics());
          }
          result_series = series::null(null_type{}, field_array->length());
        }
        const auto slice_end = slice_start + result_series.length();
        auto result_slice = subslice(slice, slice_start, slice_end);
        slice_start = slice_end;
        co_yield assign(result_, std::move(result_series), result_slice,
                        ctrl.diagnostics());
      }
    }
  }

  auto name() const -> std::string override {
    return "dns_lookup";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, copy()};
  }

  friend auto inspect(auto& f, dns_lookup_operator& x) -> bool {
    return f.object(x)
      .pretty_name("dns_lookup_operator")
      .fields(f.field("field", x.field_), f.field("result", x.result_));
  }

private:
  ast::expression field_;
  ast::field_path result_;
  struct location operator_location_;
};

class plugin final : public virtual operator_plugin2<dns_lookup_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto field = ast::expression{};
    auto result = ast::field_path::try_from(
      ast::root_field{ast::identifier{"dns_lookup", location::unknown}});
    TENZIR_ASSERT(result);
    auto parser = argument_parser2::operator_(name());
    parser.positional("field", field, "string|ip");
    parser.named("result", result);
    TRY(parser.parse(inv, ctx));
    TENZIR_ASSERT(result);
    return std::make_unique<dns_lookup_operator>(
      std::move(field), std::move(*result), inv.self.get_location());
  }
};

} // namespace

} // namespace tenzir::plugins::dns_lookup

TENZIR_REGISTER_PLUGIN(tenzir::plugins::dns_lookup::plugin)
