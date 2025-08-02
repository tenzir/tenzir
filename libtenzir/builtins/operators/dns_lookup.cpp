//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/data.hpp>
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
#include <vector>

namespace tenzir::plugins::dns_lookup {

namespace {

struct dns_result {
  std::vector<std::string> addresses;
  std::vector<std::string> types;
  std::vector<duration> ttls;
  std::string hostname;
  bool is_reverse = false;
};

class ares_channel_wrapper {
public:
  ares_channel_wrapper() {
    if (ares_library_init(ARES_LIB_INIT_ALL) != ARES_SUCCESS) {
      return;
    }
    auto options = ares_options{};
    options.timeout = 5000; // 5 second timeout
    options.tries = 2;
    options.evsys = ARES_EVSYS_DEFAULT;
    auto optmask = ARES_OPT_TIMEOUTMS | ARES_OPT_TRIES | ARES_OPT_EVENT_THREAD;
    if (ares_init_options(&channel_, &options, optmask) != ARES_SUCCESS) {
      ares_library_cleanup();
      channel_ = nullptr;
    }
  }

  ~ares_channel_wrapper() {
    if (channel_) {
      ares_destroy(channel_);
    }
    ares_library_cleanup();
  }

  ares_channel_wrapper(const ares_channel_wrapper&) = delete;
  ares_channel_wrapper& operator=(const ares_channel_wrapper&) = delete;

  auto get() const -> ares_channel {
    return channel_;
  }

  auto valid() const -> bool {
    return channel_ != nullptr;
  }

private:
  ares_channel channel_ = nullptr;
};

auto a_callback(void* arg, int status, int /*timeouts*/,
                struct ares_addrinfo* result) -> void {
  auto* dns_res = static_cast<dns_result*>(arg);
  if (status != ARES_SUCCESS) {
    return;
  }
  for (auto* node = result->nodes; node != nullptr; node = node->ai_next) {
    if (node->ai_family == AF_INET) {
      auto* addr = reinterpret_cast<struct sockaddr_in*>(node->ai_addr);
      auto ip_str = std::array<char, INET_ADDRSTRLEN>{};
      if (inet_ntop(AF_INET, &addr->sin_addr, ip_str.data(), ip_str.size())) {
        dns_res->addresses.emplace_back(ip_str.data());
        dns_res->types.emplace_back("A");
        dns_res->ttls.emplace_back(node->ai_ttl);
      }
    } else if (node->ai_family == AF_INET6) {
      auto* addr = reinterpret_cast<struct sockaddr_in6*>(node->ai_addr);
      auto ip_str = std::array<char, INET6_ADDRSTRLEN>{};
      if (inet_ntop(AF_INET6, &addr->sin6_addr, ip_str.data(), ip_str.size())) {
        dns_res->addresses.emplace_back(ip_str.data());
        dns_res->types.emplace_back("AAAA");
        dns_res->ttls.emplace_back(node->ai_ttl);
      }
    }
  }
  ares_freeaddrinfo(result);
}

auto ptr_callback(void* arg, int status, int /*timeouts*/, struct hostent* host)
  -> void {
  auto* dns_res = static_cast<dns_result*>(arg);
  if (status == ARES_SUCCESS && host && host->h_name) {
    dns_res->hostname = host->h_name;
  }
}

auto perform_dns_lookup(const data& field_value, ares_channel_wrapper& channel)
  -> data {
  if (! channel.valid()) {
    return data{};
  }
  auto result = dns_result{};
  if (const auto* addr = try_as<ip>(field_value)) {
    // Reverse DNS lookup
    result.is_reverse = true;
    if (addr->is_v4()) {
      auto bytes = as_bytes(*addr);
      auto in4 = in_addr{};
      std::memcpy(&in4, bytes.data() + 12, 4);
      ares_gethostbyaddr(channel.get(), &in4, sizeof(in4), AF_INET,
                         ptr_callback, &result);
    } else {
      auto bytes = as_bytes(*addr);
      auto in6 = in6_addr{};
      std::memcpy(&in6, bytes.data(), 16);
      ares_gethostbyaddr(channel.get(), &in6, sizeof(in6), AF_INET6,
                         ptr_callback, &result);
    }
  } else if (const auto* str = try_as<std::string>(field_value)) {
    // Forward DNS lookup
    auto hints = ares_addrinfo_hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = ARES_AI_CANONNAME;
    ares_getaddrinfo(channel.get(), str->c_str(), nullptr, &hints, a_callback,
                     &result);
  } else {
    return data{};
  }
  // Wait for DNS queries to complete. This call blocks.
  ares_queue_wait_empty(channel.get(), -1);
  // Build result record
  if (result.is_reverse) {
    if (! result.hostname.empty()) {
      return record{{"hostname", result.hostname}};
    }
  } else {
    if (! result.addresses.empty()) {
      auto records = tenzir::list{};
      records.reserve(result.addresses.size());
      for (auto i = 0uz; i < result.addresses.size(); ++i) {
        auto addr_result = to<ip>(result.addresses[i]);
        if (addr_result) {
          records.emplace_back(record{{"address", *addr_result},
                                      {"type", result.types[i]},
                                      {"ttl", result.ttls[i]}});
        }
      }
      if (! records.empty()) {
        return record{{"records", std::move(records)}};
      }
    }
  }
  return data{};
}

class dns_lookup_operator final : public crtp_operator<dns_lookup_operator> {
public:
  dns_lookup_operator() = default;
  explicit dns_lookup_operator(std::string field_name, std::string result_field)
    : field_name_{std::move(field_name)},
      result_field_{std::move(result_field)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto channel = std::make_shared<ares_channel_wrapper>();
    if (! channel->valid()) {
      diagnostic::error("failed to initialize DNS resolver")
        .emit(ctrl.diagnostics());
      co_return;
    }
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // Find the field to perform DNS lookup on
      auto schema = as<record_type>(slice.schema());
      auto field_index = schema.resolve_key(field_name_);
      if (! field_index) {
        diagnostic::warning("field '{}' not found in schema", field_name_)
          .emit(ctrl.diagnostics());
        co_yield slice;
        continue;
      }
      // Build DNS lookup results
      auto builder = series_builder{};
      // Get the field array from the record batch
      auto batch = to_record_batch(slice);
      auto field_type = schema.field(*field_index).type;
      auto field_array = batch->column(field_index->at(0));
      // Process each value
      for (auto i = int64_t{0}; i < field_array->length(); ++i) {
        if (field_array->IsNull(i)) {
          builder.null();
          continue;
        }
        auto value = value_at(field_type, *field_array, i);
        auto result = perform_dns_lookup(materialize(value), *channel);
        if (std::holds_alternative<caf::none_t>(result.get_data())) {
          builder.null();
        } else {
          builder.data(std::move(result));
        }
      }
      auto result_series_vec = std::move(builder).finish();
      if (result_series_vec.size() != 1) {
        diagnostic::error("failed to build DNS lookup results")
          .emit(ctrl.diagnostics());
        co_yield slice;
        continue;
      }
      // Add the result field to the slice
      auto result_path = ast::field_path::try_from(
        ast::root_field{ast::identifier{result_field_, location::unknown}});
      TENZIR_ASSERT(result_path.has_value());
      co_yield assign(*result_path, std::move(result_series_vec[0]), slice,
                      ctrl.diagnostics());
    }
  }

  auto name() const -> std::string override {
    return "dns_lookup";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{filter, order, copy()};
  }

  auto detached() const -> bool override {
    // While ARES_OPT_EVENT_THREAD handles DNS I/O asynchronously in a
    // separate thread managed by c-ares, we still use ares_queue_wait_empty()
    // which blocks the calling thread until all queries complete. To avoid
    // blocking the actor system's thread pool, we mark this operator as
    // detached so it runs in its own thread context.
    return true;
  }

  friend auto inspect(auto& f, dns_lookup_operator& x) -> bool {
    return f.object(x)
      .pretty_name("dns_lookup_operator")
      .fields(f.field("field_name", x.field_name_),
              f.field("result_field", x.result_field_));
  }

private:
  std::string field_name_;
  std::string result_field_;
};

class plugin final : public virtual operator_plugin2<dns_lookup_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    // Manual parsing since argument_parser2 doesn't support ast::expression
    if (inv.args.empty()) {
      diagnostic::error("expected field name").primary(inv.self).emit(ctx.dh());
      return failure::promise();
    }
    // Parse the field argument
    auto field_path = ast::field_path::try_from(inv.args[0]);
    if (! field_path) {
      diagnostic::error("expected field name")
        .primary(inv.args[0])
        .emit(ctx.dh());
      return failure::promise();
    }
    if (field_path->path().empty()) {
      diagnostic::error("cannot perform DNS lookup on 'this'")
        .primary(inv.args[0])
        .emit(ctx.dh());
      return failure::promise();
    }
    // Get the field name as a string
    auto field_segments = std::vector<std::string>{};
    for (const auto& segment : field_path->path()) {
      field_segments.emplace_back(segment.id.name);
    }
    auto field_name = fmt::to_string(fmt::join(field_segments, "."));
    // Process optional result=field parameter
    auto result_field = std::string{"dns_lookup"};
    for (size_t i = 1; i < inv.args.size(); ++i) {
      auto* assignment = try_as<ast::assignment>(inv.args[i]);
      if (assignment) {
        // Check if it's "result = ..."
        auto* field_path_left = try_as<ast::field_path>(assignment->left);
        if (field_path_left && field_path_left->path().size() == 1
            && field_path_left->path()[0].id.name == "result") {
          auto result_path = ast::field_path::try_from(assignment->right);
          if (! result_path) {
            diagnostic::error("expected field name for result")
              .primary(assignment->right)
              .emit(ctx.dh());
            return failure::promise();
          }
          if (result_path->path().empty()) {
            diagnostic::error("cannot use 'this' as result field")
              .primary(assignment->right)
              .emit(ctx.dh());
            return failure::promise();
          }
          // Get the result field name as a string
          auto result_segments = std::vector<std::string>{};
          for (const auto& segment : result_path->path()) {
            result_segments.emplace_back(segment.id.name);
          }
          result_field = fmt::to_string(fmt::join(result_segments, "."));
        } else {
          diagnostic::error("unexpected parameter")
            .primary(*assignment)
            .emit(ctx.dh());
          return failure::promise();
        }
      } else {
        diagnostic::error("expected field or named parameter")
          .primary(inv.args[i])
          .emit(ctx.dh());
        return failure::promise();
      }
    }
    return std::make_unique<dns_lookup_operator>(std::move(field_name),
                                                 std::move(result_field));
  }
};

} // namespace

} // namespace tenzir::plugins::dns_lookup

TENZIR_REGISTER_PLUGIN(tenzir::plugins::dns_lookup::plugin)
