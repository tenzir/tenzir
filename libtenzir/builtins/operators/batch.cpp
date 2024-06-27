//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/numeric/integral.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::batch {

namespace {

struct buffer_entry {
  std::vector<table_slice> events;
  uint64_t num_buffered = {};
  std::chrono::steady_clock::time_point last_yield
    = std::chrono::steady_clock::now();
};

class batch_operator final : public crtp_operator<batch_operator> {
public:
  batch_operator() = default;

  batch_operator(uint64_t limit, duration timeout, event_order order)
    : limit_{limit}, timeout_{timeout}, order_{order} {
    // nop
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    std::unordered_map<type, buffer_entry> buffers;
    for (auto&& slice : input) {
      const auto now = std::chrono::steady_clock::now();
      // Check all current buffers to see if we have hit a timeout.
      for (auto it = buffers.begin(); it != buffers.end(); ++it) {
        auto& entry = it->second;
        if (now - entry.last_yield > timeout_) {
          TENZIR_ASSERT(entry.num_buffered < limit_);
          co_yield concatenate(std::exchange(entry.events, {}));
          it = buffers.erase(it);
          if (it == buffers.end()) {
            break;
          }
        }
      }
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      // For ordered batching, on schema change, yield the current buffer
      if (order_ == event_order::ordered and not buffers.empty()
          and buffers.begin()->first != slice.schema()) {
        TENZIR_ASSERT(buffers.size() == 1);
        auto& entry = buffers.begin()->second;
        TENZIR_ASSERT(entry.num_buffered < limit_);
        co_yield concatenate(std::move(entry.events));
        buffers.clear();
      }
      // Get the buffer for the current slice schema and append to it.
      auto [it, _] = buffers.try_emplace(slice.schema());
      auto& entry = it->second;
      entry.num_buffered += slice.rows();
      entry.events.push_back(std::move(slice));
      // If the buffer hit the limit, yield until its below again.
      while (entry.num_buffered >= limit_) {
        auto [lhs, rhs] = split(entry.events, limit_);
        auto result = concatenate(std::move(lhs));
        entry.num_buffered -= result.rows();
        entry.last_yield = now;
        co_yield std::move(result);
        entry.events = std::move(rhs);
      }
      if (entry.num_buffered == 0) {
        buffers.erase(it);
      }
    }
    // When our input is done, yield the rest of all buffers.
    // We sort the remaining buffers by yield time for consistent output.
    std::vector<buffer_entry> remaining;
    remaining.reserve(buffers.size());
    for (auto& [_, entry] : buffers) {
      remaining.emplace_back(std::move(entry));
    }
    std::ranges::sort(remaining, {}, &buffer_entry::last_yield);
    for (auto& entry : remaining) {
      co_yield concatenate(std::move(entry.events));
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return optimize_result{
      filter, order, std::make_unique<batch_operator>(limit_, timeout_, order)};
  }

  auto name() const -> std::string override {
    return "batch";
  }

  friend auto inspect(auto& f, batch_operator& x) -> bool {
    return f.object(x)
      .pretty_name("batch_operator")
      .fields(f.field("limit", x.limit_), f.field("timeout", x.timeout_),
              f.field("order", x.order_));
  }

private:
  uint64_t limit_ = defaults::import::table_slice_size;
  duration timeout_ = {};
  event_order order_ = event_order::ordered;
};

class plugin final : public virtual operator_plugin<batch_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"batch", "https://docs.tenzir.com/"
                                           "operators/batch"};
    auto limit = std::optional<located<uint64_t>>{};
    auto timeout = std::optional<located<duration>>{};
    parser.add(limit, "<limit>");
    parser.add("-t,--timeout", timeout, "<limit>");
    parser.parse(p);
    if (limit and limit->inner == 0) {
      diagnostic::error("batch size must not be 0")
        .primary(limit->source)
        .throw_();
    }
    if (timeout and timeout->inner <= duration::zero()) {
      diagnostic::error("timeout must be a positive duration")
        .primary(timeout->source)
        .throw_();
    }

    return std::make_unique<batch_operator>(
      limit ? limit->inner : defaults::import::table_slice_size,
      timeout ? timeout->inner : duration::max(), event_order::ordered);
  }

  auto make(invocation inv, session ctx) const -> operator_ptr override {
    argument_parser2::operator_("batch").parse(inv, ctx);
    return std::make_unique<batch_operator>(defaults::import::table_slice_size,
                                            duration::max(),
                                            event_order::ordered);
  }
};

} // namespace

} // namespace tenzir::plugins::batch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::batch::plugin)
