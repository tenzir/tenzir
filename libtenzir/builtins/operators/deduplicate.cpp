//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/exec/operator.hpp>
#include <tenzir/null_bitmap.hpp>
#include <tenzir/plan/operator.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <tsl/robin_hash.h>

#include <chrono>

namespace tenzir::plugins::deduplicate {
namespace {

using std::chrono::steady_clock;

constexpr auto max_cleanup_duration = duration{std::chrono::minutes{15}};
constexpr auto min_cleanup_duration = duration{std::chrono::seconds{10}};

struct configuration {
  ast::expression key;
  located<int64_t> limit;
  std::optional<located<int64_t>> distance;
  std::optional<located<duration>> create_timeout;
  std::optional<located<duration>> write_timeout;
  std::optional<located<duration>> read_timeout;

  friend auto inspect(auto& f, configuration& x) -> bool {
    return f.object(x).fields(f.field("key", x.key), f.field("limit", x.limit),
                              f.field("distance", x.distance),
                              f.field("create_timeout", x.create_timeout),
                              f.field("write_timeout", x.write_timeout),
                              f.field("read_timeout", x.read_timeout));
  }
};

struct state {
  int64_t count = {};
  int64_t last_row = {};
  std::chrono::steady_clock::time_point created_at;
  std::chrono::steady_clock::time_point written_at;
  std::chrono::steady_clock::time_point read_at;

  void reset(int64_t current_row, std::chrono::steady_clock::time_point now) {
    count = 1;
    last_row = current_row;
    created_at = now;
    written_at = now;
    read_at = now;
  }

  auto is_expired(const configuration& cfg, int64_t current_row,
                  std::chrono::steady_clock::time_point now) const -> bool {
    return (cfg.create_timeout and now > created_at + cfg.create_timeout->inner)
           or (cfg.write_timeout
               and now > written_at + cfg.write_timeout->inner)
           or (cfg.read_timeout and now > read_at + cfg.read_timeout->inner)
           or (cfg.distance and current_row > last_row + cfg.distance->inner);
  }

  friend auto inspect(auto& f, state& x) -> bool {
    // FIXME: Inspect time points.
    return f.object(x).fields(f.field("count", x.count),
                              f.field("last_row", x.last_row)
                              // ,f.field("created_at", x.created_at),
                              // f.field("written_at", x.written_at),
                              // f.field("read_at", x.read_at)
    );
  }
};

struct deduplicate_state {
  tsl::robin_map<data, state> state = {};
  int64_t row = 0;
  std::chrono::steady_clock::time_point last_cleanup_time
    = std::chrono::steady_clock::now();

  friend auto inspect(auto& f, deduplicate_state& x) -> bool {
    // FIXME: Make `last_cleanup_time` inspectable.
    return f.object(x).fields(f.field("state", x.state), f.field("row", x.row));
  }
};

#if 0
class deduplicate3 : public exec::operator_base<deduplicate_state> {
public:
  explicit deduplicate3(initializer init, configuration cfg)
    : operator_base{std::move(init)}, cfg_{std::move(cfg)} {
  }

  void next(const table_slice& events) override {
    const auto now = std::chrono::steady_clock::now();
    auto& state = this->state();
    if (events.rows() == 0) {
      // We clean up every 15 minutes. This is a bit arbitrary, but there's no
      // good mechanism for detecting whether an operator is idle from within
      // the operator right now.
      if (now > state.last_cleanup_time + std::chrono::minutes{15}) {
        state.last_cleanup_time = now;
        auto expired_keys = std::vector<data>{};
        for (const auto& [key, value] : state.state) {
          if (value.is_expired(cfg_, state.row, now)) {
            expired_keys.push_back(key);
          }
        }
        for (const auto& key : expired_keys) {
          state.state.erase(key);
        }
      }
      return;
    }
    auto keys = eval(cfg_.key, events, ctx());
    auto offset = int64_t{};
    auto ids = null_bitmap{};
    for (auto&& key : keys.values()) {
      const auto current_row = state.row + offset++;
      auto it = state.state.find(key);
      if (it == state.state.end()) {
        state.state.emplace_hint(it, materialize(key), deduplicate::state{})
          .value()
          .reset(current_row, now);
        ids.append_bit(true);
        continue;
      }
      if (it->second.is_expired(cfg_, current_row, now)) {
        it.value().reset(current_row, now);
        ids.append_bit(true);
        continue;
      }
      it.value().read_at = now;
      it.value().last_row = current_row;
      if (it->second.count >= cfg_.limit.inner) {
        ids.append_bit(false);
        continue;
      }
      it.value().count += 1;
      it.value().written_at = now;
      ids.append_bit(true);
    }
    state.row += keys.length();
    for (auto [begin, end] : select_runs(ids)) {
      push(subslice(events, begin, end));
    }
  }

private:
  configuration cfg_;
};

#endif
class deduplicate_bp final : public plan::operator_base {
public:
  explicit deduplicate_bp(configuration cfg) : cfg_{std::move(cfg)} {
  }

  auto name() const -> std::string override {
    return "head_bp";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    // TODO: Initial state.
    TENZIR_UNUSED(args);
    TENZIR_TODO();
    // return exec::spawn_operator<deduplicate3>(std::move(args), {}, cfg_);
  }

  friend auto inspect(auto& f, deduplicate_bp& x) -> bool {
    return f.apply(x.cfg_);
  }

private:
  configuration cfg_;
};

class deduplicate_operator final : public crtp_operator<deduplicate_operator> {
public:
  deduplicate_operator() = default;

  explicit deduplicate_operator(configuration cfg) : cfg_(std::move(cfg)) {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto states = tsl::robin_map<data, state>{};
    auto row = int64_t{};
    constexpr auto get_duration = [](auto opt) -> duration {
      return opt ? opt->inner : duration::max();
    };
    const auto min_cfg = std::min(
      {
        get_duration(cfg_.create_timeout),
        get_duration(cfg_.write_timeout),
        get_duration(cfg_.read_timeout),
      },
      std::less<>{});
    const auto cleanup_duration
      = std::clamp(min_cfg, min_cleanup_duration, max_cleanup_duration);
    auto last_cleanup_time = std::chrono::steady_clock::now();
    for (const auto& slice : input) {
      const auto now = std::chrono::steady_clock::now();
      if (now > last_cleanup_time + cleanup_duration) {
        last_cleanup_time = now;
        for (auto it = states.begin(); it != states.end();) {
          if (it->second.is_expired(cfg_, row, now)) {
            it = states.erase(it);
          } else {
            ++it;
          }
        }
      }
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto keys = eval(cfg_.key, slice, ctrl.diagnostics());
      auto offset = int64_t{};
      auto ids = null_bitmap{};
      for (const auto& key : keys.values()) {
        const auto current_row = row + offset++;
        // FIXME: This needs to materialize the data_view, otherwise this
        // segfaults.
        auto k = materialize(key);
        auto it = states.find(k);
        if (it == states.end()) {
          states.emplace_hint(it, std::move(k), state{})
            .value()
            .reset(current_row, now);
          ids.append_bit(true);
          continue;
        }
        if (it->second.is_expired(cfg_, current_row, now)) {
          it.value().reset(current_row, now);
          ids.append_bit(true);
          continue;
        }
        it.value().read_at = now;
        it.value().last_row = current_row;
        if (it->second.count >= cfg_.limit.inner) {
          ids.append_bit(false);
          continue;
        }
        it.value().count += 1;
        it.value().written_at = now;
        ids.append_bit(true);
      }
      row += keys.length();
      for (auto [begin, end] : select_runs(ids)) {
        co_yield subslice(slice, begin, end);
      }
    }
  }

  auto name() const -> std::string override {
    return "deduplicate";
  }

  auto optimize(const expression& filter, event_order) const
    -> optimize_result override {
    if (cfg_.distance) {
      // When the `distance` option is used, we're not allowed to optimize at
      // all. Here's a simple example that proves this:
      //   metrics "platform"
      //   deduplicate connected, distance=1
      //   where not connected
      return do_not_optimize(*this);
    }
    return optimize_result{filter, event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, deduplicate_operator& x) -> bool {
    return f.object(x).fields(f.field("cfg_", x.cfg_));
  }

private:
  configuration cfg_{};
};

class plugin final : public operator_plugin2<deduplicate_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto key = std::optional<ast::expression>{};
    auto limit = std::optional<located<int64_t>>{};
    auto cfg = configuration{};
    auto parser = argument_parser2::operator_("deduplicate");
    parser.positional("key", key, "any");
    parser.named("distance", cfg.distance);
    parser.named("limit", limit);
    parser.named("create_timeout", cfg.create_timeout);
    parser.named("write_timeout", cfg.write_timeout);
    parser.named("read_timeout", cfg.read_timeout);
    TRY(parser.parse(inv, ctx));
    cfg.key = std::move(key).value_or(ast::this_{location::unknown});
    cfg.limit = limit.value_or(located{1, location::unknown});
    auto failed = false;
    if (cfg.limit.inner < 1) {
      diagnostic::error("limit must be at least 1").primary(cfg.limit).emit(ctx);
      failed = true;
    }
    if (cfg.distance and cfg.distance->inner < 1) {
      diagnostic::error("distance must be at least 1")
        .primary(*cfg.distance)
        .emit(ctx);
      failed = true;
    }
    if (cfg.read_timeout and cfg.read_timeout->inner < duration::zero()) {
      diagnostic::error("read timeout must be positive")
        .primary(*cfg.read_timeout)
        .emit(ctx);
      failed = true;
    }
    if (cfg.write_timeout and cfg.write_timeout->inner < duration::zero()) {
      diagnostic::error("write timeout must be positive")
        .primary(*cfg.write_timeout)
        .emit(ctx);
      failed = true;
    }
    if (cfg.create_timeout and cfg.create_timeout->inner < duration::zero()) {
      diagnostic::error("create timeout must be positive")
        .primary(*cfg.create_timeout)
        .emit(ctx);
      failed = true;
    }
    if (cfg.read_timeout and cfg.write_timeout
        and cfg.read_timeout->inner >= cfg.write_timeout->inner) {
      diagnostic::error("read timeout must be less than write timeout")
        .primary(*cfg.read_timeout)
        .secondary(*cfg.write_timeout)
        .emit(ctx);
      failed = true;
    }
    if (cfg.read_timeout and cfg.create_timeout
        and cfg.read_timeout->inner >= cfg.create_timeout->inner) {
      diagnostic::error("read timeout must be less than create timeout")
        .primary(*cfg.read_timeout)
        .secondary(*cfg.create_timeout)
        .emit(ctx);
      failed = true;
    }
    if (cfg.write_timeout and cfg.create_timeout
        and cfg.write_timeout->inner >= cfg.create_timeout->inner) {
      diagnostic::error("write timeout must be less than create timeout")
        .primary(*cfg.write_timeout)
        .secondary(*cfg.create_timeout)
        .emit(ctx);
      failed = true;
    }
    if (failed) {
      return failure::promise();
    }
    return std::make_unique<deduplicate_operator>(std::move(cfg));
  }
};

} // namespace
} // namespace tenzir::plugins::deduplicate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::deduplicate::plugin)
