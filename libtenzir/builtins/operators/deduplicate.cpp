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
#include <tenzir/async.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/session.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>

#include <fmt/format.h>
#include <tsl/robin_hash.h>

#include <chrono>

namespace tenzir::plugins::deduplicate {
namespace {

using std::chrono::steady_clock;

constexpr auto max_cleanup_duration = duration{std::chrono::minutes{15}};
constexpr auto min_cleanup_duration = duration{std::chrono::seconds{10}};

auto make_keys_expression(std::vector<ast::expression> exprs)
  -> ast::expression {
  TENZIR_ASSERT(! exprs.empty());
  if (exprs.size() == 1) {
    return std::move(exprs.front());
  }
  const auto begin_loc = exprs.front().get_location();
  const auto end_loc = exprs.back().get_location();
  auto items = std::vector<ast::record::item>{};
  items.reserve(exprs.size());
  for (size_t idx = 0; idx < exprs.size(); ++idx) {
    auto loc = exprs[idx].get_location();
    auto name = fmt::format("_{}", idx);
    items.emplace_back(ast::record::field{
      ast::identifier{std::move(name), loc},
      std::move(exprs[idx]),
    });
  }
  return ast::expression{ast::record{begin_loc, std::move(items), end_loc}};
}

struct configuration {
  ast::expression keys;
  located<int64_t> limit;
  std::optional<located<int64_t>> distance;
  std::optional<located<duration>> create_timeout;
  std::optional<located<duration>> write_timeout;
  std::optional<located<duration>> read_timeout;
  std::optional<ast::field_path> count_field;

  friend auto inspect(auto& f, configuration& x) -> bool {
    return f.object(x).fields(f.field("keys", x.keys),
                              f.field("limit", x.limit),
                              f.field("distance", x.distance),
                              f.field("create_timeout", x.create_timeout),
                              f.field("write_timeout", x.write_timeout),
                              f.field("read_timeout", x.read_timeout),
                              f.field("count_field", x.count_field));
  }

  static auto
  make(std::vector<ast::expression> keys, std::optional<located<int64_t>> limit,
       std::optional<located<int64_t>> distance,
       std::optional<located<duration>> create_timeout,
       std::optional<located<duration>> write_timeout,
       std::optional<located<duration>> read_timeout,
       std::optional<ast::field_path> count_field, diagnostic_handler& dh)
    -> failure_or<configuration>;

  static auto parse(operator_factory_plugin::invocation inv, session ctx)
    -> failure_or<configuration>;

  auto cleanup_duration() const -> duration;
};

struct DeduplicateArgs {
  std::vector<ast::expression> keys;
  std::optional<located<int64_t>> limit;
  std::optional<located<int64_t>> distance;
  std::optional<located<duration>> create_timeout;
  std::optional<located<duration>> write_timeout;
  std::optional<located<duration>> read_timeout;
  std::optional<ast::field_path> count_field;

  friend auto inspect(auto& f, DeduplicateArgs& x) -> bool {
    return f.object(x).fields(f.field("keys", x.keys),
                              f.field("limit", x.limit),
                              f.field("distance", x.distance),
                              f.field("create_timeout", x.create_timeout),
                              f.field("write_timeout", x.write_timeout),
                              f.field("read_timeout", x.read_timeout),
                              f.field("count_field", x.count_field));
  }
};

struct State {
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

  friend auto inspect(auto& f, State& x) -> bool {
    // We intentionally avoid serializing timeout timestamps. After a restore,
    // this favors less deduplication over accidentally over-deduplicating.
    return f.object(x).fields(f.field("count", x.count),
                              f.field("last_row", x.last_row));
  }

  auto is_double_expired(const configuration& cfg, int64_t current_row,
                         std::chrono::steady_clock::time_point now) const
    -> bool {
    constexpr auto get_duration = [](auto opt) -> duration {
      return opt ? opt->inner : duration::max();
    };
    const auto min_timeout = std::min(
      {
        get_duration(cfg.create_timeout),
        get_duration(cfg.write_timeout),
        get_duration(cfg.read_timeout),
      },
      std::less<>{});

    if (min_timeout != duration::max()
        and now > created_at + (2 * min_timeout)) {
      return true;
    }
    if (cfg.distance and current_row > last_row + (2 * cfg.distance->inner)) {
      return true;
    }
    return false;
  }
};

auto configuration::make(std::vector<ast::expression> keys,
                         std::optional<located<int64_t>> limit,
                         std::optional<located<int64_t>> distance,
                         std::optional<located<duration>> create_timeout,
                         std::optional<located<duration>> write_timeout,
                         std::optional<located<duration>> read_timeout,
                         std::optional<ast::field_path> count_field,
                         diagnostic_handler& dh) -> failure_or<configuration> {
  auto seen_general_expression = false;
  auto normalized_keys = std::vector<ast::expression>{};
  normalized_keys.reserve(keys.size());
  auto failed = false;
  for (auto& key : keys) {
    if (auto selector = ast::field_path::try_from(ast::expression{key})) {
      if (selector->has_this() or selector->path().empty()) {
        diagnostic::error("cannot deduplicate `this` explicitly")
          .primary(*selector)
          .emit(dh);
        failed = true;
        continue;
      }
      if (seen_general_expression) {
        diagnostic::error("cannot mix field selectors with general expressions")
          .primary(key)
          .emit(dh);
        failed = true;
        continue;
      }
      normalized_keys.push_back(std::move(*selector).unwrap());
      continue;
    }
    if (seen_general_expression) {
      diagnostic::error("expected selector").primary(key).emit(dh);
      failed = true;
      continue;
    }
    if (not normalized_keys.empty()) {
      diagnostic::error("cannot mix field selectors with general expressions")
        .primary(key)
        .emit(dh);
      failed = true;
      continue;
    }
    seen_general_expression = true;
    normalized_keys.push_back(std::move(key));
  }
  auto cfg = configuration{};
  cfg.keys = normalized_keys.empty()
               ? ast::expression{ast::this_{location::unknown}}
               : make_keys_expression(std::move(normalized_keys));
  cfg.limit = limit.value_or(located{1, location::unknown});
  cfg.distance = distance;
  cfg.create_timeout = create_timeout;
  cfg.write_timeout = write_timeout;
  cfg.read_timeout = read_timeout;
  cfg.count_field = std::move(count_field);
  if (cfg.limit.inner < 1) {
    diagnostic::error("limit must be at least 1").primary(cfg.limit).emit(dh);
    failed = true;
  }
  if (cfg.distance and cfg.distance->inner < 1) {
    diagnostic::error("distance must be at least 1")
      .primary(*cfg.distance)
      .emit(dh);
    failed = true;
  }
  if (cfg.read_timeout and cfg.read_timeout->inner < duration::zero()) {
    diagnostic::error("read timeout must be positive")
      .primary(*cfg.read_timeout)
      .emit(dh);
    failed = true;
  }
  if (cfg.write_timeout and cfg.write_timeout->inner < duration::zero()) {
    diagnostic::error("write timeout must be positive")
      .primary(*cfg.write_timeout)
      .emit(dh);
    failed = true;
  }
  if (cfg.create_timeout and cfg.create_timeout->inner < duration::zero()) {
    diagnostic::error("create timeout must be positive")
      .primary(*cfg.create_timeout)
      .emit(dh);
    failed = true;
  }
  if (cfg.read_timeout and cfg.write_timeout
      and cfg.read_timeout->inner >= cfg.write_timeout->inner) {
    diagnostic::error("read timeout must be less than write timeout")
      .primary(*cfg.read_timeout)
      .secondary(*cfg.write_timeout)
      .emit(dh);
    failed = true;
  }
  if (cfg.read_timeout and cfg.create_timeout
      and cfg.read_timeout->inner >= cfg.create_timeout->inner) {
    diagnostic::error("read timeout must be less than create timeout")
      .primary(*cfg.read_timeout)
      .secondary(*cfg.create_timeout)
      .emit(dh);
    failed = true;
  }
  if (cfg.write_timeout and cfg.create_timeout
      and cfg.write_timeout->inner >= cfg.create_timeout->inner) {
    diagnostic::error("write timeout must be less than create timeout")
      .primary(*cfg.write_timeout)
      .secondary(*cfg.create_timeout)
      .emit(dh);
    failed = true;
  }
  if (failed) {
    return failure::promise();
  }
  return cfg;
}

auto configuration::parse(operator_factory_plugin::invocation inv, session ctx)
  -> failure_or<configuration> {
  auto expressions = std::vector<ast::expression>{};
  auto named_args = std::vector<ast::expression>{};
  auto seen_named = false;
  auto seen_general_expression = false;
  expressions.reserve(inv.args.size());
  named_args.reserve(inv.args.size());
  for (auto& arg : inv.args) {
    if (is<ast::assignment>(arg)) {
      seen_named = true;
      named_args.push_back(std::move(arg));
      continue;
    }
    if (seen_named) {
      diagnostic::error("positional arguments must precede named arguments")
        .primary(arg)
        .emit(ctx);
      return failure::promise();
    }
    auto selector = ast::field_path::try_from(arg);
    if (selector) {
      if (selector->has_this() || selector->path().empty()) {
        diagnostic::error("cannot deduplicate `this` explicitly")
          .primary(*selector)
          .emit(ctx);
        return failure::promise();
      }
      if (seen_general_expression) {
        diagnostic::error("cannot mix field selectors with general expressions")
          .primary(arg)
          .emit(ctx);
        return failure::promise();
      }
      expressions.push_back(std::move(*selector).unwrap());
      continue;
    }
    if (seen_general_expression) {
      diagnostic::error("expected selector").primary(arg).emit(ctx);
      return failure::promise();
    }
    if (! expressions.empty()) {
      diagnostic::error("cannot mix field selectors with general expressions")
        .primary(arg)
        .emit(ctx);
      return failure::promise();
    }
    seen_general_expression = true;
    expressions.push_back(std::move(arg));
  }
  auto limit = std::optional<located<int64_t>>{};
  auto cfg = DeduplicateArgs{};
  auto parser = argument_parser2::operator_("deduplicate");
  [[maybe_unused]] auto unused_key = std::optional<ast::expression>{};
  parser.positional("key", unused_key, "any");
  parser.named("distance", cfg.distance);
  parser.named("limit", limit);
  parser.named("create_timeout", cfg.create_timeout);
  parser.named("write_timeout", cfg.write_timeout);
  parser.named("read_timeout", cfg.read_timeout);
  parser.named("count_field", cfg.count_field);
  auto parser_inv
    = operator_factory_plugin::invocation{inv.self, std::move(named_args)};
  TRY(parser.parse(parser_inv, ctx));
  return make(std::move(expressions), limit, cfg.distance, cfg.create_timeout,
              cfg.write_timeout, cfg.read_timeout, cfg.count_field, ctx);
}

auto configuration::cleanup_duration() const -> duration {
  constexpr auto get_duration = [](auto opt) -> duration {
    return opt ? opt->inner : duration::max();
  };
  const auto min_cfg = std::min(
    {
      get_duration(create_timeout),
      get_duration(write_timeout),
      get_duration(read_timeout),
    },
    std::less<>{});
  return std::clamp(min_cfg, min_cleanup_duration, max_cleanup_duration);
}

auto deduplicate_slice(const table_slice& slice, const configuration& cfg,
                       duration cleanup_duration,
                       tsl::robin_map<data, State>& states, int64_t& row,
                       std::chrono::steady_clock::time_point& last_cleanup_time,
                       diagnostic_handler& dh) -> table_slice {
  const auto now = std::chrono::steady_clock::now();
  if (now > last_cleanup_time + cleanup_duration) {
    last_cleanup_time = now;
    for (auto it = states.begin(); it != states.end();) {
      const auto should_remove = cfg.count_field
                                   ? it->second.is_double_expired(cfg, row, now)
                                   : it->second.is_expired(cfg, row, now);
      if (should_remove) {
        it = states.erase(it);
      } else {
        ++it;
      }
    }
  }
  if (slice.rows() == 0) {
    return {};
  }
  auto keys = eval(cfg.keys, slice, dh);
  auto offset = int64_t{};
  auto mask_builder = arrow::BooleanBuilder{arrow_memory_pool()};
  check(mask_builder.Reserve(detail::narrow_cast<int64_t>(slice.rows())));
  auto count_builder = std::shared_ptr<arrow::Int64Builder>{};
  if (cfg.count_field) {
    count_builder = std::make_shared<arrow::Int64Builder>(arrow_memory_pool());
    check(count_builder->Reserve(detail::narrow_cast<int64_t>(slice.rows())));
  }
  for (const auto& key : keys.values()) {
    const auto current_row = row + offset++;
    // FIXME: This needs to materialize the data_view, otherwise this
    // segfaults.
    auto k = materialize(key);
    auto it = states.find(k);
    if (it == states.end()) {
      states.emplace_hint(it, std::move(k), State{})
        .value()
        .reset(current_row, now);
      check(mask_builder.Append(true));
      if (count_builder) {
        check(count_builder->Append(0));
      }
      continue;
    }
    if (it->second.is_expired(cfg, current_row, now)) {
      if (count_builder) {
        const auto double_expired
          = it->second.is_double_expired(cfg, current_row, now);
        if (double_expired) {
          check(count_builder->Append(0));
        } else {
          const auto dropped = it->second.count - cfg.limit.inner;
          check(count_builder->Append(dropped));
        }
      }
      it.value().reset(current_row, now);
      check(mask_builder.Append(true));
      continue;
    }
    it.value().read_at = now;
    it.value().last_row = current_row;
    it.value().count += 1;
    if (it->second.count > cfg.limit.inner) {
      check(mask_builder.Append(false));
      continue;
    }
    it.value().written_at = now;
    check(mask_builder.Append(true));
    if (count_builder) {
      check(count_builder->Append(0));
    }
  }
  row += keys.length();
  auto mask = finish(mask_builder);
  auto filtered = filter(slice, *mask);
  if (cfg.count_field and filtered.rows() > 0) {
    auto count_series = series{int64_type{}, finish(*count_builder)};
    return assign(cfg.count_field.value(), count_series, filtered, dh,
                  assign_position::back);
  }
  return filtered;
}

class deduplicate_operator final : public crtp_operator<deduplicate_operator> {
public:
  deduplicate_operator() = default;

  explicit deduplicate_operator(configuration cfg) : cfg_(std::move(cfg)) {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto states = tsl::robin_map<data, State>{};
    auto row = int64_t{};
    const auto cleanup_duration = cfg_.cleanup_duration();
    auto last_cleanup_time = std::chrono::steady_clock::now();
    for (const auto& slice : input) {
      auto output
        = deduplicate_slice(slice, cfg_, cleanup_duration, states, row,
                            last_cleanup_time, ctrl.diagnostics());
      if (output.rows() > 0) {
        co_yield std::move(output);
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

auto make_configuration_checked(DeduplicateArgs args) -> configuration {
  auto dh = null_diagnostic_handler{};
  auto cfg = configuration::make(
    std::move(args.keys), std::move(args.limit), std::move(args.distance),
    std::move(args.create_timeout), std::move(args.write_timeout),
    std::move(args.read_timeout), std::move(args.count_field), dh);
  TENZIR_ASSERT(cfg);
  return std::move(*cfg);
}

class Deduplicate final : public Operator<table_slice, table_slice> {
public:
  explicit Deduplicate(DeduplicateArgs args)
    : cfg_{make_configuration_checked(std::move(args))},
      cleanup_duration_{cfg_.cleanup_duration()} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto output = deduplicate_slice(input, cfg_, cleanup_duration_, states_,
                                    row_, last_cleanup_time_, ctx);
    if (output.rows() > 0) {
      co_await push(std::move(output));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("states", states_);
    serde("row", row_);
  }

private:
  configuration cfg_;
  duration cleanup_duration_;
  tsl::robin_map<data, State> states_;
  int64_t row_ = 0;
  std::chrono::steady_clock::time_point last_cleanup_time_
    = std::chrono::steady_clock::now();
};

class Plugin final : public operator_plugin2<deduplicate_operator>,
                     public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<DeduplicateArgs, Deduplicate>{};
    auto keys = d.optional_variadic("key", &DeduplicateArgs::keys, "any");
    auto limit = d.named("limit", &DeduplicateArgs::limit);
    auto distance = d.named("distance", &DeduplicateArgs::distance);
    auto create_timeout
      = d.named("create_timeout", &DeduplicateArgs::create_timeout);
    auto write_timeout
      = d.named("write_timeout", &DeduplicateArgs::write_timeout);
    auto read_timeout = d.named("read_timeout", &DeduplicateArgs::read_timeout);
    auto count_field = d.named("count_field", &DeduplicateArgs::count_field);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto key_values = ctx.get_all(keys);
      auto key_exprs = std::vector<ast::expression>{};
      key_exprs.reserve(key_values.size());
      for (auto& value : key_values) {
        if (not value) {
          return {};
        }
        key_exprs.push_back(std::move(*value));
      }
      auto result
        = configuration::make(std::move(key_exprs), ctx.get(limit),
                              ctx.get(distance), ctx.get(create_timeout),
                              ctx.get(write_timeout), ctx.get(read_timeout),
                              ctx.get(count_field), ctx);
      if (not result) {
        return {};
      }
      return {};
    });
    return d.without_optimize();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto cfg, configuration::parse(std::move(inv), ctx));
    return std::make_unique<deduplicate_operator>(std::move(cfg));
  }
};

} // namespace
} // namespace tenzir::plugins::deduplicate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::deduplicate::Plugin)
