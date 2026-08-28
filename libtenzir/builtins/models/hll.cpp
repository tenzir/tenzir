//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/checked_math.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/hash/xxhash.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/model.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::hll {

namespace {

constexpr auto default_precision = uint64_t{14};
constexpr auto min_precision = uint64_t{4};
constexpr auto max_precision = uint64_t{18};
constexpr auto schema_version = uint64_t{1};
constexpr auto model_name = std::string_view{"hll"};
constexpr auto hash_contract = std::string_view{"tenzir.data.xxh3_64.v1"};

struct model {
  uint8_t precision = 0;
  uint64_t input_count = 0;
  uint64_t count = 0;
  uint64_t null_count = 0;
  std::vector<uint8_t> registers;
};

auto register_count(uint8_t precision) -> size_t {
  return size_t{1} << precision;
}

auto maximum_rank(uint8_t precision) -> uint8_t {
  return static_cast<uint8_t>(65 - precision);
}

auto add_hash(std::vector<uint8_t>& registers, uint8_t precision,
              uint64_t digest) -> void {
  auto const index = digest >> (64 - precision);
  auto const remainder = digest << precision;
  auto const rank
    = static_cast<uint8_t>(std::min(std::countl_zero(remainder) + 1,
                                    static_cast<int>(maximum_rank(precision))));
  registers[index] = std::max(registers[index], rank);
}

auto make_record(model const& value) -> data {
  auto registers = list{};
  registers.reserve(value.registers.size());
  for (auto const rank : value.registers) {
    registers.emplace_back(static_cast<uint64_t>(rank));
  }
  return record{
    {"model", std::string{model_name}},
    {"version", schema_version},
    {"input_count", value.input_count},
    {"count", value.count},
    {"null_count", value.null_count},
    {"precision", static_cast<uint64_t>(value.precision)},
    {"hash", std::string{hash_contract}},
    {"registers", std::move(registers)},
  };
}

auto result_type() -> type {
  return model_record_type({
    {"precision", uint64_type{}},
    {"hash", string_type{}},
    {"registers", list_type{uint64_type{}}},
  });
}

auto parse_model(record_view3 record) -> Result<model, std::string> {
  TRY(auto envelope, parse_model_envelope(record));
  if (envelope.model != model_name) {
    return Err{fmt::format("`model` must be `{}`", model_name)};
  }
  if (envelope.version != schema_version) {
    return Err{fmt::format("unsupported HLL model version {}; "
                           "expected version {}",
                           envelope.version, schema_version)};
  }
  auto precision = Option<uint64_t>{};
  auto registers = Option<list_view3>{};
  for (auto const& [name, value] : record) {
    if (name == "precision") {
      TRY(auto parsed, model_uint64(value));
      precision = parsed;
    } else if (name == "registers") {
      if (auto const* parsed = try_as<list_view3>(value)) {
        registers = *parsed;
      }
    }
  }
  if (not precision or not registers) {
    return Err{"invalid HLL model shape"};
  }
  auto result = model{
    .precision = static_cast<uint8_t>(*precision),
    .input_count = envelope.input_count,
    .count = envelope.count,
    .null_count = envelope.null_count,
    .registers = {},
  };
  result.registers.reserve(registers->size());
  for (auto const value : *registers) {
    TRY(auto rank, model_uint64(value));
    result.registers.push_back(static_cast<uint8_t>(rank));
  }
  return result;
}

auto alpha(size_t m) -> long double {
  switch (m) {
    case 16:
      return 0.673L;
    case 32:
      return 0.697L;
    case 64:
      return 0.709L;
    default:
      return 0.7213L / (1.0L + 1.079L / static_cast<long double>(m));
  }
}

/// Estimates cardinality with linear counting in the small range and the
/// original HLL correction for exhaustion of a 64-bit hash space.
template <class RankAt>
auto estimate_cardinality(size_t size, uint64_t count, RankAt rank_at)
  -> uint64_t {
  auto const m = static_cast<long double>(size);
  auto harmonic_sum = 0.0L;
  auto zeros = size_t{0};
  for (auto i = size_t{0}; i < size; ++i) {
    auto const rank = rank_at(i);
    harmonic_sum += std::ldexp(1.0L, -static_cast<int>(rank));
    zeros += rank == 0;
  }
  auto estimate = alpha(size) * m * m / harmonic_sum;
  if (estimate <= 2.5L * m and zeros > 0) {
    estimate = m * std::log(m / static_cast<long double>(zeros));
  } else {
    constexpr auto hash_space = 18'446'744'073'709'551'616.0L;
    if (estimate > hash_space / 30.0L) {
      if (estimate >= hash_space) {
        return count;
      }
      estimate = -hash_space * std::log1p(-estimate / hash_space);
    }
  }
  auto const max_uint
    = static_cast<long double>(std::numeric_limits<uint64_t>::max());
  if (estimate >= max_uint) {
    return count;
  }
  return std::min(count, static_cast<uint64_t>(std::round(estimate)));
}

class instance final : public aggregation_instance {
public:
  instance(ast::expression expr, uint8_t precision)
    : expr_{std::move(expr)},
      state_{.precision = precision,
             .registers = std::vector<uint8_t>(register_count(precision), 0)} {
  }

  auto update(table_slice const& input, session ctx) -> void override {
    for (auto& arg : eval(expr_, input, ctx)) {
      for (auto const value : values3(*arg.array)) {
        auto input_count = checked_add(state_.input_count, uint64_t{1});
        if (not input_count) {
          warn_overflow("input", ctx);
          continue;
        }
        if (is<caf::none_t>(value)) {
          auto next = checked_add(state_.null_count, uint64_t{1});
          if (not next) {
            warn_overflow("null", ctx);
            continue;
          }
          state_.input_count = *input_count;
          state_.null_count = *next;
          continue;
        }
        auto next = checked_add(state_.count, uint64_t{1});
        if (not next) {
          warn_overflow("observation", ctx);
          continue;
        }
        state_.input_count = *input_count;
        state_.count = *next;
        add_hash(state_.registers, state_.precision,
                 hash<xxh3_64>(data_view3{value}));
      }
    }
  }

  auto get() const -> data override {
    return make_record(state_);
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto const registers = fbb.CreateVector(state_.registers);
    auto const hash_name = fbb.CreateString(hash_contract);
    auto const hll
      = fbs::aggregation::CreateHll(fbb, registers, state_.precision, hash_name,
                                    state_.count, state_.null_count,
                                    state_.input_count);
    fbb.Finish(hll);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto const fb = flatbuffer<fbs::aggregation::Hll>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `hll` aggregation instance: invalid "
                  "FlatBuffer");
      return false;
    }
    auto const* registers = (*fb)->registers();
    auto const* hash_name = (*fb)->hash();
    if (not registers or not hash_name) {
      TENZIR_WARN("failed to restore `hll` aggregation instance: missing "
                  "required state");
      return false;
    }
    if ((*fb)->precision() != state_.precision
        or hash_name->string_view() != hash_contract
        or registers->size() != register_count(state_.precision)) {
      TENZIR_WARN("failed to restore `hll` aggregation instance: "
                  "incompatible configuration");
      return false;
    }
    auto const max_rank = maximum_rank(state_.precision);
    auto nonzero_registers = size_t{0};
    for (auto const rank : *registers) {
      if (rank > max_rank) {
        TENZIR_WARN("failed to restore `hll` aggregation instance: register "
                    "rank exceeds the maximum for precision {}",
                    state_.precision);
        return false;
      }
      nonzero_registers += rank != 0;
    }
    if (((*fb)->count() == 0) != (nonzero_registers == 0)) {
      TENZIR_WARN("failed to restore `hll` aggregation instance: registers "
                  "and count disagree about empty state");
      return false;
    }
    auto const classified_count
      = checked_add((*fb)->count(), (*fb)->null_count());
    if (not classified_count or *classified_count != (*fb)->input_count()) {
      TENZIR_WARN("failed to restore `hll` aggregation instance: input count "
                  "does not match accepted and null counts");
      return false;
    }
    std::ranges::copy(*registers, state_.registers.begin());
    state_.input_count = (*fb)->input_count();
    state_.count = (*fb)->count();
    state_.null_count = (*fb)->null_count();
    return true;
  }

  auto reset() -> void override {
    // Deliberately keep `warned_overflow_`: the warning flags deduplicate
    // diagnostics over the lifetime of the instance, not per row.
    std::ranges::fill(state_.registers, uint8_t{0});
    state_.input_count = 0;
    state_.count = 0;
    state_.null_count = 0;
  }

private:
  auto warn_overflow(std::string_view counter, session ctx) -> void {
    if (warned_overflow_) {
      return;
    }
    warned_overflow_ = true;
    diagnostic::warning("`hll` {} counter overflow; skipping values that "
                        "cannot be counted",
                        counter)
      .primary(expr_)
      .emit(ctx);
  }

  ast::expression expr_;
  model state_;
  bool warned_overflow_ = false;
};

class merge_state final : public model_merge_state {
public:
  explicit merge_state(model state) : state_{std::move(state)} {
  }

  auto merge(record_view3 value) -> Result<void, std::string> override {
    TRY(auto incoming, parse_model(value));
    if (incoming.precision != state_.precision) {
      return Err{fmt::format("incompatible precision: expected {}, got {}",
                             state_.precision, incoming.precision)};
    }
    if (incoming.registers.size() != state_.registers.size()) {
      return Err{"incompatible register count"};
    }
    auto input_count = checked_add(state_.input_count, incoming.input_count);
    if (not input_count) {
      return Err{"`input_count` overflows when merged"};
    }
    auto count = checked_add(state_.count, incoming.count);
    if (not count) {
      return Err{"`count` overflows when merged"};
    }
    auto null_count = checked_add(state_.null_count, incoming.null_count);
    if (not null_count) {
      return Err{"`null_count` overflows when merged"};
    }
    state_.input_count = *input_count;
    state_.count = *count;
    state_.null_count = *null_count;
    for (auto i = size_t{0}; i < state_.registers.size(); ++i) {
      state_.registers[i]
        = std::max(state_.registers[i], incoming.registers[i]);
    }
    return {};
  }

  auto get() const -> data override {
    return make_record(state_);
  }

private:
  model state_;
};

class plugin final : public aggregation_plugin, public model_plugin {
public:
  auto name() const -> std::string override {
    return std::string{model_name};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto model_version() const -> uint64_t override {
    return schema_version;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto precision = Option<located<uint64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .named("precision", precision)
          .parse(inv, ctx));
    auto const value = precision ? precision->inner : default_precision;
    if (value < min_precision or value > max_precision) {
      if (precision) {
        diagnostic::error("`precision` must be in [{}, {}]", min_precision,
                          max_precision)
          .primary(*precision)
          .emit(ctx);
      } else {
        diagnostic::error("`precision` must be in [{}, {}]", min_precision,
                          max_precision)
          .primary(inv.call)
          .emit(ctx);
      }
      return failure::promise();
    }
    return std::make_unique<instance>(std::move(expr),
                                      static_cast<uint8_t>(value));
  }

  auto list_call_result_type(type const&) const -> Option<type> override {
    return result_type();
  }

  auto make_model_merge_state(record_view3 value) const
    -> Result<Box<model_merge_state>, std::string> override {
    TRY(auto parsed, parse_model(value));
    return Box<model_merge_state>{merge_state{std::move(parsed)}};
  }
};

class cardinality final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "hll_cardinality";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("model", expr, "record")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session) -> multi_series {
        return map_series(eval(expr), [&](series input) -> series {
          auto builder = uint64_type::make_arrow_builder(arrow_memory_pool());
          if (is<null_type>(input.type)) {
            check(builder->AppendNulls(input.length()));
            return series{uint64_type{}, finish(*builder)};
          }
          if (input.type != result_type()) {
            check(builder->AppendNulls(input.length()));
            return series{uint64_type{}, finish(*builder)};
          }
          auto const records = input.as<record_type>();
          TENZIR_ASSERT(records);
          auto counts = records->field("count");
          auto registers = records->field("registers");
          TENZIR_ASSERT(counts and registers);
          auto typed_counts = counts->as<uint64_type>();
          auto typed_registers = registers->as<list_type>();
          TENZIR_ASSERT(typed_counts and typed_registers);
          auto ranks = typed_registers->list_values().as<uint64_type>();
          TENZIR_ASSERT(ranks);
          for (auto row = int64_t{0}; row < input.length(); ++row) {
            if (records->array->IsNull(row)) {
              check(builder->AppendNull());
              continue;
            }
            if (typed_counts->array->IsNull(row)
                or typed_registers->array->IsNull(row)) {
              check(builder->AppendNull());
              continue;
            }
            auto const begin = typed_registers->array->value_offset(row);
            auto const end = typed_registers->array->value_offset(row + 1);
            auto valid = begin < end;
            for (auto i = begin; valid and i < end; ++i) {
              valid = not ranks->array->IsNull(i);
            }
            if (not valid) {
              check(builder->AppendNull());
              continue;
            }
            auto const count = typed_counts->array->Value(row);
            check(builder->Append(estimate_cardinality(
              static_cast<size_t>(end - begin), count, [&](size_t i) {
                return ranks->array->Value(begin + static_cast<int64_t>(i));
              })));
          }
          return series{uint64_type{}, finish(*builder)};
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::hll

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hll::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hll::cardinality)
