//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/checked_math.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/detail/tdigest.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/model.hpp>
#include <tenzir/nova/aggregation.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "model_helpers.hpp"
#include "sketch_helpers.hpp"

namespace tenzir::plugins::tdigest {

namespace {

using namespace si_literals;

constexpr auto model_name = std::string_view{"tdigest"};
constexpr auto model_version = uint64_t{1};
constexpr auto min_compression = uint64_t{10};
constexpr auto max_compression = uint64_t{10_k};

struct model {
  explicit model(uint32_t compression)
    : compression{compression}, digest{compression} {
  }

  uint32_t compression;
  uint64_t input_count = 0;
  uint64_t count = 0;
  uint64_t null_count = 0;
  uint64_t non_finite_count = 0;
  detail::tdigest digest;
};

auto result_type() -> type {
  return model_record_type({
    {"compression", uint64_type{}},
    {"non_finite_count", uint64_type{}},
    {"min", double_type{}},
    {"max", double_type{}},
    {"centroids", list_type{record_type{
                    {"mean", double_type{}},
                    {"weight", double_type{}},
                  }}},
  });
}

auto make_record(model const& m) -> record {
  auto const state = m.digest.save();
  auto centroids = list{};
  centroids.reserve(state.means.size());
  for (auto i = size_t{0}; i < state.means.size(); ++i) {
    centroids.emplace_back(record{
      {"mean", state.means[i]},
      {"weight", state.weights[i]},
    });
  }
  return record{
    {"model", std::string{model_name}},
    {"version", model_version},
    {"input_count", m.input_count},
    {"count", m.count},
    {"null_count", m.null_count},
    {"compression", uint64_t{m.compression}},
    {"non_finite_count", m.non_finite_count},
    {"min", m.count > 0 ? data{state.min} : data{}},
    {"max", m.count > 0 ? data{state.max} : data{}},
    {"centroids", std::move(centroids)},
  };
}

auto checked_weight(double weight) -> Option<uint64_t> {
  if (not std::isfinite(weight) or weight < 1.0 or std::trunc(weight) != weight
      or weight >= static_cast<double>(std::numeric_limits<uint64_t>::max())) {
    return None{};
  }
  return static_cast<uint64_t>(weight);
}

auto make_model(uint64_t compression, uint64_t input_count, uint64_t count,
                uint64_t null_count, uint64_t non_finite_count,
                detail::tdigest_state state) -> Result<model, std::string> {
  auto result = model{static_cast<uint32_t>(compression)};
  if (not result.digest.restore(state)) {
    return Err{"invalid t-digest model shape"};
  }
  result.input_count = input_count;
  result.count = count;
  result.null_count = null_count;
  result.non_finite_count = non_finite_count;
  return result;
}

auto parse_model(record_view3 rec) -> Result<model, std::string> {
  TRY(auto envelope, parse_model_envelope(rec));
  if (envelope.model != model_name) {
    return Err{
      fmt::format("expected model `{}`, got `{}`", model_name, envelope.model)};
  }
  if (envelope.version != model_version) {
    return Err{fmt::format("unsupported model version {}; expected {}",
                           envelope.version, model_version)};
  }
  auto compression = Option<uint64_t>{};
  auto non_finite_count = Option<uint64_t>{};
  auto has_min = false;
  auto has_max = false;
  auto min_is_null = false;
  auto max_is_null = false;
  auto min = Option<double>{};
  auto max = Option<double>{};
  auto centroids = Option<list_view3>{};
  for (auto const& [name, value] : rec) {
    if (name == "compression") {
      auto parsed = model_uint64(value);
      if (not parsed) {
        return Err{fmt::format("`compression` {}", parsed.unwrap_err())};
      }
      compression = std::move(parsed).unwrap();
    } else if (name == "non_finite_count") {
      auto parsed = model_uint64(value);
      if (not parsed) {
        return Err{fmt::format("`non_finite_count` {}", parsed.unwrap_err())};
      }
      non_finite_count = std::move(parsed).unwrap();
    } else if (name == "min") {
      has_min = true;
      if (is<caf::none_t>(value)) {
        min_is_null = true;
      } else {
        auto parsed = model_double(value);
        if (not parsed) {
          return Err{fmt::format("`min` {} or null", parsed.unwrap_err())};
        }
        min = std::move(parsed).unwrap();
      }
    } else if (name == "max") {
      has_max = true;
      if (is<caf::none_t>(value)) {
        max_is_null = true;
      } else {
        auto parsed = model_double(value);
        if (not parsed) {
          return Err{fmt::format("`max` {} or null", parsed.unwrap_err())};
        }
        max = std::move(parsed).unwrap();
      }
    } else if (name == "centroids") {
      auto const* parsed = try_as<list_view3>(value);
      if (not parsed) {
        return Err{"`centroids` must be a list of records"};
      }
      centroids = *parsed;
    }
  }
  for (auto const& [name, present] : {
         std::pair{"compression", compression.has_value()},
         std::pair{"non_finite_count", non_finite_count.has_value()},
         std::pair{"min", has_min},
         std::pair{"max", has_max},
         std::pair{"centroids", centroids.has_value()},
       }) {
    if (not present) {
      return Err{fmt::format("missing field `{}`", name)};
    }
  }
  auto state = detail::tdigest_state{};
  for (auto const value : *centroids) {
    auto const* centroid = try_as<record_view3>(value);
    if (not centroid) {
      return Err{"`centroids` must contain only records"};
    }
    auto mean = Option<double>{};
    auto weight = Option<double>{};
    for (auto const& [name, field] : *centroid) {
      if (name == "mean") {
        auto parsed = model_double(field);
        if (not parsed) {
          return Err{fmt::format("centroid `mean` {}", parsed.unwrap_err())};
        }
        mean = std::move(parsed).unwrap();
      } else if (name == "weight") {
        auto parsed = model_double(field);
        if (not parsed) {
          return Err{fmt::format("centroid `weight` {}", parsed.unwrap_err())};
        }
        weight = std::move(parsed).unwrap();
      }
    }
    if (not mean or not weight) {
      return Err{"every centroid needs `mean` and `weight`"};
    }
    if (state.means.size() >= max_compression) {
      return Err{
        fmt::format("`centroids` must not exceed {} entries", max_compression)};
    }
    state.means.push_back(*mean);
    state.weights.push_back(*weight);
  }
  if (not min_is_null and min) {
    state.min = *min;
  }
  if (not max_is_null and max) {
    state.max = *max;
  }
  return make_model(*compression, envelope.input_count, envelope.count,
                    envelope.null_count, *non_finite_count, std::move(state));
}

class merge_state final : public model_merge_state {
public:
  explicit merge_state(model initial) : model_{std::move(initial)} {
  }

  auto merge(record_view3 rec) -> Result<void, std::string> override {
    TRY(auto other, parse_model(rec));
    if (other.compression != model_.compression) {
      return Err{fmt::format("incompatible compression: expected {}, got {}",
                             model_.compression, other.compression)};
    }
    auto const input_count = checked_add(model_.input_count, other.input_count);
    auto const count = checked_add(model_.count, other.count);
    auto const null_count = checked_add(model_.null_count, other.null_count);
    auto const non_finite_count
      = checked_add(model_.non_finite_count, other.non_finite_count);
    if (not input_count or not count or not null_count
        or not non_finite_count) {
      return Err{"counter overflow"};
    }
    auto merged = model_.digest;
    merged.merge(other.digest);
    if (auto valid = merged.validate(); not valid) {
      return Err{fmt::format("native t-digest merge failed validation: {}",
                             valid.unwrap_err())};
    }
    auto const state = merged.save();
    auto merged_weight = uint64_t{0};
    for (auto const weight : state.weights) {
      auto const parsed = checked_weight(weight);
      if (not parsed) {
        return Err{"native merge produced a non-integral weight"};
      }
      auto const sum = checked_add(merged_weight, *parsed);
      if (not sum) {
        return Err{"native merge weight sum overflows"};
      }
      merged_weight = *sum;
    }
    if (merged_weight != *count) {
      return Err{"native merge produced an inconsistent count"};
    }
    model_.digest = std::move(merged);
    model_.input_count = *input_count;
    model_.count = *count;
    model_.null_count = *null_count;
    model_.non_finite_count = *non_finite_count;
    return {};
  }

  auto get() const -> data override {
    return make_record(model_);
  }

private:
  model model_;
};

class instance final : public aggregation_instance {
public:
  instance(ast::expression expr, uint32_t compression)
    : expr_{std::move(expr)}, model_{compression} {
  }

  auto update(table_slice const& input, session ctx) -> void override {
    if (failed_) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      if (auto const sum = checked_add(
            model_.input_count, static_cast<uint64_t>(arg.array->length()))) {
        model_.input_count = *sum;
      } else {
        fail("`input_count` overflow", ctx);
        return;
      }
      auto const add_nulls = [&](uint64_t n) {
        if (auto const sum = checked_add(model_.null_count, n)) {
          model_.null_count = *sum;
        } else {
          fail("`null_count` overflow", ctx);
        }
      };
      auto const f = detail::overload{
        [&]<concepts::one_of<double_type, int64_type, uint64_type> Type>(
          Type const&) {
          auto const& array = as<type_to_arrow_array_t<Type>>(*arg.array);
          for (auto value : values3(array)) {
            if (not value) {
              add_nulls(1);
              if (failed_) {
                return;
              }
              continue;
            }
            auto const x = static_cast<double>(*value);
            if (not std::isfinite(x)) {
              if (auto const sum
                  = checked_add(model_.non_finite_count, uint64_t{1})) {
                model_.non_finite_count = *sum;
              } else {
                fail("`non_finite_count` overflow", ctx);
                return;
              }
              continue;
            }
            if (auto const sum = checked_add(model_.count, uint64_t{1})) {
              model_.count = *sum;
            } else {
              fail("`count` overflow", ctx);
              return;
            }
            model_.digest.add(x);
          }
        },
        [&](null_type const&) {
          add_nulls(static_cast<uint64_t>(arg.array->length()));
        },
        [&](auto const&) {
          add_nulls(static_cast<uint64_t>(arg.array->null_count()));
          if (not warned_type_) {
            warned_type_ = true;
            diagnostic::warning("expected `int`, `uint`, or `float`, got `{}`; "
                                "skipping these values",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
          }
        },
      };
      match(arg.type, f);
      if (failed_) {
        return;
      }
    }
  }

  auto get() const -> data override {
    if (failed_) {
      return {};
    }
    return make_record(model_);
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto const state = model_.digest.save();
    auto const means = fbb.CreateVector(state.means);
    auto const weights = fbb.CreateVector(state.weights);
    auto const fb = fbs::aggregation::CreateTDigest(
      fbb, means, weights, model_.compression, model_.count, model_.null_count,
      model_.non_finite_count, model_.count > 0, state.min, state.max,
      model_.input_count);
    fbb.Finish(fb);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto const fb
      = flatbuffer<fbs::aggregation::TDigest>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN(
        "failed to restore `tdigest` aggregation instance: invalid FlatBuffer");
      return false;
    }
    if ((*fb)->compression() != model_.compression) {
      TENZIR_WARN("failed to restore `tdigest` aggregation instance: "
                  "mismatching compression");
      return false;
    }
    auto state = detail::tdigest_state{};
    state.means.assign((*fb)->means()->begin(), (*fb)->means()->end());
    state.weights.assign((*fb)->weights()->begin(), (*fb)->weights()->end());
    state.min = (*fb)->min();
    state.max = (*fb)->max();
    auto restored = make_model((*fb)->compression(), (*fb)->input_count(),
                               (*fb)->count(), (*fb)->null_count(),
                               (*fb)->non_finite_count(), std::move(state));
    if (not restored) {
      TENZIR_WARN("failed to restore `tdigest` aggregation instance: {}",
                  restored.unwrap_err());
      return false;
    }
    model_ = std::move(restored).unwrap();
    failed_ = false;
    return true;
  }

  auto reset() -> void override {
    // Deliberately keep the warning flags: they deduplicate diagnostics over
    // the lifetime of the instance, not per row.
    model_.input_count = 0;
    model_.count = 0;
    model_.null_count = 0;
    model_.non_finite_count = 0;
    model_.digest.reset();
    failed_ = false;
  }

private:
  auto fail(std::string_view message, session ctx) -> void {
    failed_ = true;
    if (not warned_failure_) {
      warned_failure_ = true;
      diagnostic::warning("`tdigest` failed: {}", message)
        .primary(expr_)
        .emit(ctx);
    }
  }

  ast::expression expr_;
  model model_;
  bool failed_ = false;
  bool warned_type_ = false;
  bool warned_failure_ = false;
};

auto make_nova_record(model const& m) -> nova::Data {
  auto const state = m.digest.save();
  auto centroids = nova::List{};
  centroids.reserve(state.means.size());
  for (auto i = size_t{0}; i < state.means.size(); ++i) {
    centroids.emplace_back(nova::Record{
      {"mean", state.means[i]},
      {"weight", state.weights[i]},
    });
  }
  return nova::Record{
    {"model", std::string{model_name}},
    {"version", model_version},
    {"input_count", m.input_count},
    {"count", m.count},
    {"null_count", m.null_count},
    {"compression", uint64_t{m.compression}},
    {"non_finite_count", m.non_finite_count},
    {"min", m.count > 0 ? nova::Data{state.min} : nova::Data{}},
    {"max", m.count > 0 ? nova::Data{state.max} : nova::Data{}},
    {"centroids", std::move(centroids)},
  };
}

struct TdigestArgs {
  nova::ValueArgument x;
  located<uint64_t> compression{100, location::unknown};
};

class Tdigest {
public:
  explicit Tdigest(uint32_t compression) : model_{compression} {
  }

  template <class Tag>
  auto add(nova::RowView<Tag> value, location source, diagnostic_handler& dh,
           nova::WarnOnce& warned_type) -> void {
    if (failed_) {
      return;
    }
    auto next = checked_add(model_.input_count, uint64_t{1});
    if (not next) {
      failed_ = true;
      diagnostic::warning("`tdigest` failed: `input_count` overflow")
        .primary(source)
        .emit(dh);
      return;
    }
    model_.input_count = *next;
    if constexpr (std::same_as<Tag, nova::Null>) {
      ++model_.null_count;
    } else if constexpr (concepts::one_of<Tag, nova::Int, nova::UInt,
                                          nova::Float>) {
      auto x = static_cast<double>(*value);
      if (std::isfinite(x)) {
        ++model_.count;
        model_.digest.add(x);
      } else {
        ++model_.non_finite_count;
      }
    } else {
      warned_type(dh, diagnostic::warning("expected `int`, `uint`, or `float`, "
                                          "got `{}`; skipping these values",
                                          nova::Type<Tag>::static_name)
                        .primary(source));
    }
  }

  auto get() const -> nova::Data {
    return failed_ ? nova::Data{} : make_nova_record(model_);
  }

private:
  model model_;
  bool failed_ = false;
};

class TdigestFunction {
public:
  static auto eval(TdigestArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto warned_type = nova::WarnOnce{};
    return nova::aggregate_lists(
      args.x, frame, [&](auto const& elements, auto& builder) {
        auto digest = Tdigest{static_cast<uint32_t>(args.compression.inner)};
        elements.for_each([&](auto value) {
          digest.add(value, args.x.source, frame, warned_type);
        });
        nova::append_data(builder, digest.get());
      });
  }

  auto update(TdigestArgs const& args, nova::EvalFrame frame) -> void {
    if (not digest_) {
      digest_.emplace(static_cast<uint32_t>(args.compression.inner));
    }
    sketch::visit(args.x.data, frame.mask(), [&](auto value) {
      digest_->add(value, args.x.source, frame, warned_type_);
    });
  }

  auto get() const -> nova::Data {
    return digest_ ? digest_->get() : nova::Data{};
  }

  auto reset() -> void {
    // Deduplicate type warnings over the accumulator's lifetime, not per window.
    digest_ = None{};
  }

private:
  Option<Tdigest> digest_;
  nova::WarnOnce warned_type_;
};

// Read the persisted record directly, without converting through Arrow or
// assuming that its numeric fields kept their physical types after JSON.
auto read_digest(nova::RowView<nova::Record> record)
  -> Option<detail::tdigest> {
  auto name = sketch::field<nova::String>(record, "model");
  auto version = sketch::unsigned_value(sketch::field(record, "version"));
  auto compression
    = sketch::unsigned_value(sketch::field(record, "compression"));
  auto input_count
    = sketch::unsigned_value(sketch::field(record, "input_count"));
  auto count = sketch::unsigned_value(sketch::field(record, "count"));
  auto null_count = sketch::unsigned_value(sketch::field(record, "null_count"));
  auto non_finite_count
    = sketch::unsigned_value(sketch::field(record, "non_finite_count"));
  auto min = sketch::number(sketch::field(record, "min"));
  auto max = sketch::number(sketch::field(record, "max"));
  auto centroids = sketch::field<nova::List>(record, "centroids");
  if (not name or **name != model_name or not version
      or *version != model_version or not compression
      or *compression < min_compression or *compression > max_compression
      or not input_count or not count or *count == 0 or not null_count
      or not non_finite_count or not min or not max or not centroids
      or static_cast<uint64_t>(centroids->length()) > *compression) {
    return None{};
  }
  auto classified = checked_add(*count, *null_count);
  if (not classified) {
    return None{};
  }
  classified = checked_add(*classified, *non_finite_count);
  if (not classified or *classified > *input_count) {
    return None{};
  }
  auto state = detail::tdigest_state{
    .means = {}, .weights = {}, .min = *min, .max = *max};
  auto total_weight = uint64_t{0};
  for (auto entry : *centroids) {
    auto centroid = try_as<nova::RowView<nova::Record>>(entry);
    if (not centroid) {
      return None{};
    }
    auto mean = sketch::number(sketch::field(*centroid, "mean"));
    auto weight = sketch::number(sketch::field(*centroid, "weight"));
    if (not mean or not weight) {
      return None{};
    }
    auto integral_weight = checked_weight(*weight);
    if (not integral_weight) {
      return None{};
    }
    auto next = checked_add(total_weight, *integral_weight);
    if (not next) {
      return None{};
    }
    total_weight = *next;
    state.means.push_back(*mean);
    state.weights.push_back(*weight);
  }
  if (total_weight != *count) {
    return None{};
  }
  auto digest = detail::tdigest{static_cast<uint32_t>(*compression)};
  if (not digest.restore(state)) {
    return None{};
  }
  return digest;
}

class plugin final : public aggregation_plugin,
                     public nova::AggregationPlugin,
                     public model_distance_plugin {
public:
  auto name() const -> std::string override {
    return std::string{model_name};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<TdigestArgs, TdigestFunction>{};
    d.positional("x", &TdigestArgs::x, "number");
    d.named_optional("compression", &TdigestArgs::compression);
    d.validate(
      [](TdigestArgs& args, diagnostic_handler& dh) -> failure_or<void> {
        if (args.compression.inner < min_compression
            or args.compression.inner > max_compression) {
          diagnostic::error("`compression` must be in [{}, {}]",
                            min_compression, max_compression)
            .primary(args.compression)
            .emit(dh);
          return failure::promise();
        }
        return {};
      });
    return std::move(d).finish();
  }

  auto model_version() const -> uint64_t override {
    return ::tenzir::plugins::tdigest::model_version;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto compression = Option<located<uint64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number")
          .named("compression", compression)
          .parse(inv, ctx));
    auto const value = compression ? compression->inner : uint64_t{100};
    if (value < min_compression or value > max_compression) {
      if (compression) {
        diagnostic::error("`compression` must be in [{}, {}]", min_compression,
                          max_compression)
          .primary(*compression)
          .emit(ctx);
      } else {
        diagnostic::error("`compression` must be in [{}, {}]", min_compression,
                          max_compression)
          .primary(inv.call)
          .emit(ctx);
      }
      return failure::promise();
    }
    return std::make_unique<instance>(std::move(expr),
                                      static_cast<uint32_t>(value));
  }

  auto list_call_result_type(type const&) const -> Option<type> override {
    return result_type();
  }

  auto make_model_merge_state(record_view3 rec) const
    -> Result<Box<model_merge_state>, std::string> override {
    TRY(auto parsed, parse_model(rec));
    return Box<model_merge_state>{merge_state{std::move(parsed)}};
  }

  auto model_distance(record_view3 lhs, record_view3 rhs,
                      std::string_view method) const
    -> Result<Option<double>, std::string> override {
    if (method != "kolmogorov_smirnov" and method != "wasserstein") {
      return Err{fmt::format("model `{}` does not support distance method `{}`",
                             name(), method)};
    }
    auto lhs_model = parse_model(lhs);
    if (not lhs_model) {
      return Err{
        fmt::format("malformed t-digest model: {}", lhs_model.unwrap_err())};
    }
    auto rhs_model = parse_model(rhs);
    if (not rhs_model) {
      return Err{
        fmt::format("malformed t-digest model: {}", rhs_model.unwrap_err())};
    }
    auto lhs_value = std::move(lhs_model).unwrap();
    auto rhs_value = std::move(rhs_model).unwrap();
    if (lhs_value.count == 0 or rhs_value.count == 0) {
      return None{};
    }
    if (method == "kolmogorov_smirnov") {
      return Option{lhs_value.digest.ks_distance(rhs_value.digest)};
    }
    TENZIR_ASSERT(method == "wasserstein");
    return Option{lhs_value.digest.wasserstein_distance(rhs_value.digest)};
  }
};

enum class unary_operation { quantile, cdf };

struct QueryArgs {
  nova::ValueArgument model;
  nova::ValueArgument value;
  location call;
};

template <unary_operation Operation>
class QueryFunction {
public:
  static auto eval(QueryArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto warned_value = nova::WarnOnce{};
    auto warned_type = nova::WarnOnce{};
    return nova::apply_kernel<2>(
      frame,
      Operation == unary_operation::quantile ? "tdigest_quantile"
                                             : "tdigest_cdf",
      {args.model, args.value}, args.call,
      [&](diagnostic_handler& dh, auto model,
          auto value) -> Option<nova::Float> {
        using Value = decltype(value);
        if constexpr (std::same_as<Value, nova::Null>) {
          return None{};
        } else if constexpr (not concepts::one_of<Value, nova::Int, nova::UInt,
                                                  nova::Float>) {
          warned_type(dh, diagnostic::warning("expected `number`")
                            .primary(args.value.source));
          return None{};
        } else if constexpr (std::same_as<decltype(model),
                                          nova::RowView<nova::Record>>) {
          auto digest = read_digest(model);
          if (not digest) {
            return None{};
          }
          auto x = static_cast<double>(value);
          if (not std::isfinite(x)
              or (Operation == unary_operation::quantile
                  and (x < 0.0 or x > 1.0))) {
            warned_value(dh, diagnostic::warning(
                               Operation == unary_operation::quantile
                                 ? "expected a finite quantile in [0.0, 1.0]"
                                 : "expected a finite query value")
                               .primary(args.value.source));
            return None{};
          }
          if constexpr (Operation == unary_operation::quantile) {
            return digest->quantile(x);
          } else {
            return digest->cdf(x);
          }
        } else {
          return None{};
        }
      });
  }
};

template <unary_operation Operation>
class unary_function final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<QueryArgs, QueryFunction<Operation>>{};
    d.positional("model", &QueryArgs::model, "record");
    d.positional(Operation == unary_operation::quantile ? "q" : "x",
                 &QueryArgs::value, "number");
    d.call_location(&QueryArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    if constexpr (Operation == unary_operation::quantile) {
      return "tdigest_quantile";
    }
    return "tdigest_cdf";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto model_expr = ast::expression{};
    auto value_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("model", model_expr, "record")
          .positional(Operation == unary_operation::quantile ? "q" : "x",
                      value_expr, "number")
          .parse(inv, ctx));
    return function_use::make([model_expr = std::move(model_expr),
                               value_expr = std::move(value_expr)](
                                evaluator eval, session ctx) -> multi_series {
      return map_series(
        eval(model_expr), eval(value_expr),
        [&](series models, series values) -> series {
          auto builder = double_type::make_arrow_builder(arrow_memory_pool());
          auto numbers = detail::extract_numbers(values);
          if (not numbers) {
            diagnostic::warning("expected `number`, got `{}`",
                                values.type.kind())
              .primary(value_expr)
              .emit(ctx);
            return series::null(double_type{}, models.length());
          }
          if (is<null_type>(models.type)) {
            return series::null(double_type{}, models.length());
          }
          if (models.type != result_type()) {
            return series::null(double_type{}, models.length());
          }
          auto records = models.as<record_type>();
          TENZIR_ASSERT(records);
          auto count_field = records->field("count");
          auto compression_field = records->field("compression");
          auto min_field = records->field("min");
          auto max_field = records->field("max");
          auto centroids_field = records->field("centroids");
          TENZIR_ASSERT(count_field and compression_field and min_field
                        and max_field and centroids_field);
          auto counts = count_field->as<uint64_type>();
          auto compressions = compression_field->as<uint64_type>();
          auto mins = min_field->as<double_type>();
          auto maxs = max_field->as<double_type>();
          auto centroids = centroids_field->as<list_type>();
          TENZIR_ASSERT(counts and compressions and mins and maxs
                        and centroids);
          auto centroid_records = centroids->list_values().as<record_type>();
          TENZIR_ASSERT(centroid_records);
          auto means_field = centroid_records->field("mean");
          auto weights_field = centroid_records->field("weight");
          TENZIR_ASSERT(means_field and weights_field);
          auto means = means_field->as<double_type>();
          auto weights = weights_field->as<double_type>();
          TENZIR_ASSERT(means and weights);
          auto warned_value = false;
          for (auto row = int64_t{0}; row < models.length(); ++row) {
            auto const& value = (*numbers)[row];
            if (records->array->IsNull(row) or not value) {
              check(builder->AppendNull());
              continue;
            }
            if (counts->array->IsNull(row)) {
              check(builder->AppendNull());
              continue;
            }
            if (counts->array->Value(row) == 0) {
              check(builder->AppendNull());
              continue;
            }
            if (not std::isfinite(*value)
                or (Operation == unary_operation::quantile
                    and (*value < 0.0 or *value > 1.0))) {
              if (not warned_value) {
                warned_value = true;
                if constexpr (Operation == unary_operation::quantile) {
                  diagnostic::warning(
                    "expected a finite quantile in [0.0, 1.0]")
                    .primary(value_expr)
                    .emit(ctx);
                } else {
                  diagnostic::warning("expected a finite query value")
                    .primary(value_expr)
                    .emit(ctx);
                }
              }
              check(builder->AppendNull());
              continue;
            }
            if (compressions->array->IsNull(row) or mins->array->IsNull(row)
                or maxs->array->IsNull(row) or centroids->array->IsNull(row)) {
              check(builder->AppendNull());
              continue;
            }
            auto state = detail::tdigest_state{
              .means = {},
              .weights = {},
              .min = mins->array->Value(row),
              .max = maxs->array->Value(row),
            };
            auto const begin = centroids->array->value_offset(row);
            auto const end = centroids->array->value_offset(row + 1);
            state.means.reserve(end - begin);
            state.weights.reserve(end - begin);
            auto valid = true;
            for (auto i = begin; i < end; ++i) {
              if (means->array->IsNull(i) or weights->array->IsNull(i)) {
                valid = false;
                break;
              }
              state.means.push_back(means->array->Value(i));
              state.weights.push_back(weights->array->Value(i));
            }
            if (not valid) {
              check(builder->AppendNull());
              continue;
            }
            auto digest = detail::tdigest{
              static_cast<uint32_t>(compressions->array->Value(row))};
            if (not digest.restore(std::move(state))) {
              check(builder->AppendNull());
              continue;
            }
            if constexpr (Operation == unary_operation::quantile) {
              check(builder->Append(digest.quantile(*value)));
            } else {
              check(builder->Append(digest.cdf(*value)));
            }
          }
          return series{double_type{}, finish(*builder)};
        });
    });
  }
};

using tdigest_quantile = unary_function<unary_operation::quantile>;
using tdigest_cdf = unary_function<unary_operation::cdf>;

} // namespace

} // namespace tenzir::plugins::tdigest

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tdigest::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::tdigest::tdigest_quantile)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::tdigest::tdigest_cdf)
