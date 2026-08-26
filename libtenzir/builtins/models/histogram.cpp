//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/checked_math.hpp>
#include <tenzir/detail/distribution.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/model.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <string_view>
#include <tuple>
#include <vector>

#include "model_helpers.hpp"

namespace tenzir::plugins::histogram {

namespace {

using namespace si_literals;

/// The maximum number of regular bins a histogram model may have.
constexpr auto max_bins = uint64_t{16_Ki};

/// The histogram model identity and schema version.
constexpr auto model_name = std::string_view{"histogram"};
constexpr auto model_version = uint64_t{1};

/// The only model kind produced and accepted by this implementation.
constexpr auto fixed_width_kind = std::string_view{"fixed_width"};

/// Computes the represented edges `edge[i] = start + i * width` for
/// `0 <= i <= n`. Returns an empty optional when any edge is non-finite or
/// floating-point rounding collapses adjacent edges.
auto make_edges(double start, double width, uint64_t n)
  -> Option<std::vector<double>> {
  auto edges = std::vector<double>{};
  edges.reserve(n + 1);
  for (auto i = uint64_t{0}; i <= n; ++i) {
    auto const edge = std::fma(static_cast<double>(i), width, start);
    if (not std::isfinite(edge)) {
      return None{};
    }
    if (i > 0 and not(edges.back() < edge)) {
      return None{};
    }
    edges.push_back(edge);
  }
  return edges;
}

/// Returns the bucket index for a finite value `x` given represented edges.
/// The result is -1 for underflow, `n` for overflow, and the regular bin
/// index otherwise. The final represented edge belongs to the final regular
/// bin.
auto find_bucket_index(std::vector<double> const& edges, double x) -> int64_t {
  auto const n = static_cast<int64_t>(edges.size()) - 1;
  if (x < edges.front()) {
    return -1;
  }
  if (x > edges.back()) {
    return n;
  }
  // First edge strictly greater than x; the bin left of it contains x.
  auto const it = std::ranges::upper_bound(edges, x);
  auto index = static_cast<int64_t>(it - edges.begin()) - 1;
  if (index == n) {
    // x equals the final edge, which belongs to the final regular bin.
    index = n - 1;
  }
  return index;
}

/// A validated in-memory histogram model.
struct model {
  double start = 0.0;
  double width = 0.0;
  std::vector<double> edges;
  std::vector<uint64_t> counts;
  uint64_t underflow = 0;
  uint64_t overflow = 0;
  uint64_t input_count = 0;
  uint64_t count = 0;
  uint64_t null_count = 0;
  uint64_t non_finite_count = 0;
  double min = 0.0;
  double max = 0.0;
};

auto view_to_double(data_view3 v) -> Option<double> {
  auto result = model_double(v);
  if (not result) {
    return None{};
  }
  return std::move(result).unwrap();
}

template <class Value>
auto number_to_double(Value const& value) -> Option<double> {
  return match(
    value,
    [](double x) -> Option<double> {
      return x;
    },
    [](int64_t x) -> Option<double> {
      return static_cast<double>(x);
    },
    [](uint64_t x) -> Option<double> {
      return static_cast<double>(x);
    },
    [](auto const&) -> Option<double> {
      return None{};
    });
}

auto view_to_count(data_view3 v) -> Option<uint64_t> {
  auto result = model_uint64(v);
  if (not result) {
    return None{};
  }
  return std::move(result).unwrap();
}

/// Parses and validates a histogram model from a record view. Additional
/// record fields are ignored for forward compatibility.
auto parse_model(record_view3 rec) -> Result<model, std::string> {
  TRY(auto envelope, parse_model_envelope(rec));
  if (envelope.model != model_name) {
    return Err{
      fmt::format("expected model `{}`, got `{}`", model_name, envelope.model)};
  }
  if (envelope.version != model_version) {
    return Err{
      fmt::format("unsupported histogram model version {}; expected {}",
                  envelope.version, model_version)};
  }
  auto result = model{};
  auto kind = Option<std::string_view>{};
  auto count = Option<uint64_t>{};
  auto null_count = Option<uint64_t>{};
  auto non_finite_count = Option<uint64_t>{};
  auto min = Option<double>{};
  auto max = Option<double>{};
  auto has_min = false;
  auto has_max = false;
  auto start = Option<double>{};
  auto width = Option<double>{};
  auto underflow = Option<uint64_t>{};
  auto overflow = Option<uint64_t>{};
  auto bins = Option<view3<list>>{};
  for (auto const& [key, value] : rec) {
    if (key == "kind") {
      kind = try_as<std::string_view>(value)
               ? Option{as<std::string_view>(value)}
               : None{};
      if (not kind) {
        return Err{"`kind` must be a string"};
      }
    } else if (key == "count") {
      count = view_to_count(value);
      if (not count) {
        return Err{"`count` must be a non-negative integer"};
      }
    } else if (key == "null_count") {
      null_count = view_to_count(value);
      if (not null_count) {
        return Err{"`null_count` must be a non-negative integer"};
      }
    } else if (key == "non_finite_count") {
      non_finite_count = view_to_count(value);
      if (not non_finite_count) {
        return Err{"`non_finite_count` must be a non-negative integer"};
      }
    } else if (key == "min") {
      has_min = true;
      if (not is<caf::none_t>(value)) {
        min = view_to_double(value);
      }
    } else if (key == "max") {
      has_max = true;
      if (not is<caf::none_t>(value)) {
        max = view_to_double(value);
      }
    } else if (key == "start") {
      start = view_to_double(value);
      if (not start) {
        return Err{"`start` must be a number"};
      }
    } else if (key == "width") {
      width = view_to_double(value);
      if (not width) {
        return Err{"`width` must be a number"};
      }
    } else if (key == "underflow") {
      underflow = view_to_count(value);
      if (not underflow) {
        return Err{"`underflow` must be a non-negative integer"};
      }
    } else if (key == "overflow") {
      overflow = view_to_count(value);
      if (not overflow) {
        return Err{"`overflow` must be a non-negative integer"};
      }
    } else if (key == "bins") {
      if (auto const* xs = try_as<view3<list>>(value)) {
        bins.emplace(*xs);
      } else {
        return Err{"`bins` must be a list of records"};
      }
    }
  }
  if (not kind) {
    return Err{"missing field `kind`"};
  }
  if (*kind != fixed_width_kind) {
    return Err{fmt::format("unsupported model kind `{}`", *kind)};
  }
  for (auto const& [name, present] : {
         std::pair{"count", count.has_value()},
         std::pair{"null_count", null_count.has_value()},
         std::pair{"non_finite_count", non_finite_count.has_value()},
         std::pair{"min", has_min},
         std::pair{"max", has_max},
         std::pair{"start", start.has_value()},
         std::pair{"width", width.has_value()},
         std::pair{"underflow", underflow.has_value()},
         std::pair{"overflow", overflow.has_value()},
         std::pair{"bins", bins.has_value()},
       }) {
    if (not present) {
      return Err{fmt::format("missing field `{}`", name)};
    }
  }
  for (auto bin : *bins) {
    auto const* bin_rec = try_as<view3<record>>(bin);
    if (not bin_rec) {
      return Err{"invalid histogram model shape"};
    }
    auto lower = Option<double>{};
    auto upper = Option<double>{};
    auto bin_count = Option<uint64_t>{};
    for (auto const& [key, value] : *bin_rec) {
      if (key == "lower") {
        lower = view_to_double(value);
      } else if (key == "upper") {
        upper = view_to_double(value);
      } else if (key == "count") {
        bin_count = view_to_count(value);
      }
    }
    if (not lower or not upper or not bin_count) {
      return Err{"invalid histogram model shape"};
    }
    if (result.edges.empty()) {
      result.edges.push_back(*lower);
    }
    result.edges.push_back(*upper);
    result.counts.push_back(*bin_count);
  }
  if (result.counts.empty()) {
    return Err{"invalid histogram model shape"};
  }
  result.start = *start;
  result.width = *width;
  result.underflow = *underflow;
  result.overflow = *overflow;
  result.input_count = envelope.input_count;
  result.count = *count;
  result.null_count = *null_count;
  result.non_finite_count = *non_finite_count;
  if (min) {
    result.min = *min;
  }
  if (max) {
    result.max = *max;
  }
  return result;
}

/// Materializes a validated model in the public field order.
auto model_to_data(model const& m) -> data {
  auto bins = list{};
  bins.reserve(m.counts.size());
  for (auto i = size_t{0}; i < m.counts.size(); ++i) {
    bins.emplace_back(record{
      {"lower", m.edges[i]},
      {"upper", m.edges[i + 1]},
      {"count", m.counts[i]},
    });
  }
  return record{
    {"model", std::string{model_name}},
    {"version", model_version},
    {"input_count", m.input_count},
    {"count", m.count},
    {"null_count", m.null_count},
    {"kind", std::string{fixed_width_kind}},
    {"non_finite_count", m.non_finite_count},
    {"min", m.count > 0 ? data{m.min} : data{}},
    {"max", m.count > 0 ? data{m.max} : data{}},
    {"start", m.start},
    {"width", m.width},
    {"underflow", m.underflow},
    {"overflow", m.overflow},
    {"bins", std::move(bins)},
  };
}

/// Computes the Jensen-Shannon divergence in nats between two compatible,
/// nonempty models over the outcomes underflow, regular bins, overflow.
auto js_divergence(model const& p, model const& q) -> double {
  auto result = detail::jensen_shannon_accumulator{
    static_cast<double>(p.count), static_cast<double>(q.count)};
  result.add(p.underflow, q.underflow);
  for (auto i = size_t{0}; i < p.counts.size(); ++i) {
    result.add(p.counts[i], q.counts[i]);
  }
  result.add(p.overflow, q.overflow);
  return result.value();
}

/// Checks whether two valid models have identical fixed-width geometry.
auto compatible(model const& p, model const& q) -> bool {
  return p.start == q.start and p.width == q.width
         and p.counts.size() == q.counts.size() and p.edges == q.edges;
}

class histogram_merge_state final : public model_merge_state {
public:
  explicit histogram_merge_state(model state) : state_{std::move(state)} {
  }

  auto merge(record_view3 input) -> Result<void, std::string> override {
    TRY(auto parsed, parse_model(input));
    if (not compatible(state_, parsed)) {
      return Err{
        "histogram kind, start, width, bin count, and represented edges must "
        "match exactly"};
    }
    // Merge into a copy so overflow leaves the existing state unchanged.
    auto merged = state_;
    auto const add
      = [](uint64_t lhs, uint64_t rhs,
           std::string_view field) -> Result<uint64_t, std::string> {
      auto result = checked_add(lhs, rhs);
      if (not result) {
        return Err{fmt::format("`{}` counter overflows", field)};
      }
      return std::move(result).unwrap();
    };
    auto input_count
      = add(merged.input_count, parsed.input_count, "input_count");
    if (not input_count) {
      return Err{std::move(input_count.unwrap_err())};
    }
    auto count = add(merged.count, parsed.count, "count");
    if (not count) {
      return Err{std::move(count.unwrap_err())};
    }
    auto null_count = add(merged.null_count, parsed.null_count, "null_count");
    if (not null_count) {
      return Err{std::move(null_count.unwrap_err())};
    }
    auto non_finite_count = add(merged.non_finite_count,
                                parsed.non_finite_count, "non_finite_count");
    if (not non_finite_count) {
      return Err{std::move(non_finite_count.unwrap_err())};
    }
    auto underflow = add(merged.underflow, parsed.underflow, "underflow");
    if (not underflow) {
      return Err{std::move(underflow.unwrap_err())};
    }
    auto overflow = add(merged.overflow, parsed.overflow, "overflow");
    if (not overflow) {
      return Err{std::move(overflow.unwrap_err())};
    }
    for (auto i = size_t{0}; i < merged.counts.size(); ++i) {
      auto bin_count = add(merged.counts[i], parsed.counts[i], "bin");
      if (not bin_count) {
        return Err{fmt::format("bin {}: {}", i, bin_count.unwrap_err())};
      }
      merged.counts[i] = std::move(bin_count).unwrap();
    }
    if (parsed.count > 0) {
      if (state_.count > 0) {
        merged.min = std::min(merged.min, parsed.min);
        merged.max = std::max(merged.max, parsed.max);
      } else {
        merged.min = parsed.min;
        merged.max = parsed.max;
      }
    }
    merged.input_count = std::move(input_count).unwrap();
    merged.count = std::move(count).unwrap();
    merged.null_count = std::move(null_count).unwrap();
    merged.non_finite_count = std::move(non_finite_count).unwrap();
    merged.underflow = std::move(underflow).unwrap();
    merged.overflow = std::move(overflow).unwrap();
    state_ = std::move(merged);
    return {};
  }

  auto get() const -> data override {
    return model_to_data(state_);
  }

private:
  model state_;
};

auto result_type() -> type {
  return model_record_type({
    {"kind", string_type{}},
    {"non_finite_count", uint64_type{}},
    {"min", double_type{}},
    {"max", double_type{}},
    {"start", double_type{}},
    {"width", double_type{}},
    {"underflow", uint64_type{}},
    {"overflow", uint64_type{}},
    {"bins", list_type{record_type{
               {"lower", double_type{}},
               {"upper", double_type{}},
               {"count", uint64_type{}},
             }}},
  });
}

class histogram_instance final : public aggregation_instance {
public:
  histogram_instance(ast::expression expr, double start, double width,
                     std::vector<double> edges)
    : expr_{std::move(expr)},
      start_{start},
      width_{width},
      edges_{std::move(edges)},
      counts_(edges_.size() - 1, 0) {
  }

  void update(table_slice const& input, session ctx) override {
    for (auto& arg : eval(expr_, input, ctx)) {
      if (not add_to_counter(input_count_,
                             static_cast<uint64_t>(arg.array->length()),
                             "input_count", ctx)) {
        continue;
      }
      auto const f = detail::overload{
        [&]<concepts::one_of<double_type, int64_type, uint64_type> Type>(
          Type const&) {
          auto const& array = as<type_to_arrow_array_t<Type>>(*arg.array);
          for (auto value : values3(array)) {
            if (not value) {
              add_to_counter(null_count_, 1, "null_count", ctx);
              continue;
            }
            add(static_cast<double>(*value), ctx);
          }
        },
        [&](null_type const&) {
          add_to_counter(null_count_,
                         static_cast<uint64_t>(arg.array->length()),
                         "null_count", ctx);
        },
        [&]<concepts::one_of<duration_type, time_type> Type>(Type const&) {
          add_to_counter(null_count_,
                         static_cast<uint64_t>(arg.array->null_count()),
                         "null_count", ctx);
          if (not warned_temporal_) {
            warned_temporal_ = true;
            diagnostic::warning("`histogram` does not support `{}` values yet; "
                                "skipping them",
                                arg.type.kind())
              .primary(expr_)
              .hint("convert to a number first, e.g., `x / 1s` for durations "
                    "or `x.since_epoch() / 1s` for timestamps")
              .emit(ctx);
          }
        },
        [&](auto const&) {
          add_to_counter(null_count_,
                         static_cast<uint64_t>(arg.array->null_count()),
                         "null_count", ctx);
          if (not warned_type_) {
            warned_type_ = true;
            diagnostic::warning("expected `int`, `uint`, or `float`, got "
                                "`{}`; skipping these values",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
          }
        },
      };
      match(arg.type, f);
    }
  }

  auto get() const -> data override {
    return model_to_data(model{
      .start = start_,
      .width = width_,
      .edges = edges_,
      .counts = counts_,
      .underflow = underflow_,
      .overflow = overflow_,
      .input_count = input_count_,
      .count = count_,
      .null_count = null_count_,
      .non_finite_count = non_finite_count_,
      .min = min_,
      .max = max_,
    });
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto const fb_counts = fbb.CreateVector(counts_);
    auto const fb_histogram = fbs::aggregation::CreateHistogram(
      fbb, fb_counts, start_, width_, underflow_, overflow_, null_count_,
      non_finite_count_, count_, count_ > 0, min_, max_, input_count_);
    fbb.Finish(fb_histogram);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto const fb
      = flatbuffer<fbs::aggregation::Histogram>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `histogram` aggregation instance: "
                  "invalid FlatBuffer");
      return false;
    }
    auto const* counts = (*fb)->counts();
    if (not counts or counts->size() != counts_.size()) {
      TENZIR_WARN("failed to restore `histogram` aggregation instance: "
                  "mismatching bin count");
      return false;
    }
    if ((*fb)->start() != start_ or (*fb)->width() != width_) {
      TENZIR_WARN("failed to restore `histogram` aggregation instance: "
                  "mismatching configuration");
      return false;
    }
    auto total = checked_sum(*counts);
    if (total) {
      total = checked_add(*total, (*fb)->underflow());
    }
    if (total) {
      total = checked_add(*total, (*fb)->overflow());
    }
    if (not total or *total != (*fb)->count()) {
      TENZIR_WARN("failed to restore `histogram` aggregation instance: "
                  "inconsistent counters");
      return false;
    }
    if ((*fb)->has_extrema() != ((*fb)->count() > 0)
        or ((*fb)->has_extrema()
            and (not std::isfinite((*fb)->min())
                 or not std::isfinite((*fb)->max())
                 or (*fb)->min() > (*fb)->max()))) {
      TENZIR_WARN("failed to restore `histogram` aggregation instance: "
                  "inconsistent extrema");
      return false;
    }
    auto classified_count = checked_sum(std::array{
      (*fb)->count(), (*fb)->null_count(), (*fb)->non_finite_count()});
    if (not classified_count or *classified_count > (*fb)->input_count()) {
      TENZIR_WARN("failed to restore `histogram` aggregation instance: "
                  "classified counters exceed input count");
      return false;
    }
    std::ranges::copy(*counts, counts_.begin());
    underflow_ = (*fb)->underflow();
    overflow_ = (*fb)->overflow();
    input_count_ = (*fb)->input_count();
    null_count_ = (*fb)->null_count();
    non_finite_count_ = (*fb)->non_finite_count();
    count_ = (*fb)->count();
    min_ = (*fb)->min();
    max_ = (*fb)->max();
    return true;
  }

  auto reset() -> void override {
    // Do not touch the configuration (`start_`, `width_`, `edges_`) or the
    // warning flags, which deduplicate diagnostics over the lifetime of the
    // instance rather than per row; only clear the dynamic state.
    std::ranges::fill(counts_, 0);
    underflow_ = 0;
    overflow_ = 0;
    input_count_ = 0;
    null_count_ = 0;
    non_finite_count_ = 0;
    count_ = 0;
    min_ = 0.0;
    max_ = 0.0;
  }

private:
  auto add_to_counter(uint64_t& counter, uint64_t amount,
                      std::string_view field, session ctx) -> bool {
    auto result = checked_add(counter, amount);
    if (not result) {
      warn_overflow(field, ctx);
      return false;
    }
    counter = *result;
    return true;
  }

  auto warn_overflow(std::string_view field, session ctx) -> void {
    if (warned_overflow_) {
      return;
    }
    warned_overflow_ = true;
    diagnostic::warning("`histogram` {} counter overflowed; skipping values",
                        field)
      .primary(expr_)
      .emit(ctx);
  }

  void add(double x, session ctx) {
    if (not std::isfinite(x)) {
      std::ignore
        = add_to_counter(non_finite_count_, 1, "non_finite_count", ctx);
      return;
    }
    auto const index = find_bucket_index(edges_, x);
    auto* bucket = index == -1 ? &underflow_
                   : index == static_cast<int64_t>(counts_.size())
                     ? &overflow_
                     : &counts_[index];
    auto const next_count = checked_add(count_, uint64_t{1});
    auto const next_bucket = checked_add(*bucket, uint64_t{1});
    if (not next_count) {
      warn_overflow("count", ctx);
      return;
    }
    if (not next_bucket) {
      warn_overflow("bucket", ctx);
      return;
    }
    if (count_ == 0) {
      min_ = x;
      max_ = x;
    } else {
      min_ = std::min(min_, x);
      max_ = std::max(max_, x);
    }
    count_ = *next_count;
    *bucket = *next_bucket;
  }

  ast::expression expr_;
  // Configuration.
  double start_;
  double width_;
  std::vector<double> edges_;
  // Dynamic state.
  std::vector<uint64_t> counts_;
  uint64_t underflow_ = 0;
  uint64_t overflow_ = 0;
  uint64_t input_count_ = 0;
  uint64_t null_count_ = 0;
  uint64_t non_finite_count_ = 0;
  uint64_t count_ = 0;
  double min_ = 0.0;
  double max_ = 0.0;
  bool warned_type_ = false;
  bool warned_temporal_ = false;
  bool warned_overflow_ = false;
};

class histogram_plugin final : public aggregation_plugin,
                               public model_divergence_plugin {
public:
  auto name() const -> std::string override {
    return "histogram";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto model_version() const -> uint64_t override {
    return histogram::model_version;
  }

  auto make_model_merge_state(record_view3 input) const
    -> Result<Box<model_merge_state>, std::string> override {
    TRY(auto parsed, parse_model(input));
    return Box<model_merge_state>{histogram_merge_state{std::move(parsed)}};
  }

  auto model_divergence(record_view3 lhs, record_view3 rhs,
                        std::string_view method) const
    -> Result<Option<double>, std::string> override {
    if (method != "jensen_shannon") {
      return Err{fmt::format(
        "model `{}` does not support divergence method `{}`", name(), method)};
    }
    auto lhs_result = parse_model(lhs);
    if (not lhs_result) {
      return Err{
        fmt::format("malformed histogram model: {}", lhs_result.unwrap_err())};
    }
    auto rhs_result = parse_model(rhs);
    if (not rhs_result) {
      return Err{
        fmt::format("malformed histogram model: {}", rhs_result.unwrap_err())};
    }
    auto lhs_model = std::move(lhs_result).unwrap();
    auto rhs_model = std::move(rhs_result).unwrap();
    if (not compatible(lhs_model, rhs_model)) {
      return Err{"histogram models have different represented edges"};
    }
    if (lhs_model.count == 0 or rhs_model.count == 0) {
      return None{};
    }
    return Option{js_divergence(lhs_model, rhs_model)};
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto bins = located<uint64_t>{};
    auto width = located<data>{};
    auto start = Option<located<data>>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number")
          .named("bins", bins)
          .named("width", width)
          .named("start", start)
          .parse(inv, ctx));
    auto failed = false;
    if (bins.inner == 0 or bins.inner > max_bins) {
      diagnostic::error("`bins` must be in [1, {}]", max_bins)
        .primary(bins)
        .emit(ctx);
      failed = true;
    }
    auto width_value = number_to_double(width.inner);
    if (not width_value or not std::isfinite(*width_value)
        or *width_value <= 0.0) {
      diagnostic::error("`width` must be a finite number greater than zero")
        .primary(width)
        .emit(ctx);
      failed = true;
    }
    auto start_value = Option<double>{0.0};
    if (start) {
      start_value = number_to_double(start->inner);
      if (not start_value or not std::isfinite(*start_value)) {
        diagnostic::error("`start` must be a finite number")
          .primary(*start)
          .emit(ctx);
        failed = true;
      }
    }
    if (failed) {
      return failure::promise();
    }
    auto edges = make_edges(*start_value, *width_value, bins.inner);
    if (not edges) {
      diagnostic::error("`bins`, `width`, and `start` do not produce finite, "
                        "strictly increasing bin edges")
        .primary(bins)
        .secondary(width)
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<histogram_instance>(
      std::move(expr), *start_value, *width_value, std::move(*edges));
  }

  auto list_call_result_type(type const&) const -> Option<type> override {
    return result_type();
  }
};

// -- lookup and comparison functions -----------------------------------------

class histogram_bucket final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "histogram_bucket";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto model_expr = ast::expression{};
    auto x_expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("model", model_expr, "record")
          .positional("x", x_expr, "number")
          .parse(inv, ctx));
    return function_use::make(
      [model_expr = std::move(model_expr), x_expr = std::move(x_expr)](
        evaluator eval, session ctx) -> multi_series {
        return map_series(
          eval(model_expr), eval(x_expr), [&](series ms, series xs) -> series {
            auto const bucket_ty = type{record_type{
              {"kind", string_type{}},
              {"index", int64_type{}},
              {"lower", double_type{}},
              {"upper", double_type{}},
              {"count", uint64_type{}},
            }};
            auto builder = series_builder{bucket_ty};
            auto const values = detail::extract_numbers(xs);
            if (not values) {
              diagnostic::warning("expected `number`, got `{}`", xs.type.kind())
                .primary(x_expr)
                .emit(ctx);
              return series::null(bucket_ty, ms.length());
            }
            if (is<null_type>(ms.type)) {
              return series::null(bucket_ty, ms.length());
            }
            if (ms.type != result_type()) {
              return series::null(bucket_ty, ms.length());
            }
            auto records = ms.as<record_type>();
            TENZIR_ASSERT(records);
            auto underflow_field = records->field("underflow");
            auto overflow_field = records->field("overflow");
            auto bins_field = records->field("bins");
            TENZIR_ASSERT(underflow_field and overflow_field and bins_field);
            auto underflows = underflow_field->as<uint64_type>();
            auto overflows = overflow_field->as<uint64_type>();
            auto bins = bins_field->as<list_type>();
            TENZIR_ASSERT(underflows and overflows and bins);
            auto bin_records = bins->list_values().as<record_type>();
            TENZIR_ASSERT(bin_records);
            auto lower_field = bin_records->field("lower");
            auto upper_field = bin_records->field("upper");
            auto count_field = bin_records->field("count");
            TENZIR_ASSERT(lower_field and upper_field and count_field);
            auto lowers = lower_field->as<double_type>();
            auto uppers = upper_field->as<double_type>();
            auto counts = count_field->as<uint64_type>();
            TENZIR_ASSERT(lowers and uppers and counts);
            auto warned_non_finite = false;
            for (auto row = int64_t{0}; row < ms.length(); ++row) {
              auto const& x = (*values)[row];
              if (records->array->IsNull(row) or not x) {
                builder.null();
                continue;
              }
              if (not std::isfinite(*x)) {
                if (not warned_non_finite) {
                  warned_non_finite = true;
                  diagnostic::warning("histogram query value must be finite")
                    .primary(x_expr)
                    .emit(ctx);
                }
                builder.null();
                continue;
              }
              if (bins->array->IsNull(row)) {
                builder.null();
                continue;
              }
              auto const begin = bins->array->value_offset(row);
              auto const end = bins->array->value_offset(row + 1);
              if (begin >= end or lowers->array->IsNull(begin)) {
                builder.null();
                continue;
              }
              auto edges = std::vector<double>{};
              edges.reserve(static_cast<size_t>(end - begin + 1));
              edges.push_back(lowers->array->Value(begin));
              auto valid = true;
              for (auto i = begin; i < end; ++i) {
                if (uppers->array->IsNull(i)) {
                  valid = false;
                  break;
                }
                edges.push_back(uppers->array->Value(i));
              }
              if (not valid) {
                builder.null();
                continue;
              }
              auto const n = end - begin;
              auto const index = find_bucket_index(edges, *x);
              if ((index == -1 and underflows->array->IsNull(row))
                  or (index == n and overflows->array->IsNull(row))
                  or (index >= 0 and index < n
                      and counts->array->IsNull(begin + index))) {
                builder.null();
                continue;
              }
              auto rec = builder.record();
              auto count = uint64_t{0};
              if (index == -1) {
                rec.field("kind").data(std::string{"underflow"});
                rec.field("index").data(int64_t{-1});
                rec.field("lower").data(caf::none);
                rec.field("upper").data(edges.front());
                count = underflows->array->Value(row);
              } else if (index == n) {
                rec.field("kind").data(std::string{"overflow"});
                rec.field("index").data(index);
                rec.field("lower").data(edges.back());
                rec.field("upper").data(caf::none);
                count = overflows->array->Value(row);
              } else {
                rec.field("kind").data(std::string{"regular"});
                rec.field("index").data(index);
                rec.field("lower").data(edges[index]);
                rec.field("upper").data(edges[index + 1]);
                count = counts->array->Value(begin + index);
              }
              rec.field("count").data(count);
            }
            return builder.finish_assert_one_array();
          });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::histogram

TENZIR_REGISTER_PLUGIN(tenzir::plugins::histogram::histogram_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::histogram::histogram_bucket)
