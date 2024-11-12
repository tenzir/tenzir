//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/context.hpp>
#include <tenzir/data.hpp>
#include <tenzir/dcso_bloom_filter.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/fbs/data.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/type.hpp>

#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <caf/error.hpp>
#include <caf/sum_type.hpp>

#include <memory>
#include <string>

namespace tenzir::plugins::bloom_filter {

namespace {

class bloom_filter_context final : public virtual context {
public:
  bloom_filter_context() noexcept = default;

  explicit bloom_filter_context(dcso_bloom_filter bloom_filter) noexcept
    : bloom_filter_{std::move(bloom_filter)} {
  }

  bloom_filter_context(uint64_t n, double p) : bloom_filter_{n, p} {
  }

  auto context_type() const -> std::string override {
    return "bloom-filter";
  }

  /// Emits context information for every event in `slice` in order.
  auto apply(series array, bool replace)
    -> caf::expected<std::vector<series>> override {
    (void)replace;
    auto builder = series_builder{};
    for (const auto& value : array.values()) {
      if (bloom_filter_.lookup(value)) {
        builder.data(true);
      } else {
        builder.null();
      }
    }
    return builder.finish();
  }

  auto apply2(const series& array, session ctx)
    -> std::vector<series> override {
    TENZIR_UNUSED(ctx);
    auto builder = series_builder{};
    for (const auto& value : array.values()) {
      if (bloom_filter_.lookup(value)) {
        builder.data(true);
      } else {
        builder.null();
      }
    }
    return builder.finish();
  }

  /// Inspects the context.
  auto show() const -> record override {
    return record{
      {"num_elements", bloom_filter_.num_elements()},
      {"parameters",
       record{
         {"m", bloom_filter_.parameters().m},
         {"n", bloom_filter_.parameters().n},
         {"p", bloom_filter_.parameters().p},
         {"k", bloom_filter_.parameters().k},
       }},
    };
  }

  auto dump() -> generator<table_slice> override {
    const auto* ptr
      = reinterpret_cast<const std::byte*>(bloom_filter_.data().data());
    auto size = bloom_filter_.data().size();
    auto data = std::basic_string<std::byte>{ptr, size};
    auto entry_builder = series_builder{};
    auto row = entry_builder.record();
    row.field("num_elements", bloom_filter_.num_elements());
    auto params = row.field("parameters").record();
    if (bloom_filter_.parameters().m) {
      params.field("m", *bloom_filter_.parameters().m);
    }
    if (bloom_filter_.parameters().n) {
      params.field("n", *bloom_filter_.parameters().n);
    }
    if (bloom_filter_.parameters().p) {
      params.field("p", *bloom_filter_.parameters().p);
    }
    if (bloom_filter_.parameters().k) {
      params.field("k", *bloom_filter_.parameters().k);
    }
    co_yield entry_builder.finish_assert_one_slice(
      fmt::format("tenzir.{}.info", context_type()));
  }

  /// Updates the context.
  auto update(table_slice slice, context_parameter_map parameters)
    -> caf::expected<context_update_result> override {
    TENZIR_ASSERT(slice.rows() != 0);
    if (caf::get<record_type>(slice.schema()).num_fields() == 0) {
      return caf::make_error(ec::invalid_argument,
                             "context update cannot handle empty input events");
    }
    if (not parameters.contains("key")) {
      return caf::make_error(ec::invalid_argument, "missing 'key' parameter");
    }
    auto key_column = [&]() -> caf::expected<offset> {
      if (not parameters.contains("key")) {
        return offset{0};
      }
      auto key_field = parameters["key"];
      if (not key_field) {
        return caf::make_error(ec::invalid_argument, "invalid 'key' parameter; "
                                                     "'key' must be a string");
      }
      auto key_column = slice.schema().resolve_key_or_concept_once(*key_field);
      if (not key_column) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("key '{}' does not exist in schema "
                                           "'{}'",
                                           *key_field, slice.schema()));
      }
      return std::move(*key_column);
    }();
    if (not key_column) {
      return std::move(key_column.error());
    }
    auto [key_type, key_array] = key_column->get(slice);
    auto context_array = std::static_pointer_cast<arrow::Array>(
      to_record_batch(slice)->ToStructArray().ValueOrDie());
    auto key_values = values(key_type, *key_array);
    auto key_values_list = list{};
    auto context_values = values(slice.schema(), *context_array);
    for (const auto& value : key_values) {
      auto materialized_key = materialize(value);
      bloom_filter_.add(materialized_key);
      key_values_list.emplace_back(std::move(materialized_key));
    }
    auto query_f
      = [key_values_list = std::move(key_values_list)](
          context_parameter_map, const std::vector<std::string>& fields)
      -> caf::expected<std::vector<expression>> {
      auto result = std::vector<expression>{};
      result.reserve(fields.size());
      for (const auto& field : fields) {
        auto lhs = to<operand>(field);
        TENZIR_ASSERT(lhs);
        result.emplace_back(predicate{
          *lhs,
          relational_operator::in,
          data{key_values_list},
        });
      }
      return result;
    };
    return context_update_result{
      .update_info = show(),
      .make_query = std::move(query_f),
    };
  }

  auto update2(const table_slice& events, const context_update_args& args,
               session ctx) -> failure_or<context_update_result> override {
    for (const auto& timeout :
         {args.create_timeout, args.write_timeout, args.read_timeout}) {
      if (timeout) {
        diagnostic::warning("unsupported option for bloom-filter context")
          .primary(*timeout)
          .emit(ctx);
      }
    }
    auto keys = eval(args.key, events, ctx);
    auto key_values_list = list{};
    for (const auto& key : keys.values()) {
      auto materialized_key = materialize(key);
      bloom_filter_.add(materialized_key);
      key_values_list.emplace_back(std::move(materialized_key));
    }
    auto make_query
      = [key_values_list = std::move(key_values_list)](
          context_parameter_map, const std::vector<std::string>& fields)
      -> caf::expected<std::vector<expression>> {
      auto result = std::vector<expression>{};
      result.reserve(fields.size());
      for (const auto& field : fields) {
        auto lhs = to<operand>(field);
        TENZIR_ASSERT(lhs);
        result.emplace_back(predicate{
          *lhs,
          relational_operator::in,
          data{key_values_list},
        });
      }
      return result;
    };
    return context_update_result{
      .update_info = show(),
      .make_query = std::move(make_query),
    };
  }

  auto reset() -> caf::expected<void> override {
    auto params = bloom_filter_.parameters();
    TENZIR_ASSERT(params.n && params.p);
    bloom_filter_ = dcso_bloom_filter{*params.n, *params.p};
    return {};
  }

  auto save() const -> caf::expected<context_save_result> override {
    std::vector<std::byte> buffer;
    if (auto err = convert(bloom_filter_, buffer)) {
      return add_context(err, "failed to serialize Bloom filter context");
    }
    return context_save_result{.data = chunk::make(std::move(buffer)),
                               .version = 1};
  }

private:
  dcso_bloom_filter bloom_filter_;
};

struct v1_loader : public context_loader {
  auto version() const -> int {
    return 1;
  }

  auto load(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> {
    TENZIR_ASSERT(serialized != nullptr);
    auto bloom_filter = dcso_bloom_filter{};
    if (auto err = convert(as_bytes(*serialized), bloom_filter)) {
      return add_context(err, "failed to deserialize Bloom filter context");
    }
    return std::make_unique<bloom_filter_context>(std::move(bloom_filter));
  }
};

class plugin : public virtual context_factory_plugin<"bloom-filter"> {
  auto initialize(const record&, const record&) -> caf::error override {
    register_loader(std::make_unique<v1_loader>());
    return caf::none;
  }

  auto make_context(context_parameter_map parameters) const
    -> caf::expected<std::unique_ptr<context>> override {
    auto n = uint64_t{0};
    auto p = double{0.0};
    for (const auto& [key, value] : parameters) {
      if (key == "capacity") {
        if (not value) {
          return caf::make_error(ec::parse_error, "no --capacity provided");
        }
        if (not parsers::u64(*value, n)) {
          return caf::make_error(ec::invalid_argument,
                                 "--capacity is not an integer");
        }
      } else if (key == "fp-probability") {
        if (not value) {
          return caf::make_error(ec::parse_error,
                                 "no --fp-probability provided");
        }
        if (not parsers::real(*value, p)) {
          return caf::make_error(ec::invalid_argument,
                                 "--fp-probability is not a double");
        }
      } else {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("invalid option: {}", key));
      }
    }
    if (n == 0) {
      return caf::make_error(ec::invalid_argument, "--capacity must be > 0");
    }
    if (p <= 0.0 || p >= 1.0) {
      return caf::make_error(ec::invalid_argument,
                             "--fp-probability not in (0,1)");
    }
    return std::make_unique<bloom_filter_context>(n, p);
  }

  auto make_context(invocation inv, session ctx) const
    -> failure_or<make_context_result> override {
    auto name = located<std::string>{};
    auto capacity = located<uint64_t>{};
    auto fp_probability = located<double>{};
    auto parser = argument_parser2::context("bloom-filter");
    parser.add(name, "<name>");
    parser.add("capacity", capacity);
    parser.add("fp_probability", fp_probability);
    TRY(parser.parse(inv, ctx));
    auto failed = false;
    if (capacity.inner == 0) {
      diagnostic::error("capacity must be greater than zero")
        .primary(capacity)
        .emit(ctx);
      failed = true;
    }
    if (fp_probability.inner <= 0.0 or fp_probability.inner >= 1.0) {
      diagnostic::error("false-positive probability must be in (0, 1)")
        .primary(fp_probability)
        .emit(ctx);
      failed = true;
    }
    if (failed) {
      return failure::promise();
    }
    return make_context_result{
      std::move(name),
      std::make_unique<bloom_filter_context>(capacity.inner,
                                             fp_probability.inner),
    };
  }
};

} // namespace

} // namespace tenzir::plugins::bloom_filter

TENZIR_REGISTER_PLUGIN(tenzir::plugins::bloom_filter::plugin)
