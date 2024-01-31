//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/data.hpp>
#include <tenzir/dcso_bloom_filter.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/fbs/data.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/project.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>
#include <tenzir/typed_array.hpp>

#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <caf/error.hpp>

#include <chrono>
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

  /// Emits context information for every event in `slice` in order.
  auto apply(table_slice slice, context::parameter_map parameters) const
    -> caf::expected<std::vector<typed_array>> override {
    auto resolved_slice = resolve_enumerations(slice);
    auto field_name = std::optional<std::string>{};
    for (const auto& [key, value] : parameters) {
      if (key == "field") {
        if (not value) {
          return caf::make_error(ec::invalid_argument,
                                 "invalid argument type for `field`: expected "
                                 "a string");
        }
        field_name = *value;
        continue;
      }
    }
    if (not field_name) {
      return caf::make_error(ec::invalid_argument, "missing argument `field`");
    }
    auto field_builder = series_builder{};
    auto column_offset = slice.schema().resolve_key_or_concept(*field_name);
    if (not column_offset) {
      for (auto i = size_t{0}; i < slice.rows(); ++i) {
        field_builder.null();
      }
      return field_builder.finish();
    }
    auto [type, slice_array] = column_offset->get(resolved_slice);
    for (const auto& value : values(type, *slice_array)) {
      if (bloom_filter_.lookup(value)) {
        auto ptr = bloom_filter_.data().data();
        auto size = bloom_filter_.data().size();
        auto r = field_builder.record();
        r.field("key", value);
        auto context = r.field("context").record();
        context.field("data", std::basic_string<std::byte>{ptr, size});
        r.field("timestamp", std::chrono::system_clock::now());
      } else {
        field_builder.null();
      }
    }
    return field_builder.finish();
  }

  auto snapshot(parameter_map) const -> caf::expected<expression> override {
    return caf::make_error(ec::unimplemented,
                           "bloom filter doesn't support snapshots");
  }

  /// Inspects the context.
  auto show() const -> record override {
    auto ptr = reinterpret_cast<const std::byte*>(bloom_filter_.data().data());
    auto size = bloom_filter_.data().size();
    auto data = std::basic_string<std::byte>{ptr, size};
    return record{
      {"num_elements", bloom_filter_.num_elements()},
      {"parameters",
       record{
         {"m", bloom_filter_.parameters().m},
         {"n", bloom_filter_.parameters().n},
         {"p", bloom_filter_.parameters().p},
         {"k", bloom_filter_.parameters().k},
       }},
      {"data", std::move(data)},
    };
  }

  /// Updates the context.
  auto update(table_slice slice, context::parameter_map parameters)
    -> caf::expected<update_result> override {
    TENZIR_ASSERT_CHEAP(slice.rows() != 0);
    if (not parameters.contains("key")) {
      return caf::make_error(ec::invalid_argument, "missing 'key' parameter");
    }
    auto key_field = parameters["key"];
    if (not key_field) {
      return caf::make_error(ec::invalid_argument,
                             "invalid 'key' parameter; 'key' must be a string");
    }
    auto key_column = slice.schema().resolve_key_or_concept(*key_field);
    if (not key_column) {
      // If there's no key column then we cannot do much.
      return update_result{record{}};
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
    auto query_f = [key_values_list = std::move(key_values_list)](
                     parameter_map params) -> caf::expected<expression> {
      auto column = params["field"];
      if (not column) {
        return caf::make_error(ec::invalid_argument,
                               "missing 'field' parameter");
      }
      return expression{
        predicate{
          field_extractor(*column),
          relational_operator::in,
          data{key_values_list},
        },
      };
    };
    return update_result{.update_info = show(),
                         .make_query = std::move(query_f)};
  }

  auto update(chunk_ptr, context::parameter_map)
    -> caf::expected<update_result> override {
    // TODO: We're getting chunks piece-meal currently and therefore cannot
    // determine the boundary of the stream. In theory, we should accumulate the
    // chunks until the upstream pipeline is complete. What we need in the
    // context API is a signal that we're done.
    return ec::unimplemented;
  }

  auto update(context::parameter_map) -> caf::expected<update_result> override {
    return caf::make_error(ec::unimplemented,
                           "bloom-filter context can not be updated with void");
  }

  auto save() const -> caf::expected<chunk_ptr> override {
    std::vector<std::byte> buffer;
    if (auto err = convert(bloom_filter_, buffer))
      return add_context(err, "failed to serialize Bloom filter context");
    return chunk::make(std::move(buffer));
  }

private:
  dcso_bloom_filter bloom_filter_;
};

class plugin : public virtual context_plugin {
  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "bloom-filter";
  }

  auto make_context(context::parameter_map parameters) const
    -> caf::expected<std::unique_ptr<context>> override {
    auto capacity = parameters["capacity"];
    if (not capacity)
      return caf::make_error(ec::parse_error, "no --capacity provided");
    auto fpp = parameters["fp-prob"];
    if (not fpp)
      return caf::make_error(ec::parse_error, "no --fp-prob provided");
    auto n = uint64_t{0};
    if (not parsers::u64(*capacity, n)) {
      return caf::make_error(ec::invalid_argument,
                             "--capacity is not an integer");
    }
    auto p = double{0.0};
    if (not parsers::real(*fpp, p)) {
      return caf::make_error(ec::invalid_argument, "--fp-prob is not a double");
    }
    if (n == 0) {
      return caf::make_error(ec::invalid_argument, "--capacity must be > 0");
    }
    if (p <= 0.0 || p >= 1.0) {
      return caf::make_error(ec::invalid_argument, "--fp-prob not in (0,1)");
    }
    return std::make_unique<bloom_filter_context>(n, p);
  }

  auto load_context(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> override {
    TENZIR_ASSERT_CHEAP(serialized != nullptr);
    auto bloom_filter = dcso_bloom_filter{};
    if (auto err = convert(as_bytes(*serialized), bloom_filter))
      return add_context(err, "failed to deserialize Bloom filter context");
    return std::make_unique<bloom_filter_context>(std::move(bloom_filter));
  }
};

} // namespace

} // namespace tenzir::plugins::bloom_filter

TENZIR_REGISTER_PLUGIN(tenzir::plugins::bloom_filter::plugin)
