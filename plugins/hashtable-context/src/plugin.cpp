//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/data.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/typed_array.hpp>

#include <arrow/array.h>
#include <tsl/robin_map.h>

#include <string>

namespace tenzir::plugins::hashtable_context {

namespace {

class ctx : public virtual context {
public:
  /// Emits context information for every event in `slice` in order.
  auto apply(table_slice slice, record parameters) const
    -> caf::expected<typed_array> override {
    (void)parameters;
    auto builder = arrow::StringBuilder{};
    for (size_t i = 0; i < slice.rows(); ++i) {
      auto ok = builder.Append("unimplemented");
      if (!ok.ok())
        return caf::make_error(ec::unimplemented, ":sad:");
    }
    auto ok = builder.Finish();
    if (!ok.ok())
      return caf::make_error(ec::unimplemented, ":sad:");
    return typed_array{string_type{}, ok.ValueUnsafe()};
  }

  /// Inspects the context.
  auto status(status_verbosity verbosity) const -> record override {
    (void)verbosity;
    return record{{"TODO", "unimplemented"}};
  }

  /// Updates the context.
  auto update(table_slice slice, record parameters) -> record override {
    // context does stuff on its own with slice & parameters
    if (parameters.contains("clear")) {
      auto* clear = get_if<bool>(&parameters, "clear");
      if (clear and *clear) {
        current_offset = 0;
        offset_table.clear();
        slice_table.clear();
      }
    }
    // slice schema for this update is:
    /*
        {
          "key": string,
          "context" : data,
          "retire": bool,
        }
        all of them arrays
        if not = diagnostic & ignore.
    */
    auto t = caf::get<record_type>(slice.schema());
    auto array = to_record_batch(slice)->ToStructArray().ValueOrDie();
    for (auto i = current_offset; i < (current_offset + slice.rows()); ++i) {
      auto v = slice.at(0, i);
      offset_table[materialize(v)] = i;
    }
    slice_table.insert(current_offset, current_offset + slice.rows(), slice);
    current_offset += slice.rows();

    return {{"updated", slice.rows()}};
  }

private:
  tsl::robin_map<data, size_t> offset_table;
  detail::range_map<size_t, table_slice> slice_table;
  size_t current_offset = 0;
};

class plugin : public virtual context_plugin {
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)plugin_config;
    (void)global_config;
    return caf::none;
  }

  auto name() const -> std::string override {
    return "hashtable-context";
  }

  auto make_context(record parameters) const
    -> caf::expected<std::unique_ptr<context>> override {
    (void)parameters;
    return std::make_unique<ctx>();
  }
};

} // namespace

} // namespace tenzir::plugins::hashtable_context

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hashtable_context::plugin)
