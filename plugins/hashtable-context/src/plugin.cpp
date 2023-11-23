//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/fwd.hpp>
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
#include <caf/sum_type.hpp>
#include <tsl/robin_map.h>

#include <chrono>
#include <memory>
#include <string>

namespace tenzir::plugins::hashtable_context {

namespace {

class ctx : public virtual context {
public:
  /// Emits context information for every event in `slice` in order.
  auto apply(table_slice slice, record parameters) const
    -> caf::expected<std::vector<typed_array>> override {
    // TODO: Support more than 1 field if required (ASAP actually). More complex.
    auto resolved_slice = resolve_enumerations(slice);
    auto field_name = caf::get_if<std::string>(&parameters["field"]);
    if (!field_name) {
      // todo return null array of length slice.rows().
      // Or decide what to do here ...
    }
    if (field_name->empty()) {
      // todo return null array of length slice.rows().
      // Or decide what to do here ...
    }
    auto field_builder = series_builder{};
    auto column_offset
      = caf::get<record_type>(slice.schema()).resolve_key(*field_name);
    if (not column_offset) {
      for (auto i = size_t{0}; i < slice.rows(); ++i) {
        field_builder.null();
      }
      return field_builder.finish();
    }
    auto [type, slice_array] = column_offset->get(resolved_slice);
    for (auto value : values(type, *slice_array)) {
      if (auto it = context_entries.find(value); it != context_entries.end()) {
        // match
        auto r = field_builder.record();
        r.field("key", it->first);
        r.field("context", it->second);
        r.field("timestamp", std::chrono::system_clock::now());
      } else {
        // no match
        field_builder.null();
      }
    }
    return field_builder.finish();
  }

  /// Inspects the context.
  auto show() const -> record override {
    auto entries = list{};
    for (const auto& [key, context] : context_entries) {
      entries.push_back(record{
        {"key", key},
        {"context", context},
      });
    }
    return record{
      {"num_entries", context_entries.size()},
      {"entries", std::move(entries)}, // TODO: is this too verbose?
    };
  }

  /// Updates the context.
  auto update(table_slice slice, record parameters)
    -> caf::expected<record> override {
    // context does stuff on its own with slice & parameters
    if (parameters.contains("clear")) {
      auto* clear = get_if<bool>(&parameters, "clear");
      if (clear and *clear) {
        context_entries.clear();
      }
    }
    if (slice.rows() == 0) {
      TENZIR_INFO("got an empty slice");
      return record{};
    }
    // slice schema for this update is:
    /*
        {
          "key": string,
          "context" : data,
          "retire": bool,
        }
    */
    const auto& t = caf::get<record_type>(slice.schema());
    auto k = t.resolve_key("key");
    auto c = t.resolve_key("context");

    auto [k_type, k_slice_array] = k->get(slice);
    auto [c_type, c_slice_array] = c->get(slice);
    auto k_val = values(k_type, *k_slice_array);
    auto c_val = values(c_type, *c_slice_array);
    auto k_it = k_val.begin();
    auto c_it = c_val.begin();
    while (k_it != k_val.end() or c_it != c_val.end()) {
      context_entries.emplace(materialize(*k_it), materialize(*c_it));
      ++k_it;
      ++c_it;
    }
    return record{{"updated", slice.rows()}};
  }

  auto update(chunk_ptr bytes, record parameters)
    -> caf::expected<record> override {
    return ec::unimplemented;
  }

  auto update(record parameters) -> caf::expected<record> override {
    return ec::unimplemented;
  }

  auto save() const -> caf::expected<chunk_ptr> override {
    return ec::unimplemented;
  }

private:
  tsl::robin_map<data, data> context_entries;
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

  auto load_context(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> override {
    // do something...
    return std::make_unique<ctx>();
  }
};

} // namespace

} // namespace tenzir::plugins::hashtable_context

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hashtable_context::plugin)
