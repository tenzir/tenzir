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
  auto update(table_slice slice, record parameters) -> caf::error override {
    (void)slice;
    (void)parameters;
    return caf::none;
  }

private:
  tsl::robin_map<data, data> table;
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
