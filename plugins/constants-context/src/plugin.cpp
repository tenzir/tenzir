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
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/typed_array.hpp>
#include <tenzir/view.hpp>

#include <arrow/array.h>
#include <tsl/robin_map.h>

#include <string>

namespace tenzir::plugins::constants_context {

namespace {

class ctx : public virtual context {
public:
  ctx() = default;

  explicit ctx(record fields) : fields{std::move(fields)} {
  }

  /// Emits context information for every event in `slice` in order.
  auto apply(table_slice slice, record parameters) const
    -> caf::expected<std::vector<typed_array>> override {
    (void)parameters;
    const auto fields_schema
      = caf::get<record_type>(type::infer(fields).value_or(type{}));
    // TODO: We should actually build a dictinary array in the constructor and
    // only finalize it with the number of rows here.
    auto builder
      = fields_schema.make_arrow_builder(arrow::default_memory_pool());
    for (size_t i = 0; i < slice.rows(); ++i) {
      const auto append_fields_result
        = append_builder(fields_schema, *builder, make_view(fields));
      TENZIR_ASSERT(append_fields_result.ok());
    }
    auto ok = builder->Finish();
    if (!ok.ok())
      return caf::make_error(ec::unimplemented, ":sad:");
    return std::vector{
      typed_array{std::move(fields_schema), ok.MoveValueUnsafe()},
    };
  }

  /// Inspects the context.
  auto show() const -> record override {
    return record{{"TODO", "unimplemented"}};
  }

  /// Updates the context.
  auto update(table_slice events, record parameters)
    -> caf::expected<record> override {
    (void)events;
    (void)parameters;
    return caf::make_error(ec::unimplemented, "unimplemented");
  }

  /// Updates the context.
  auto update(chunk_ptr bytes, record parameters)
    -> caf::expected<record> override {
    (void)bytes;
    (void)parameters;
    return caf::make_error(ec::unimplemented,
                           "constants-context can not be updated with bytes");
  }

  /// Updates the context.
  auto update(record parameters) -> caf::expected<record> override {
    (void)parameters;
    return caf::make_error(ec::unimplemented,
                           "constants-context can not be updated with void");
  }

  auto save() const -> caf::expected<chunk_ptr> override {
    return ec::unimplemented;
  }

private:
  record fields;
};

class plugin : public virtual context_plugin {
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)plugin_config;
    (void)global_config;
    return caf::none;
  }

  auto name() const -> std::string override {
    return "constants-context";
  }

  auto context_name() const -> std::string override {
    return "constants";
  }

  auto make_context(record fields) const
    -> caf::expected<std::unique_ptr<context>> override {
    return std::make_unique<ctx>(std::move(fields));
  }

  auto load_context(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> override {
    // do something...
    return std::make_unique<ctx>();
  }
};

} // namespace

} // namespace tenzir::plugins::constants_context

TENZIR_REGISTER_PLUGIN(tenzir::plugins::constants_context::plugin)
