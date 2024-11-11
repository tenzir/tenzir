//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin.hpp"

namespace tenzir {

class context {
public:
  using parameter_map
    = std::unordered_map<std::string, std::optional<std::string>>;
  using make_query_type
    = std::function<auto(parameter_map parameters,
                         const std::vector<std::string>& fields)
                      ->caf::expected<std::vector<expression>>>;

  static constexpr auto dump_batch_size_limit = 65536;

  /// Information about a context update that gets propagated to live lookups.
  struct update_result {
    // TODO The update info is no longer needed since context update became a
    // sink operator.
    record update_info;
    // Function for emitting an updated expression. Used for retroactive lookups.
    make_query_type make_query = {};
  };

  struct save_result {
    chunk_ptr data;
    int version;

    friend auto inspect(auto& f, save_result& x) -> bool {
      return f.object(x).fields(f.field("data", x.data),
                                f.field("version", x.version));
    }
  };

  virtual ~context() noexcept = default;

  virtual auto context_type() const -> std::string = 0;

  /// Emits context information for every event in `array` in order.
  /// @param array The values to look up in the context.
  /// @param replace If true, return the input values for missing fields rather
  /// than nulls.
  virtual auto apply(series array, bool replace)
    -> caf::expected<std::vector<series>>
    = 0;

  /// Inspects the context.
  virtual auto show() const -> record = 0;

  /// Dumps the context content or a dumping error.
  virtual auto dump() -> generator<table_slice> = 0;

  /// Updates the context.
  virtual auto update(table_slice events, parameter_map parameters)
    -> caf::expected<update_result>
    = 0;

  /// Clears the context state, with optional parameters.
  virtual auto reset() -> caf::expected<void> = 0;

  /// Serializes a context for persistence.
  virtual auto save() const -> caf::expected<save_result> = 0;
};

class context_loader {
public:
  virtual ~context_loader() noexcept = default;

  virtual auto version() const -> int = 0;

  virtual auto load(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>>
    = 0;
};

class context_plugin : public virtual plugin {
public:
  /// Create a context.
  [[nodiscard]] virtual auto
  make_context(context::parameter_map parameters) const
    -> caf::expected<std::unique_ptr<context>>
    = 0;

  [[nodiscard]] auto get_latest_loader() const -> const context_loader&;

  [[nodiscard]] auto get_versioned_loader(int version) const
    -> const context_loader*;

  [[nodiscard]] virtual auto context_name() const -> std::string {
    return name();
  };

protected:
  void register_loader(std::unique_ptr<context_loader> loader);

private:
  std::vector<std::unique_ptr<context_loader>> loaders_{};
};

} // namespace tenzir
