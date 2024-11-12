//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <cctype>

namespace tenzir {

struct context_parameter_map
  : std::unordered_map<std::string, std::optional<std::string>> {
  using super = std::unordered_map<std::string, std::optional<std::string>>;
  using super::super;

  friend auto inspect(auto& f, context_parameter_map& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

struct context_update_args {
  ast::expression key = {};
  std::optional<located<duration>> create_timeout = {};
  std::optional<located<duration>> write_timeout = {};
  std::optional<located<duration>> read_timeout = {};

  friend auto inspect(auto& f, context_update_args& x) -> bool {
    return f.object(x).fields(f.field("key", x.key),
                              f.field("create_timeout", x.create_timeout),
                              f.field("update_timeout", x.write_timeout),
                              f.field("read_timeout", x.read_timeout));
  }
};

/// Information about a context update that gets propagated to live lookups.
struct context_update_result {
  using make_query_type
    = std::function<auto(context_parameter_map parameters,
                         const std::vector<std::string>& fields)
                      ->caf::expected<std::vector<expression>>>;

  // TODO The update info is no longer needed since context update became a
  // sink operator.
  record update_info;
  // Function for emitting an updated expression. Used for retroactive lookups.
  make_query_type make_query = {};
};

struct context_save_result {
  chunk_ptr data;
  int version;

  friend auto inspect(auto& f, context_save_result& x) -> bool {
    return f.object(x).fields(f.field("data", x.data),
                              f.field("version", x.version));
  }
};

class context {
public:
  static constexpr auto dump_batch_size_limit = 65536;

  virtual ~context() noexcept = default;

  virtual auto context_type() const -> std::string = 0;

  /// Emits context information for every event in `array` in order.
  /// @param array The values to look up in the context.
  /// @param replace If true, return the input values for missing fields rather
  /// than nulls.
  virtual auto apply(series array, bool replace)
    -> caf::expected<std::vector<series>>
    = 0;
  virtual auto apply2(const series& array, session ctx) -> std::vector<series>
    = 0;

  /// Inspects the context.
  virtual auto show() const -> record = 0;

  /// Dumps the context content or a dumping error.
  virtual auto dump() -> generator<table_slice> = 0;

  /// Updates the context.
  virtual auto update(table_slice events, context_parameter_map parameters)
    -> caf::expected<context_update_result>
    = 0;
  virtual auto update2(const table_slice& events,
                       const context_update_args& args, session ctx)
    -> failure_or<context_update_result>
    = 0;

  /// Clears the context state, with optional parameters.
  virtual auto reset() -> caf::expected<void> = 0;

  /// Serializes a context for persistence.
  virtual auto save() const -> caf::expected<context_save_result> = 0;
};

struct make_context_result {
  located<std::string> name;
  std::unique_ptr<context> ctx;
};

class context_loader {
public:
  virtual ~context_loader() noexcept = default;

  virtual auto version() const -> int = 0;

  virtual auto load(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>>
    = 0;
};

using context_create_actor
  = caf::typed_actor<auto(atom::create, std::string, std::string,
                          context_save_result)
                       ->caf::result<void>>;

template <detail::string_literal Name>
class context_create_operator final
  : public crtp_operator<context_create_operator<Name>> {
public:
  context_create_operator() = default;

  context_create_operator(located<std::string> name,
                          context_save_result save_result)
    : name_{std::move(name)}, save_result_{std::move(save_result)} {
    // nop
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto context_manager
      = ctrl.self().system().registry().get<context_create_actor>(
        "tenzir.context-manager");
    TENZIR_ASSERT(context_manager);
    ctrl.set_waiting(true);
    ctrl.self()
      .request(context_manager, caf::infinite, atom::create_v, name_.inner,
               std::string{Name.str()}, save_result_)
      .then(
        [&]() {
          ctrl.set_waiting(false);
        },
        [&](caf::error& err) {
          diagnostic::error(err)
            .primary(name_)
            .note("failed to create context")
            .emit(ctrl.diagnostics());
        });
    co_yield {};
  }

  auto name() const -> std::string override {
    return fmt::format("context::create_{}",
                       detail::replace_all(std::string{Name.str()}, "-", "_"));
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    (void)order;
    return do_not_optimize(*this);
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  friend auto inspect(auto& f, context_create_operator& x) -> bool {
    return f.object(x).fields(f.field("name", x.name_),
                              f.field("save_result", x.save_result_));
  }

private:
  located<std::string> name_ = {};
  context_save_result save_result_ = {};
};

class context_plugin : public virtual plugin {
public:
  using invocation = operator_factory_plugin::invocation;

  [[nodiscard]] virtual auto context_name() const -> std::string {
    return name();
  };

  /// Create a context.
  [[nodiscard]] virtual auto
  make_context(context_parameter_map parameters) const
    -> caf::expected<std::unique_ptr<context>>
    = 0;

  [[nodiscard]] virtual auto make_context(invocation inv, session ctx) const
    -> failure_or<make_context_result>
    = 0;

  [[nodiscard]] auto get_latest_loader() const -> const context_loader& {
    TENZIR_ASSERT(not loaders_.empty());
    return **std::ranges::max_element(loaders_, std::ranges::less{},
                                      [](const auto& loader) {
                                        return loader->version();
                                      });
  }

  [[nodiscard]] auto get_versioned_loader(int version) const
    -> const context_loader* {
    auto it = std::ranges::find(loaders_, version, [](const auto& loader) {
      return loader->version();
    });
    if (it == loaders_.end()) {
      return nullptr;
    }
    return it->get();
  }

protected:
  void register_loader(std::unique_ptr<context_loader> loader) {
    loaders_.emplace_back(std::move(loader));
  }

private:
  std::vector<std::unique_ptr<context_loader>> loaders_{};
};

template <detail::string_literal Name>
class context_factory_plugin
  : public virtual operator_plugin2<context_create_operator<Name>>,
    public virtual context_plugin {
public:
  using context_plugin::invocation;

private:
  auto name() const -> std::string final {
    return fmt::format("context::create_{}",
                       detail::replace_all(std::string{Name.str()}, "-", "_"));
  }

  auto context_name() const -> std::string final {
    return std::string{Name.str()};
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> final {
    TRY(auto result, this->make_context(std::move(inv), ctx));
    const auto is_valid_char = [](char ch) {
      return static_cast<bool>(std::isalnum(static_cast<unsigned char>(ch)))
             or ch == '-' or ch == '_';
    };
    if (not std::ranges::all_of(result.name.inner, is_valid_char)) {
      diagnostic::error("context name contains invalid characters")
        .primary(result.name)
        .hint("only alphanumeric characters, hyphens, and underscores are "
              "allowed")
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<context_create_operator<Name>>(
      result.name, check(result.ctx->save()));
  }
};

namespace plugins {

inline auto find_context(std::string_view name) noexcept
  -> const context_plugin* {
  for (const auto* plugin : get<context_plugin>()) {
    const auto current_name = plugin->context_name();
    const auto match
      = std::equal(current_name.begin(), current_name.end(), name.begin(),
                   name.end(), [](const char lhs, const char rhs) {
                     return std::tolower(static_cast<unsigned char>(lhs))
                            == std::tolower(static_cast<unsigned char>(rhs));
                   });
    if (match) {
      return plugin;
    }
  }
  return nullptr;
}

} // namespace plugins

} // namespace tenzir
