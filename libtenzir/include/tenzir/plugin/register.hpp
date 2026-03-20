//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// Minimal header for registering plugins via TENZIR_REGISTER_PLUGIN.
//
// Provides:
//   - tenzir::plugin_type_id_block
//   - tenzir::plugin_ptr
//   - tenzir::plugins::get_mutable() / get_static_type_id_blocks()
//   - TENZIR_REGISTER_PLUGIN(...)
//   - TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK(...)

#include "tenzir/detail/pp.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/plugin/base.hpp"

#include <caf/error.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace tenzir {

// -- plugin type ID blocks ----------------------------------------------------

/// The type ID block used by a plugin as [begin, end).
extern "C" struct plugin_type_id_block {
  uint16_t begin;
  uint16_t end;
};

/// Support CAF type-inspection.
/// @relates plugin_type_id_block
template <class Inspector>
auto inspect(Inspector& f, plugin_type_id_block& x) ->
  typename Inspector::result_type {
  return f(x.begin, x.end);
}

// -- plugin singleton ---------------------------------------------------------

namespace plugins {

/// Retrieves the system-wide plugin singleton (mutable).
/// @note Use this function carefully; modifying the system-wide plugin
/// singleton must only be done before the actor system is running.
auto get_mutable() noexcept -> std::vector<plugin_ptr>&;

/// Retrieves the system-wide plugin singleton (read-only).
auto get() noexcept -> const std::vector<plugin_ptr>&;

/// Retrieves the type-ID blocks and assigners singleton for static plugins.
auto get_static_type_id_blocks() noexcept
  -> std::vector<std::pair<plugin_type_id_block, void (*)()>>&;

} // namespace plugins

// -- plugin_ptr ---------------------------------------------------------------

/// An owned plugin and dynamically loaded plugin.
/// @relates plugin
class plugin_ptr final {
public:
  /// The type of the plugin.
  enum class type {
    dynamic, ///< The plugin is dynamically linked.
    static_, ///< The plugin is statically linked.
    builtin, ///< The plugin is builtin to the binary.
  };

  /// Load a dynamic plugin from the specified library filename.
  /// @param filename The filename that's passed to 'dlopen'.
  static auto make_dynamic(const char* filename) noexcept
    -> caf::expected<plugin_ptr>;

  /// Take ownership of a static plugin.
  /// @param instance The plugin instance.
  /// @param deleter A deleter for the plugin instance.
  /// @param version The version of the plugin.
  /// @param dependencies The plugin's dependencies.
  static auto
  make_static(plugin* instance, void (*deleter)(plugin*), const char* version,
              std::vector<std::string> dependencies) noexcept -> plugin_ptr;

  /// Take ownership of a builtin.
  /// @param instance The plugin instance.
  /// @param deleter A deleter for the plugin instance.
  /// @param version The version of the plugin.
  /// @param dependencies The plugin's dependencies.
  static auto
  make_builtin(plugin* instance, void (*deleter)(plugin*), const char* version,
               std::vector<std::string> dependencies) noexcept -> plugin_ptr;

  /// Default-construct an invalid plugin.
  plugin_ptr() noexcept;

  /// Unload a plugin and its required resources.
  ~plugin_ptr() noexcept;

  /// Forbid copying of plugins.
  plugin_ptr(const plugin_ptr&) = delete;
  auto operator=(const plugin_ptr&) -> plugin_ptr& = delete;

  /// Move-construction and move-assignment.
  plugin_ptr(plugin_ptr&& other) noexcept;
  auto operator=(plugin_ptr&& rhs) noexcept -> plugin_ptr&;

  /// Pointer facade.
  explicit operator bool() const noexcept;
  auto operator->() const noexcept -> const plugin*;
  auto operator->() noexcept -> plugin*;
  auto operator*() const noexcept -> const plugin&;
  auto operator&() noexcept -> plugin&;

  /// Downcast a plugin to a more specific plugin type.
  /// @tparam Plugin The specific plugin type to try to downcast to.
  /// @returns A pointer to the downcasted plugin, or 'nullptr' on failure.
  template <class Plugin>
  [[nodiscard]] auto as() const -> const Plugin* {
    static_assert(std::is_base_of_v<plugin, Plugin>, "'Plugin' must be derived "
                                                     "from 'tenzir::plugin'");
    return dynamic_cast<const Plugin*>(ctrl_->instance);
  }

  /// Downcast a plugin to a more specific plugin type.
  /// @tparam Plugin The specific plugin type to try to downcast to.
  /// @returns A pointer to the downcasted plugin, or 'nullptr' on failure.
  template <class Plugin>
  auto as() -> Plugin* {
    static_assert(std::is_base_of_v<plugin, Plugin>, "'Plugin' must be derived "
                                                     "from 'tenzir::plugin'");
    return dynamic_cast<Plugin*>(ctrl_->instance);
  }

  /// Returns the plugin version.
  [[nodiscard]] auto version() const noexcept -> const char*;

  /// Returns the plugin's dependencies.
  [[nodiscard]] auto dependencies() const noexcept
    -> const std::vector<std::string>&;

  /// Returns the plugins type.
  [[nodiscard]] auto type() const noexcept -> enum type;

  /// Bump the reference count of all dependencies.
  auto reference_dependencies() noexcept -> void;

  /// Compare two plugins.
  friend auto operator==(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept
    -> bool;
  friend auto operator<=>(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept
    -> std::strong_ordering;

  /// Compare a plugin by its name.
  friend auto operator==(const plugin_ptr& lhs, std::string_view rhs) noexcept
    -> bool;
  friend auto operator<=>(const plugin_ptr& lhs, std::string_view rhs) noexcept
    -> std::strong_ordering;

private:
  struct control_block {
    control_block(void* library, plugin* instance, void (*deleter)(plugin*),
                  const char* version, std::vector<std::string> dependencies,
                  enum type type) noexcept;
    ~control_block() noexcept;

    control_block(const control_block&) = delete;
    auto operator=(const control_block&) -> control_block& = delete;
    control_block(control_block&& other) noexcept = delete;
    auto operator=(control_block&& rhs) noexcept -> control_block& = delete;

    void* library = {};
    plugin* instance = {};
    void (*deleter)(plugin*) = {};
    const char* version = nullptr;
    std::vector<std::string> dependencies;
    std::vector<std::shared_ptr<control_block>> dependencies_ctrl;
    enum type type = {};
  };

  /// Create a plugin_ptr.
  explicit plugin_ptr(std::shared_ptr<control_block> ctrl) noexcept;

  /// The plugin's control block.
  std::shared_ptr<control_block> ctrl_;
};

} // namespace tenzir

namespace fmt {

template <>
struct formatter<enum tenzir::plugin_ptr::type> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(enum tenzir::plugin_ptr::type value, FormatContext& ctx) const {
    auto out = ctx.out();
    switch (value) {
      case tenzir::plugin_ptr::type::builtin:
        return fmt::format_to(ctx.out(), "builtin");
      case tenzir::plugin_ptr::type::static_:
        return fmt::format_to(ctx.out(), "static");
      case tenzir::plugin_ptr::type::dynamic:
        return fmt::format_to(ctx.out(), "dynamic");
    }
    return out;
  }
};

template <>
struct formatter<tenzir::plugin_ptr> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::plugin_ptr& value, FormatContext& ctx) const {
    if (value) {
      return fmt::format_to(ctx.out(), "{} ({})", value->name(), value.type());
    }
    return fmt::format_to(ctx.out(), "<disabled> ({})", value.type());
  }
};

} // namespace fmt

// -- template function definitions --------------------------------------------

namespace tenzir::plugins {

/// Retrieves all plugins of a given plugin type.
template <class Plugin>
auto get() noexcept -> generator<const Plugin*> {
  for (auto const& plugin : get()) {
    if (auto const* specific_plugin = plugin.as<Plugin>()) {
      co_yield specific_plugin;
    }
  }
}

/// Retrieves the plugin of type `Plugin` with the given name
/// (case-insensitive), or nullptr if it doesn't exist.
template <class Plugin = plugin>
auto find(std::string_view name) noexcept -> const Plugin* {
  const auto& plugins = get();
  const auto found = std::find(plugins.begin(), plugins.end(), name);
  if (found == plugins.end()) {
    return nullptr;
  }
  return found->template as<Plugin>();
}

} // namespace tenzir::plugins

// -- helper macros ------------------------------------------------------------

#if defined(TENZIR_ENABLE_BUILTINS)
#  define TENZIR_PLUGIN_VERSION nullptr
#else
extern const char* TENZIR_PLUGIN_VERSION;
#endif

#if defined(TENZIR_ENABLE_STATIC_PLUGINS) && defined(TENZIR_ENABLE_BUILTINS)

#  error "Plugins cannot be both static and builtin"

#elif defined(TENZIR_ENABLE_STATIC_PLUGINS) || defined(TENZIR_ENABLE_BUILTINS)

#  if defined(TENZIR_ENABLE_STATIC_PLUGINS)
#    define TENZIR_MAKE_PLUGIN(...)                                            \
      ::tenzir::plugin_ptr::make_static(__VA_ARGS__,                           \
                                        {TENZIR_PLUGIN_DEPENDENCIES})
#  elif defined(TENZIR_BUILTIN_DEPENDENCY)
#    define TENZIR_MAKE_PLUGIN(...)                                            \
      ::tenzir::plugin_ptr::make_builtin(                                      \
        __VA_ARGS__, {TENZIR_PP_STRINGIFY(TENZIR_BUILTIN_DEPENDENCY)})
#  else
#    define TENZIR_MAKE_PLUGIN(...)                                            \
      ::tenzir::plugin_ptr::make_builtin(__VA_ARGS__, {})
#  endif

// NOTE: The decltype expression is necessary for gcc 14 to produce a unique
// type for the lambda on each invocation. While every lambda is supposed to
// be a unique type according to the C++ standard, empirically we noticed here
// that it is not across different translation units. So now we use a lambda
// for distinguishing instances within the same translation unit and the
// decltype expression for distinguishing across translation units.
#  define TENZIR_REGISTER_PLUGIN(...)                                          \
    template <auto>                                                            \
    struct auto_register_plugin;                                               \
    template <>                                                                \
    struct auto_register_plugin<[](decltype(new __VA_ARGS__)) {}> {            \
      auto_register_plugin() {                                                 \
        static_cast<void>(flag);                                               \
      }                                                                        \
      static auto init() -> bool {                                             \
        /* NOLINTBEGIN(cppcoreguidelines-owning-memory) */                     \
        auto plugin = TENZIR_MAKE_PLUGIN(                                      \
          new __VA_ARGS__,                                                     \
          +[](::tenzir::plugin* plugin) noexcept {                             \
            delete plugin;                                                     \
          },                                                                   \
          TENZIR_PLUGIN_VERSION);                                              \
        /* NOLINTEND(cppcoreguidelines-owning-memory) */                       \
        const auto it = std::ranges::upper_bound(                              \
          ::tenzir::plugins::get_mutable(), plugin);                           \
        ::tenzir::plugins::get_mutable().insert(it, std::move(plugin));        \
        return true;                                                           \
      }                                                                        \
      inline static auto flag = init();                                        \
    };

#  define TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name)                         \
    struct auto_register_type_id_##name {                                      \
      auto_register_type_id_##name() {                                         \
        static_cast<void>(flag);                                               \
      }                                                                        \
      static auto init() -> bool {                                             \
        ::tenzir::plugins::get_static_type_id_blocks().emplace_back(           \
          ::tenzir::plugin_type_id_block{::caf::id_block::name::begin,         \
                                         ::caf::id_block::name::end},          \
          +[]() noexcept {                                                     \
            ::caf::init_global_meta_objects<::caf::id_block::name>();          \
          });                                                                  \
        return true;                                                           \
      }                                                                        \
      inline static auto flag = init();                                        \
    };

#  define TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_2(name1, name2)                 \
    TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name1)                              \
    TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name2)

#else

#  define TENZIR_REGISTER_PLUGIN(...)                                          \
    extern "C" auto tenzir_plugin_create() -> ::tenzir::plugin* {              \
      /* NOLINTNEXTLINE(cppcoreguidelines-owning-memory) */                    \
      return new __VA_ARGS__;                                                  \
    }                                                                          \
    extern "C" auto tenzir_plugin_destroy(class ::tenzir::plugin* plugin)      \
      -> void {                                                                \
      /* NOLINTNEXTLINE(cppcoreguidelines-owning-memory) */                    \
      delete plugin;                                                           \
    }                                                                          \
    extern "C" auto tenzir_plugin_version() -> const char* {                   \
      return TENZIR_PLUGIN_VERSION;                                            \
    }                                                                          \
    extern "C" auto tenzir_libtenzir_version() -> const char* {                \
      return ::tenzir::version::version;                                       \
    }                                                                          \
    extern "C" auto tenzir_plugin_dependencies() -> const char* const* {       \
      static constexpr auto dependencies = [](auto... xs) {                    \
        return std::array<const char*, sizeof...(xs) + 1>{xs..., nullptr};     \
      }(TENZIR_PLUGIN_DEPENDENCIES);                                           \
      return dependencies.data();                                              \
    }

#  define TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name)                         \
    extern "C" auto tenzir_plugin_register_type_id_block() -> void {           \
      ::caf::init_global_meta_objects<::caf::id_block::name>();                \
    }                                                                          \
    extern "C" auto tenzir_plugin_type_id_block()                              \
      -> ::tenzir::plugin_type_id_block {                                      \
      return {::caf::id_block::name::begin, ::caf::id_block::name::end};       \
    }

#  define TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_2(name1, name2)                 \
    extern "C" auto tenzir_plugin_register_type_id_block() -> void {           \
      ::caf::init_global_meta_objects<::caf::id_block::name1>();               \
      ::caf::init_global_meta_objects<::caf::id_block::name2>();               \
    }                                                                          \
    extern "C" auto tenzir_plugin_type_id_block()                              \
      -> ::tenzir::plugin_type_id_block {                                      \
      return {::caf::id_block::name1::begin < ::caf::id_block::name2::begin    \
                ? ::caf::id_block::name1::begin                                \
                : ::caf::id_block::name2::begin,                               \
              ::caf::id_block::name1::end > ::caf::id_block::name2::end        \
                ? ::caf::id_block::name1::end                                  \
                : ::caf::id_block::name2::end};                                \
    }

#endif

#define TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK(...)                              \
  TENZIR_PP_OVERLOAD(TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_, __VA_ARGS__)       \
  (__VA_ARGS__)
