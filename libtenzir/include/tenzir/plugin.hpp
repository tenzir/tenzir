//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/command.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/detail/pp.hpp"
#include "tenzir/http_api.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/series.hpp"
#include "tenzir/type.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <caf/error.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <caf/stream.hpp>
#include <caf/typed_actor.hpp>

#include <cctype>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
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

/// Retrieves the system-wide plugin singleton.
/// @note Use this function carefully; modifying the system-wide plugin
/// singleton must only be done before the actor system is running.
auto get_mutable() noexcept -> std::vector<plugin_ptr>&;

/// Retrieves the system-wide plugin singleton.
auto get() noexcept -> const std::vector<plugin_ptr>&;

/// Retrieves all plugins of a given plugin type.
template <class Plugin>
auto get() noexcept -> generator<const Plugin*>;

/// Retrieves the plugin of type `Plugin` with the given name
/// (case-insensitive), or nullptr if it doesn't exist.
template <class Plugin = plugin>
auto find(std::string_view name) noexcept -> const Plugin*;

/// Retrieves the type-ID blocks and assigners singleton for static plugins.
auto get_static_type_id_blocks() noexcept
  -> std::vector<std::pair<plugin_type_id_block, void (*)()>>&;

/// Load plugins specified in the configuration.
/// @param bundled_plugins The names of the bundled plugins.
/// @param cfg The actor system configuration of Tenzir for registering
/// additional type ID blocks.
/// @returns A list of paths to the loaded plugins, or an error detailing what
/// went wrong.
/// @note Invoke exactly once before \ref get() may be used.
auto load(const std::vector<std::string>& bundled_plugins,
          caf::actor_system_config& cfg)
  -> caf::expected<std::vector<std::filesystem::path>>;

/// Initialize loaded plugins.
auto initialize(caf::actor_system_config& cfg) -> caf::error;

/// @returns The loaded plugin-specific config files.
/// @note This function is not threadsafe.
auto loaded_config_files() -> const std::vector<std::filesystem::path>&;

} // namespace plugins

// -- plugin -------------------------------------------------------------------

/// The plugin base class.
class plugin {
public:
  /// Destroys any runtime state that the plugin created. For example,
  /// de-register from existing components, deallocate memory.
  virtual ~plugin() noexcept = default;

  /// Satisfy the rule of five.
  plugin() noexcept = default;
  plugin(const plugin&) noexcept = default;
  auto operator=(const plugin&) noexcept -> plugin& = default;
  plugin(plugin&&) noexcept = default;
  auto operator=(plugin&&) noexcept -> plugin& = default;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param plugin_config The relevant subsection of the configuration.
  /// @param global_config The entire Tenzir configuration for potential access
  /// to global options.
  [[nodiscard]] virtual auto
  initialize(const record& plugin_config, const record& global_config)
    -> caf::error {
    (void)plugin_config;
    (void)global_config;
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] virtual auto name() const -> std::string = 0;
};

// -- component plugin --------------------------------------------------------

/// A base class for plugins that spawn components in the NODE.
/// @relates plugin
class component_plugin : public virtual plugin {
public:
  /// The name for this component in the registry.
  /// Defaults to the plugin name.
  virtual auto component_name() const -> std::string;

  /// Components that should be created before the current one so initialization
  /// can succeed.
  /// Note that the *only* guarantee made is that components are able to
  /// retrieve actor handles of the wanted components from the registry.
  /// If actors send requests before returning their behaviors, there is
  /// no guarantee that these requests will arrive at the destination in
  /// the correct order.
  /// Defaults to empty list.
  virtual auto wanted_components() const -> std::vector<std::string>;

  /// Creates an actor as a component in the NODE.
  /// @param node A stateful pointer to the NODE actor.
  /// @returns The actor handle to the NODE component.
  /// @note This function runs in the actor context of the NODE actor and can
  /// safely access the NODE's state.
  virtual auto
  make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor
    = 0;
};

// -- command plugin -----------------------------------------------------------

/// A base class for plugins that add commands.
/// @relates plugin
class command_plugin : public virtual plugin {
public:
  /// Creates additional commands.
  /// @note Tenzir calls this function before initializing the plugin, which
  /// means that this function cannot depend on any plugin state. The logger
  /// is unavailable when this function is called.
  [[nodiscard]] virtual auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory>
    = 0;
};

// -- serialization plugin -----------------------------------------------------

/// This plugin interface can be used to serialize and deserialize classes
/// derived from `Base`. To this end, the base class provides a virtual
/// `Base::name()` function, which is matched against `plugin::name()`.
template <class Base>
class serialization_plugin : public virtual plugin {
public:
  /// @pre `x.name() == name()`
  [[nodiscard]] virtual auto serialize(serializer f, const Base& x) const
    -> bool
    = 0;

  /// @post `!x || x->name() == name()`
  virtual void deserialize(deserializer f, std::unique_ptr<Base>& x) const = 0;
};

template <class Inspector, class Base>
auto plugin_serialize(Inspector& f, const Base& x) -> bool {
  static_assert(not Inspector::is_loading);
  auto name = x.name();
  auto const* p = plugins::find<serialization_plugin<Base>>(name);
  if (auto dbg = as_debug_writer(f)) {
    if (not dbg->prepend("{} ", name)) {
      return false;
    }
    // Workaround for debug formatting non-serializable plugins. In that case we
    // only print the name instead of throwing an exception.
    if (not p) {
      return true;
    }
  } else {
    if (not f.apply(name)) {
      return false;
    }
  }
  TENZIR_ASSERT(p,
                fmt::format("serialization plugin `{}` for `{}` not found",
                            name, caf::detail::pretty_type_name(typeid(Base))));
  return p->serialize(std::ref(f), x);
}

/// Inspects a polymorphic object `x` by using the serialization plugin with the
/// name that matches `x->name()`.
template <class Inspector, class Base>
auto plugin_inspect(Inspector& f, std::unique_ptr<Base>& x) -> bool {
  if constexpr (Inspector::is_loading) {
    auto name = std::string{};
    if (not f.apply(name)) {
      return false;
    }
    auto const* p = plugins::find<serialization_plugin<Base>>(name);
    TENZIR_ASSERT(p, fmt::format("serialization plugin `{}` for `{}` not found",
                                 name,
                                 caf::detail::pretty_type_name(typeid(Base))));
    p->deserialize(f, x);
    return x != nullptr;
  } else {
    if (auto dbg = as_debug_writer(f)) {
      if (not x) {
        return dbg->fmt_value("<invalid>");
      }
    }
    TENZIR_ASSERT(x);
    return plugin_serialize(f, *x);
  }
}

/// Implements `serialization_plugin` for a concrete class derived from `Base`
/// by using its `inspect()` overload. Also provides a default implemenetation
/// of `plugin::name()` based on `Concrete::name()`.
template <class Base, class Concrete>
  requires std::is_base_of_v<Base, Concrete>
           && std::is_default_constructible_v<Concrete>
           && std::is_final_v<Concrete>
class inspection_plugin : public virtual serialization_plugin<Base> {
public:
  auto name() const -> std::string override {
    return Concrete{}.name();
  }

  auto serialize(serializer f, const Base& op) const -> bool override {
    TENZIR_ASSERT(op.name() == name());
    auto x = dynamic_cast<const Concrete*>(&op);
    TENZIR_ASSERT(x, fmt::format("expected {}, got {} ({}) ({} == {})",
                                 typeid(Concrete).name(), typeid(op).name(),
                                 typeid(Concrete) == typeid(op),
                                 typeid(Concrete).hash_code(),
                                 typeid(op).hash_code()));
    return std::visit(
      [&](auto& f) {
        return f.get().apply(*x);
      },
      f);
  }

  void deserialize(deserializer f, std::unique_ptr<Base>& x) const override {
    x = std::visit(
      [&](auto& f) -> std::unique_ptr<Concrete> {
        auto x = std::make_unique<Concrete>();
        if (not f.get().apply(*x)) {
          f.get().set_error(caf::make_error(
            ec::serialization_error, fmt::format("inspector of `{}` failed: {}",
                                                 name(), f.get().get_error())));
          return nullptr;
        }
        return x;
      },
      f);
  }
};

// -- operator plugin ----------------------------------------------------------

/// Deriving from this plugin will add an operator with the name of this plugin
/// to the pipeline parser. Derive from this class when you want to introduce an
/// alias to existing operators. This plugin itself does not add a new operator,
/// but only a parser for it. For most use cases: @see operator_plugin
class operator_parser_plugin : public virtual plugin {
public:
  /// @returns the name of the operator
  virtual auto operator_name() const -> std::string {
    return name();
  }

  /// @returns the signature of the operator.
  virtual auto signature() const -> operator_signature = 0;

  /// @throws diagnostic
  virtual auto parse_operator(parser_interface& p) const -> operator_ptr {
    // TODO: Remove this default implementation and adjust `parser.cpp`
    // accordingly when all operators are converted.
    (void)p;
    return nullptr;
  }

  virtual auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> {
    return {pipeline,
            caf::make_error(ec::unspecified, "this operator does not support "
                                             "the legacy parsing API")};
  }
};

using operator_serialization_plugin = serialization_plugin<operator_base>;

template <class Operator>
using operator_inspection_plugin = inspection_plugin<operator_base, Operator>;

/// This plugin adds a new operator with the name `Operator::name()` and
/// internal systems. Most operator plugins should use this class, but if you
/// only want to add an alias to existing operators, use
/// `operator_parser_plugin` instead.
template <class Operator>
class operator_plugin : public virtual operator_inspection_plugin<Operator>,
                        public virtual operator_parser_plugin {};

// -- loader plugin -----------------------------------------------------------

class plugin_loader {
public:
  virtual ~plugin_loader() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>>
    = 0;

  virtual auto default_parser() const -> std::string {
    return "json";
  }

  virtual auto internal() const -> bool {
    return false;
  }
};

/// @see operator_parser_plugin
class loader_parser_plugin : public virtual plugin {
public:
  virtual auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader>
    = 0;

  virtual auto supported_uri_schemes() const -> std::vector<std::string>;
};

using loader_serialization_plugin = serialization_plugin<plugin_loader>;

template <class Loader>
using loader_inspection_plugin = inspection_plugin<plugin_loader, Loader>;

/// @see operator_plugin
template <class Loader>
class loader_plugin : public virtual loader_inspection_plugin<Loader>,
                      public virtual loader_parser_plugin {};

// -- parser plugin -----------------------------------------------------------

class plugin_parser {
public:
  virtual ~plugin_parser() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>>
    = 0;

  /// Apply the parser to an array of strings.
  ///
  /// The default implementation of creates a new parser with `instantiate()`
  /// for every single string.
  ///
  /// @post `input->length() == result_array->length()`
  virtual auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                             operator_control_plane& ctrl) const
    -> std::vector<series>;

  /// Implement ordering optimization for parsers. See
  /// `operator_base::optimize(...)` for details. The default implementation
  /// does not optimize.
  virtual auto optimize(event_order order) -> std::unique_ptr<plugin_parser> {
    (void)order;
    return nullptr;
  }

  virtual auto idle_after() const -> duration {
    return duration::zero();
  }

  virtual auto detached() const -> bool {
    return false;
  }
};

/// @see operator_parser_plugin
class parser_parser_plugin : public virtual plugin {
public:
  virtual auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser>
    = 0;
};

using parser_serialization_plugin = serialization_plugin<plugin_parser>;

template <class Parser>
using parser_inspection_plugin = inspection_plugin<plugin_parser, Parser>;

/// @see operator_plugin
template <class Parser>
class parser_plugin : public virtual parser_parser_plugin,
                      public virtual parser_inspection_plugin<Parser> {};

// -- printer plugin ----------------------------------------------------------

class printer_instance {
public:
  virtual ~printer_instance() = default;

  virtual auto process(table_slice slice) -> generator<chunk_ptr> = 0;

  virtual auto finish() -> generator<chunk_ptr> {
    return {};
  }

  template <class F>
  static auto make(F f) -> std::unique_ptr<printer_instance> {
    class func_printer : public printer_instance {
    public:
      explicit func_printer(F f) : f_{std::move(f)} {
      }

      auto process(table_slice slice) -> generator<chunk_ptr> override {
        return f_(std::move(slice));
      }

    private:
      F f_;
    };
    return std::make_unique<func_printer>(std::move(f));
  }
};

class plugin_printer {
public:
  virtual ~plugin_printer() = default;

  virtual auto name() const -> std::string = 0;

  /// Returns a printer for a specified schema. If `allows_joining()`,
  /// then `input_schema`can also be `type{}`, which means that the printer
  /// should expect a heterogeneous input instead.
  virtual auto
  instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>>
    = 0;

  /// Returns whether the printer allows for joining output streams into a
  /// single saver.
  virtual auto allows_joining() const -> bool = 0;

  /// Returns whether it is safe to assume that the printer returns text that is
  /// encoded as UTF8.
  virtual auto prints_utf8() const -> bool = 0;
};

/// @see operator_parser_plugin
class printer_parser_plugin : public virtual plugin {
public:
  virtual auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer>
    = 0;
};

using printer_serialization_plugin = serialization_plugin<plugin_printer>;

template <class Printer>
using printer_inspection_plugin = inspection_plugin<plugin_printer, Printer>;

/// @see operator_plugin
template <class Printer>
class printer_plugin : public virtual printer_inspection_plugin<Printer>,
                       public virtual printer_parser_plugin {};

// -- saver plugin ------------------------------------------------------------

struct printer_info {
  type input_schema;
  std::string format;
};

class plugin_saver {
public:
  virtual ~plugin_saver() = default;

  virtual auto name() const -> std::string = 0;

  virtual auto
  instantiate(operator_control_plane& ctrl, std::optional<printer_info> info)
    -> caf::expected<std::function<void(chunk_ptr)>>
    = 0;

  /// Returns true if the saver joins the output from its preceding printer. If
  /// so, `instantiate()` will only be called once.
  virtual auto is_joining() const -> bool = 0;

  virtual auto default_printer() const -> std::string {
    return "json";
  }

  virtual auto internal() const -> bool {
    return false;
  }
};

/// @see operator_parser_plugin
class saver_parser_plugin : public virtual plugin {
public:
  virtual auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver>
    = 0;

  virtual auto supported_uri_schemes() const -> std::vector<std::string>;
};

using saver_serialization_plugin = serialization_plugin<plugin_saver>;

template <class Saver>
using saver_inspection_plugin = inspection_plugin<plugin_saver, Saver>;

/// @see operator_plugin
template <class Saver>
class saver_plugin : public virtual saver_inspection_plugin<Saver>,
                     public virtual saver_parser_plugin {};

// -- rest endpoint plugin -----------------------------------------------------

// A rest endpoint plugin declares a set of routes on which it can respond
// to HTTP requests, together with a `handler` actor that is responsible
// for doing that. A server (usually the `web` plugin) can then accept
// incoming requests and dispatch them to the correct handler according to the
// request path.
class rest_endpoint_plugin : public virtual plugin {
public:
  /// OpenAPI description of the plugin endpoints.
  /// @returns A record containing entries for the `paths` element of an
  ///          OpenAPI spec.
  [[nodiscard]] virtual auto
  openapi_endpoints(api_version version = api_version::latest) const -> record
    = 0;

  /// OpenAPI description of the schemas used by the plugin endpoints, if any.
  /// @returns A record containing entries for the `schemas` element of an
  ///          OpenAPI spec. The record may be empty if the plugin defines
  ///          no custom schemas.
  [[nodiscard]] virtual auto
  openapi_schemas(api_version /*version*/ = api_version::latest) const
    -> record {
    return record{};
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] virtual auto rest_endpoints() const
    -> const std::vector<rest_endpoint>& = 0;

  /// Actor that will handle this endpoint.
  //  TODO: This should get some integration with component_plugin so that
  //  the component can be used to answer requests directly.
  [[nodiscard]] virtual auto
  handler(caf::actor_system& system, node_actor node) const
    -> rest_handler_actor
    = 0;
};

// -- store plugin ------------------------------------------------------------

/// A base class for plugins that add new store backends.
/// @note Consider using the simler `store_plugin` instead, which abstracts
/// the actor system logic away with a default implementation, which usually
/// suffices for most store backends.
class store_actor_plugin : public virtual plugin {
public:
  /// A store_builder actor and a chunk called the "header". The contents of
  /// the header will be persisted on disk, and should allow the plugin to
  /// retrieve the correct store actor when `make_store()` below is called.
  struct builder_and_header {
    store_builder_actor store_builder;
    chunk_ptr header;
  };

  /// Create a store builder actor that accepts incoming table slices.
  /// The store builder is required to keep a reference to itself alive
  /// as long as its input stream is live, and persist itself and exit as
  /// soon as the input stream terminates.
  /// @param fs The actor handle of a filesystem.
  /// @param id The partition id for which we want to create a store. Can be
  /// used as a unique key by the implementation.
  /// @returns A handle to the store builder actor to add events to, and a
  /// header that uniquely identifies this store for later use in `make_store`.
  [[nodiscard]] virtual auto
  make_store_builder(filesystem_actor fs, const tenzir::uuid& id) const
    -> caf::expected<builder_and_header>
    = 0;

  /// Create a store actor from the given header. Called when deserializing a
  /// partition that uses this partition as a store backend.
  /// @param fs The actor handle of a filesystem.
  /// @param header The store header as found in the partition flatbuffer.
  /// @returns A new store actor.
  [[nodiscard]] virtual auto
  make_store(filesystem_actor fs, std::span<const std::byte> header) const
    -> caf::expected<store_actor>
    = 0;
};

/// A base class for plugins that add new store backends.
class store_plugin : public virtual store_actor_plugin {
public:
  /// Create a store for passive partitions.
  [[nodiscard]] virtual auto make_passive_store() const
    -> caf::expected<std::unique_ptr<passive_store>>
    = 0;

  /// Create a store for active partitions.
  /// @param tenzir_config The tenzir node configuration.
  [[nodiscard]] virtual auto make_active_store() const
    -> caf::expected<std::unique_ptr<active_store>>
    = 0;

private:
  [[nodiscard]] auto
  make_store_builder(filesystem_actor fs, const tenzir::uuid& id) const
    -> caf::expected<builder_and_header> final;

  [[nodiscard]] auto
  make_store(filesystem_actor fs, std::span<const std::byte> header) const
    -> caf::expected<store_actor> final;
};

// -- metrics plugin ----------------------------------------------------------

class metrics_plugin : public virtual plugin {
public:
  using collector = std::function<caf::expected<record>()>;

  /// The name under which this metric should be displayed.
  [[nodiscard]] virtual auto metric_name() const -> std::string {
    return name();
  }

  /// The format in which metrics will be reported by this plugin.
  [[nodiscard]] virtual auto metric_layout() const -> record_type = 0;

  /// Create a metrics collector.
  /// Plugins may return an error if the collector is not supported on the
  /// platform the node is currently running on.
  [[nodiscard]] virtual auto make_collector(caf::actor_system& system) const
    -> caf::expected<collector>
    = 0;

  /// Returns the frequency for collecting the metrics, expressed as the
  /// interval between calls to the collector.
  [[nodiscard]] virtual auto metric_frequency() const -> duration {
    return std::chrono::seconds{1};
  }
};

// -- aspect plugin ------------------------------------------------------------

class aspect_plugin : public virtual plugin {
public:
  /// The name of the aspect that enables `show aspect`.
  /// @note defaults to `plugin::name()`.
  virtual auto aspect_name() const -> std::string;

  /// Produces the data to show.
  virtual auto show(operator_control_plane& ctrl) const
    -> generator<table_slice>
    = 0;
};

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
  /// @param cfg The actor system config to register type IDs with.
  static auto
  make_dynamic(const char* filename, caf::actor_system_config& cfg) noexcept
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

// -- template function definitions -------------------------------------------

namespace tenzir::plugins {

template <class Plugin>
auto find(std::string_view name) noexcept -> const Plugin* {
  const auto& plugins = get();
  const auto found = std::find(plugins.begin(), plugins.end(), name);
  if (found == plugins.end()) {
    return nullptr;
  }
  return found->template as<Plugin>();
}

inline auto find_operator(std::string_view name) noexcept
  -> const operator_parser_plugin* {
  for (const auto* plugin : get<operator_parser_plugin>()) {
    const auto current_name = plugin->operator_name();
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

template <class Plugin>
auto get() noexcept -> generator<const Plugin*> {
  for (auto const& plugin : get()) {
    if (auto const* specific_plugin = plugin.as<Plugin>()) {
      co_yield specific_plugin;
    }
  }
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
    extern "C" auto tenzir_libtenzir_build_tree_hash() -> const char* {        \
      return ::tenzir::version::build::tree_hash;                              \
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
