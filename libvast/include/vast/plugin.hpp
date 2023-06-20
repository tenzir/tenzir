//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"
#include "vast/command.hpp"
#include "vast/config_options.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/pp.hpp"
#include "vast/detail/weak_handle.hpp"
#include "vast/http_api.hpp"
#include "vast/operator_control_plane.hpp"
#include "vast/pipeline.hpp"
#include "vast/type.hpp"

#include <caf/detail/pretty_type_name.hpp>
#include <caf/error.hpp>
#include <caf/init_global_meta_objects.hpp>
#include <caf/stream.hpp>
#include <caf/typed_actor.hpp>

#include <cctype>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <type_traits>
#include <vector>

namespace vast {

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
std::vector<plugin_ptr>& get_mutable() noexcept;

/// Retrieves the system-wide plugin singleton.
const std::vector<plugin_ptr>& get() noexcept;

/// Retrieves all plugins of a given plugin type.
template <class Plugin>
generator<const Plugin*> get() noexcept;

/// Retrieves the plugin of type `Plugin` with the given name
/// (case-insensitive), or nullptr if it doesn't exist.
template <class Plugin = plugin>
const Plugin* find(std::string_view name) noexcept;

/// Retrieves the type-ID blocks and assigners singleton for static plugins.
std::vector<std::pair<plugin_type_id_block, void (*)()>>&
get_static_type_id_blocks() noexcept;

/// Load plugins specified in the configuration.
/// @param bundled_plugins The names of the bundled plugins.
/// @param cfg The actor system configuration of VAST for registering additional
/// type ID blocks.
/// @returns A list of paths to the loaded plugins, or an error detailing what
/// went wrong.
/// @note Invoke exactly once before \ref get() may be used.
caf::expected<std::vector<std::filesystem::path>>
load(const std::vector<std::string>& bundled_plugins,
     caf::actor_system_config& cfg);

/// Initialize loaded plugins.
caf::error initialize(caf::actor_system_config& cfg);

/// @returns The loaded plugin-specific config files.
/// @note This function is not threadsafe.
const std::vector<std::filesystem::path>& loaded_config_files();

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
  plugin& operator=(const plugin&) noexcept = default;
  plugin(plugin&&) noexcept = default;
  plugin& operator=(plugin&&) noexcept = default;

  /// Allow the plugin to have its own logic for when it should be loaded.
  /// The plugin will no be initialized if `enabled()` returns false.
  /// The default implementation looks for a key named 'enabled' in the
  /// plugin config, and defaults to `true` if that does not exist.
  [[nodiscard]] virtual bool
  enabled(const record& plugin_config, const record& global_config) const;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param plugin_config The relevant subsection of the configuration.
  /// @param global_config The entire VAST configuration for potential access to
  /// global options.
  [[nodiscard]] virtual auto
  initialize(const record& plugin_config, const record& global_config)
    -> caf::error {
    (void)plugin_config;
    (void)global_config;
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] virtual std::string name() const = 0;
};

// -- component plugin --------------------------------------------------------

/// A base class for plugins that spawn components in the NODE.
/// @relates plugin
class component_plugin : public virtual plugin {
public:
  /// The name for this component in the registry.
  /// Defaults to the plugin name.
  virtual std::string component_name() const;

  /// Creates an actor as a component in the NODE.
  /// @param node A stateful pointer to the NODE actor.
  /// @returns The actor handle to the NODE component.
  /// @note This function runs in the actor context of the NODE actor and can
  /// safely access the NODE's state.
  virtual component_plugin_actor
  make_component(node_actor::stateful_pointer<node_state> node) const
    = 0;
};

// -- analyzer plugin ----------------------------------------------------------

/// A base class for plugins that hook into the input stream.
/// @relates component_plugin
class analyzer_plugin : public virtual component_plugin {
public:
  /// Gets or spawns the ANALYZER actor spawned by the plugin.
  /// @param node A pointer to the NODE actor handle. This argument is optional
  /// for retrieving an already spawned ANALYZER.
  /// @returns The actor handle to the analyzer, or `nullptr` if the actor was
  /// spawned but shut down already.
  analyzer_plugin_actor
  analyzer(node_actor::stateful_pointer<node_state> node = nullptr) const;

  /// Implicitly fulfill the requirements of a COMPONENT PLUGIN actor via the
  /// ANALYZER PLUGIN actor.
  component_plugin_actor
  make_component(node_actor::stateful_pointer<node_state> node) const final;

protected:
  /// Creates an actor that hooks into the input table slice stream.
  /// @param node A stateful pointer to the NODE actor.
  /// @returns The actor handle to the analyzer.
  /// @note It is guaranteed that this function is not called while the ANALYZER
  /// is still running.
  /// @note This function runs in the actor context of the NODE actor and can
  /// safely access the NODE's state.
  virtual analyzer_plugin_actor
  make_analyzer(node_actor::stateful_pointer<node_state> node) const
    = 0;

private:
  /// A weak handle to the spawned actor handle.
  mutable detail::weak_handle<analyzer_plugin_actor> weak_handle_ = {};

  /// Indicates that the ANALYZER was spawned at least once. This flag is used
  /// to ensure that `make_analyzer` is called at most once per plugin.
  mutable bool spawned_once_ = false;
};

// -- command plugin -----------------------------------------------------------

/// A base class for plugins that add commands.
/// @relates plugin
class command_plugin : public virtual plugin {
public:
  /// Creates additional commands.
  /// @note VAST calls this function before initializing the plugin, which
  /// means that this function cannot depend on any plugin state. The logger
  /// is unavailable when this function is called.
  [[nodiscard]] virtual std::pair<std::unique_ptr<command>, command::factory>
  make_command() const = 0;
};

// -- reader plugin -----------------------------------------------------------

/// A base class for plugins that add import formats.
/// @relates plugin
class reader_plugin : public virtual plugin {
public:
  /// Returns the import format's name.
  [[nodiscard]] virtual const char* reader_format() const = 0;

  /// Returns the `vast import <format>` helptext.
  [[nodiscard]] virtual const char* reader_help() const = 0;

  /// Returns the options for the `vast import <format>` command.
  [[nodiscard]] virtual config_options
  reader_options(command::opts_builder&& opts) const
    = 0;

  /// Creates a reader, which will be available via `vast import <format>` and
  /// `vast spawn source <format>`.
  /// @note Use `vast::detail::make_input_stream` to create an input stream from
  /// the options.
  [[nodiscard]] virtual std::unique_ptr<format::reader>
  make_reader(const caf::settings& options) const = 0;
};

// -- writer plugin -----------------------------------------------------------

/// A base class for plugins that add export formats.
/// @relates plugin
class writer_plugin : public virtual plugin {
public:
  /// Returns the export format's name.
  [[nodiscard]] virtual const char* writer_format() const = 0;

  /// Returns the `vast export <format>` helptext.
  [[nodiscard]] virtual const char* writer_help() const = 0;

  /// Returns the options for the `vast export <format>` command.
  [[nodiscard]] virtual config_options
  writer_options(command::opts_builder&& opts) const
    = 0;

  /// Creates a reader, which will be available via `vast export <format>` and
  /// `vast spawn sink <format>`.
  /// @note Use `vast::detail::make_output_stream` to create an output stream
  /// from the options.
  [[nodiscard]] virtual std::unique_ptr<format::writer>
  make_writer(const caf::settings& options) const = 0;
};

// -- serialization plugin -----------------------------------------------------

/// This plugin interface can be used to serialize and deserialize classes
/// derived from `Base`. To this end, the base class provides a virtual
/// `Base::name()` function, which is matched against `plugin::name()`.
template <class Base>
class serialization_plugin : public virtual plugin {
public:
  /// @pre `x.name() == name()`
  [[nodiscard]] virtual auto serialize(inspector& f, const Base& x) const
    -> bool
    = 0;

  /// @post `!x || x->name() == name()`
  virtual void deserialize(inspector& f, std::unique_ptr<Base>& x) const = 0;
};

/// Inspects a polymorphic object `x` by using the serialization plugin with the
/// name that matches `x->name()`.
template <class Inspector, class Base>
auto plugin_inspect(Inspector& f, std::unique_ptr<Base>& x) -> bool {
  if constexpr (Inspector::is_loading) {
    auto name = std::string{};
    if (!f.apply(name)) {
      return false;
    }
    auto const* p = plugins::find<serialization_plugin<Base>>(name);
    if (!p) {
      f.set_error(caf::make_error(
        ec::serialization_error,
        fmt::format("serialization plugin `{}` for `{}` not found", name,
                    caf::detail::pretty_type_name(typeid(Base)))));
      return false;
    }
    auto g = inspector{f};
    p->deserialize(g, x);
    return x != nullptr;
  } else {
    VAST_ASSERT(x);
    auto name = x->name();
    if (!f.apply(name)) {
      return false;
    }
    auto const* p = plugins::find<serialization_plugin<Base>>(name);
    if (!p) {
      f.set_error(caf::make_error(
        ec::serialization_error,
        fmt::format("serialization plugin `{}` for `{}` not found", name,
                    caf::detail::pretty_type_name(typeid(Base)))));
      return false;
    }
    auto g = inspector{f};
    return p->serialize(g, *x);
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

  auto serialize(inspector& f, const Base& op) const -> bool override {
    VAST_ASSERT(op.name() == name());
    auto x = dynamic_cast<const Concrete*>(&op);
    VAST_ASSERT(x);
    // TODO: Can probably remove this const_cast by splitting up `inspector`.
    auto y = const_cast<Concrete*>(x);
    return f.apply(*y);
  }

  void deserialize(inspector& f, std::unique_ptr<Base>& x) const override {
    x = std::visit(
      [this]<class Inspector>(
        std::reference_wrapper<Inspector> g) -> std::unique_ptr<Concrete> {
        if constexpr (Inspector::is_loading) {
          auto x = std::make_unique<Concrete>();
          auto& f = g.get();
          if (!f.apply(*x)) {
            f.set_error(
              caf::make_error(ec::serialization_error,
                              fmt::format("inspector of `{}` failed: {}",
                                          name(), f.get_error())));
            return nullptr;
          }
          return x;
        } else {
          die("unreachable");
        }
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

  // TODO: Consider adding a default implementation.
  virtual auto to_string() const -> std::string = 0;
};

/// @see operator_parser_plugin
class loader_parser_plugin : public virtual plugin {
public:
  virtual auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader>
    = 0;
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
  /// should expect a hetergenous input instead.
  virtual auto
  instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>>
    = 0;

  /// Returns whether the printer allows for joining output streams into a
  /// single saver.
  virtual auto allows_joining() const -> bool = 0;
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
  type input_schema{};
  std::string format{};
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
};

/// @see operator_parser_plugin
class saver_parser_plugin : public virtual plugin {
public:
  virtual auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver>
    = 0;
};

using saver_serialization_plugin = serialization_plugin<plugin_saver>;

template <class Saver>
using saver_inspection_plugin = inspection_plugin<plugin_saver, Saver>;

/// @see operator_plugin
template <class Saver>
class saver_plugin : public virtual saver_inspection_plugin<Saver>,
                     public virtual saver_parser_plugin {};

// -- aggregation function plugin ---------------------------------------------

/// A base class for plugins that add new aggregation functions.
class aggregation_function_plugin : public virtual plugin {
public:
  /// Creates a new aggregation function that maps incrementally added input
  /// to a single output value.
  /// @param input_types The input types for which to create the aggregation
  /// function.
  [[nodiscard]] virtual caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const = 0;
};

// -- language plugin ---------------------------------------------------

/// A language parser to pass query in a custom language to VAST.
/// @relates plugin
class language_plugin : public virtual plugin {
public:
  /// Parses a query string into a pipeline object.
  /// @param query The string representing the custom query.
  virtual auto parse_query(std::string_view query) const
    -> caf::expected<pipeline>
    = 0;
};

// -- rest endpoint plugin -----------------------------------------------------

// A rest endpoint plugin declares a set of routes on which it can respond
// to HTTP requests, together with a `handler` actor that is responsible
// for doing that. A server (usually the `web` plugin) can then accept
// incoming requests and dispatch them to the correct handler according to the
// request path.
class rest_endpoint_plugin : public virtual plugin {
public:
  /// OpenAPI spec for the plugin endpoints.
  /// @returns A `vast::data` object that is a record containing entries for
  /// the `paths` element of an OpenAPI spec.
  [[nodiscard]] virtual auto
  openapi_specification(api_version version = api_version::latest) const -> data
    = 0;

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
  /// @param accountant The actor handle of the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param id The partition id for which we want to create a store. Can be
  /// used as a unique key by the implementation.
  /// @returns A handle to the store builder actor to add events to, and a
  /// header that uniquely identifies this store for later use in `make_store`.
  [[nodiscard]] virtual caf::expected<builder_and_header>
  make_store_builder(accountant_actor accountant, filesystem_actor fs,
                     const vast::uuid& id) const
    = 0;

  /// Create a store actor from the given header. Called when deserializing a
  /// partition that uses this partition as a store backend.
  /// @param accountant The actor handle the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param header The store header as found in the partition flatbuffer.
  /// @returns A new store actor.
  [[nodiscard]] virtual caf::expected<store_actor>
  make_store(accountant_actor accountant, filesystem_actor fs,
             std::span<const std::byte> header) const
    = 0;
};

/// A base class for plugins that add new store backends.
class store_plugin : public virtual store_actor_plugin,
                     public virtual parser_parser_plugin,
                     public virtual printer_parser_plugin {
public:
  /// Create a store for passive partitions.
  [[nodiscard]] virtual caf::expected<std::unique_ptr<passive_store>>
  make_passive_store() const = 0;

  /// Create a store for active partitions.
  /// @param vast_config The vast node configuration.
  [[nodiscard]] virtual caf::expected<std::unique_ptr<active_store>>
  make_active_store() const = 0;

private:
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(accountant_actor accountant, filesystem_actor fs,
                     const vast::uuid& id) const final;

  [[nodiscard]] caf::expected<store_actor>
  make_store(accountant_actor accountant, filesystem_actor fs,
             std::span<const std::byte> header) const final;

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> final;

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> final;
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
  static caf::expected<plugin_ptr>
  make_dynamic(const char* filename, caf::actor_system_config& cfg) noexcept;

  /// Take ownership of a static plugin.
  /// @param instance The plugin instance.
  /// @param deleter A deleter for the plugin instance.
  /// @param version The version of the plugin.
  static plugin_ptr make_static(plugin* instance, void (*deleter)(plugin*),
                                const char* version) noexcept;

  /// Take ownership of a builtin.
  /// @param instance The plugin instance.
  /// @param deleter A deleter for the plugin instance.
  /// @param version The version of the plugin.
  static plugin_ptr make_builtin(plugin* instance, void (*deleter)(plugin*),
                                 const char* version) noexcept;

  /// Default-construct an invalid plugin.
  plugin_ptr() noexcept;

  /// Unload a plugin and its required resources.
  ~plugin_ptr() noexcept;

  /// Forbid copying of plugins.
  plugin_ptr(const plugin_ptr&) = delete;
  plugin_ptr& operator=(const plugin_ptr&) = delete;

  /// Move-construction and move-assignment.
  plugin_ptr(plugin_ptr&& other) noexcept;
  plugin_ptr& operator=(plugin_ptr&& rhs) noexcept;

  /// Pointer facade.
  explicit operator bool() const noexcept;
  const plugin* operator->() const noexcept;
  plugin* operator->() noexcept;
  const plugin& operator*() const noexcept;
  plugin& operator&() noexcept;

  /// Downcast a plugin to a more specific plugin type.
  /// @tparam Plugin The specific plugin type to try to downcast to.
  /// @returns A pointer to the downcasted plugin, or 'nullptr' on failure.
  template <class Plugin>
  [[nodiscard]] const Plugin* as() const {
    static_assert(std::is_base_of_v<plugin, Plugin>, "'Plugin' must be derived "
                                                     "from 'vast::plugin'");
    return dynamic_cast<const Plugin*>(instance_);
  }

  /// Downcast a plugin to a more specific plugin type.
  /// @tparam Plugin The specific plugin type to try to downcast to.
  /// @returns A pointer to the downcasted plugin, or 'nullptr' on failure.
  template <class Plugin>
  Plugin* as() {
    static_assert(std::is_base_of_v<plugin, Plugin>, "'Plugin' must be derived "
                                                     "from 'vast::plugin'");
    return dynamic_cast<Plugin*>(instance_);
  }

  /// Returns the plugin version.
  [[nodiscard]] const char* version() const noexcept;

  /// Returns the plugins type.
  [[nodiscard]] enum type type() const noexcept;

  /// Compare two plugins.
  friend bool operator==(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept;
  friend std::strong_ordering
  operator<=>(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept;

  /// Compare a plugin by its name.
  friend bool operator==(const plugin_ptr& lhs, std::string_view rhs) noexcept;
  friend std::strong_ordering
  operator<=>(const plugin_ptr& lhs, std::string_view rhs) noexcept;

private:
  /// Create a plugin_ptr.
  plugin_ptr(void* library, plugin* instance, void (*deleter)(plugin*),
             const char* version, enum type type) noexcept;

  /// Helper function to release ownership of a plugin.
  void release() noexcept;

  /// Implementation details.
  void* library_ = {};
  plugin* instance_ = {};
  void (*deleter_)(plugin*) = {};
  const char* version_ = nullptr;
  enum type type_ = {};
};

} // namespace vast

// -- template function definitions -------------------------------------------

namespace vast::plugins {
template <class Plugin>
const Plugin* find(std::string_view name) noexcept {
  const auto& plugins = get();
  const auto found = std::find(plugins.begin(), plugins.end(), name);
  if (found == plugins.end())
    return nullptr;
  return found->template as<Plugin>();
}

template <class Plugin>
generator<const Plugin*> get() noexcept {
  for (auto const& plugin : get())
    if (auto const* specific_plugin = plugin.as<Plugin>())
      co_yield specific_plugin;
}

} // namespace vast::plugins

// -- helper macros ------------------------------------------------------------

#if defined(VAST_ENABLE_BUILTINS)
#  define VAST_PLUGIN_VERSION nullptr
#else
extern const char* VAST_PLUGIN_VERSION;
#endif

#if defined(VAST_ENABLE_STATIC_PLUGINS) && defined(VAST_ENABLE_BUILTINS)

#  error "Plugins cannot be both static and builtin"

#elif defined(VAST_ENABLE_STATIC_PLUGINS) || defined(VAST_ENABLE_BUILTINS)

#  if defined(VAST_ENABLE_STATIC_PLUGINS)
#    define VAST_MAKE_PLUGIN ::vast::plugin_ptr::make_static
#  else
#    define VAST_MAKE_PLUGIN ::vast::plugin_ptr::make_builtin
#  endif

#  define VAST_REGISTER_PLUGIN(name)                                           \
    template <class>                                                           \
    struct auto_register_plugin;                                               \
    template <>                                                                \
    struct auto_register_plugin<name> {                                        \
      auto_register_plugin() {                                                 \
        static_cast<void>(flag);                                               \
      }                                                                        \
      static auto init() -> bool {                                             \
        ::vast::plugins::get_mutable().push_back(VAST_MAKE_PLUGIN(             \
          new (name), /* NOLINT(cppcoreguidelines-owning-memory) */            \
          +[](::vast::plugin* plugin) noexcept {                               \
            delete plugin; /* NOLINT(cppcoreguidelines-owning-memory) */       \
          },                                                                   \
          VAST_PLUGIN_VERSION));                                               \
        return true;                                                           \
      }                                                                        \
      inline static auto flag = init();                                        \
    };

#  define VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name)                           \
    struct auto_register_type_id_##name {                                      \
      auto_register_type_id_##name() {                                         \
        static_cast<void>(flag);                                               \
      }                                                                        \
      static auto init() -> bool {                                             \
        ::vast::plugins::get_static_type_id_blocks().emplace_back(             \
          ::vast::plugin_type_id_block{::caf::id_block::name::begin,           \
                                       ::caf::id_block::name::end},            \
          +[]() noexcept {                                                     \
            caf::init_global_meta_objects<::caf::id_block::name>();            \
          });                                                                  \
        return true;                                                           \
      }                                                                        \
      inline static auto flag = init();                                        \
    };

#  define VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK_2(name1, name2)                   \
    VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name1)                                \
    VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name2)

#else

#  define VAST_REGISTER_PLUGIN(name)                                           \
    extern "C" auto vast_plugin_create()->::vast::plugin* {                    \
      /* NOLINTNEXTLINE(cppcoreguidelines-owning-memory) */                    \
      return new (name);                                                       \
    }                                                                          \
    extern "C" auto vast_plugin_destroy(class ::vast::plugin* plugin)->void {  \
      /* NOLINTNEXTLINE(cppcoreguidelines-owning-memory) */                    \
      delete plugin;                                                           \
    }                                                                          \
    extern "C" auto vast_plugin_version()->const char* {                       \
      return VAST_PLUGIN_VERSION;                                              \
    }                                                                          \
    extern "C" auto vast_libvast_version()->const char* {                      \
      return ::vast::version::version;                                         \
    }                                                                          \
    extern "C" auto vast_libvast_build_tree_hash()->const char* {              \
      return ::vast::version::build::tree_hash;                                \
    }

#  define VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name)                            \
    extern "C" auto vast_plugin_register_type_id_block()->void {                \
      caf::init_global_meta_objects<::caf::id_block::name>();                   \
    }                                                                           \
    extern "C" auto vast_plugin_type_id_block()->::vast::plugin_type_id_block { \
      return {::caf::id_block::name::begin, ::caf::id_block::name::end};        \
    }

#  define VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK_2(name1, name2)                    \
    extern "C" auto vast_plugin_register_type_id_block()->void {                \
      caf::init_global_meta_objects<::caf::id_block::name1>();                  \
      caf::init_global_meta_objects<::caf::id_block::name2>();                  \
    }                                                                           \
    extern "C" auto vast_plugin_type_id_block()->::vast::plugin_type_id_block { \
      return {::caf::id_block::name1::begin < ::caf::id_block::name2::begin     \
                ? ::caf::id_block::name1::begin                                 \
                : ::caf::id_block::name2::begin,                                \
              ::caf::id_block::name1::end > ::caf::id_block::name2::end         \
                ? ::caf::id_block::name1::end                                   \
                : ::caf::id_block::name2::end};                                 \
    }

#endif

#define VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK(...)                                \
  VAST_PP_OVERLOAD(VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK_, __VA_ARGS__)           \
  (__VA_ARGS__)
