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
#include "tenzir/expression.hpp"
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
/// @param cfg The actor system configuration of Tenzir for registering
/// additional type ID blocks.
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

  // Deinitializes a plugin.
  virtual auto deinitialize() -> void {
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

  /// Components that should be created before the current one so initialization
  /// can succeed.
  /// Defaults to empty list.
  virtual auto wanted_components() const -> std::vector<std::string>;

  /// Creates an actor as a component in the NODE.
  /// @param node A stateful pointer to the NODE actor.
  /// @returns The actor handle to the NODE component.
  /// @note This function runs in the actor context of the NODE actor and can
  /// safely access the NODE's state.
  virtual component_plugin_actor
  make_component(node_actor::stateful_pointer<node_state> node) const
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
  [[nodiscard]] virtual std::pair<std::unique_ptr<command>, command::factory>
  make_command() const = 0;
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
  if (not p) {
    f.set_error(caf::make_error(
      ec::serialization_error,
      fmt::format("serialization plugin `{}` for `{}` not found", name,
                  caf::detail::pretty_type_name(typeid(Base)))));
    return false;
  }
  if (auto dbg = as_debug_writer(f)) {
    return dbg->prepend("{} ", name) && p->serialize(std::ref(f), x);
  }
  return f.apply(name) && p->serialize(std::ref(f), x);
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
    if (not p) {
      f.set_error(caf::make_error(
        ec::serialization_error,
        fmt::format("serialization plugin `{}` for `{}` not found", name,
                    caf::detail::pretty_type_name(typeid(Base)))));
      return false;
    }
    p->deserialize(f, x);
    return x != nullptr;
  } else {
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
    TENZIR_ASSERT(x);
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

  /// Return the value that should be used if there is no input.
  virtual auto aggregation_default() const -> data = 0;
};

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
  /// @param accountant The actor handle of the accountant.
  /// @param fs The actor handle of a filesystem.
  /// @param id The partition id for which we want to create a store. Can be
  /// used as a unique key by the implementation.
  /// @returns A handle to the store builder actor to add events to, and a
  /// header that uniquely identifies this store for later use in `make_store`.
  [[nodiscard]] virtual caf::expected<builder_and_header>
  make_store_builder(accountant_actor accountant, filesystem_actor fs,
                     const tenzir::uuid& id) const
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
class store_plugin : public virtual store_actor_plugin {
public:
  /// Create a store for passive partitions.
  [[nodiscard]] virtual caf::expected<std::unique_ptr<passive_store>>
  make_passive_store() const = 0;

  /// Create a store for active partitions.
  /// @param tenzir_config The tenzir node configuration.
  [[nodiscard]] virtual caf::expected<std::unique_ptr<active_store>>
  make_active_store() const = 0;

private:
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(accountant_actor accountant, filesystem_actor fs,
                     const tenzir::uuid& id) const final;

  [[nodiscard]] caf::expected<store_actor>
  make_store(accountant_actor accountant, filesystem_actor fs,
             std::span<const std::byte> header) const final;
};

// -- lookup table plugin -----------

class lookup_table_plugin : public virtual plugin {
public:
  virtual auto apply_lookup(std::vector<table_slice> slices,
                            std::unordered_set<std::string> fields,
                            record indicators) const -> std::vector<table_slice>
    = 0;
};

// -- context plugin -----------------------------------------------------------

class context {
public:
  using parameter_map
    = std::unordered_map<std::string, std::optional<std::string>>;
  using make_query_type
    = std::function<auto(parameter_map parameters,
                         const std::vector<std::string>& fields)
                      ->caf::expected<expression>>;

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

  /// Create a snapshot of the initial expression.
  virtual auto snapshot(parameter_map parameters,
                        const std::vector<std::string>& fields) const
    -> caf::expected<expression>
    = 0;

  /// Serializes a context for persistence.
  virtual auto save() const -> caf::expected<save_result> = 0;

  /// Returns a callback for retroactive lookups.
  virtual auto make_query() -> make_query_type = 0;
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
  [[nodiscard]] virtual auto make_collector() const -> caf::expected<collector>
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
  static caf::expected<plugin_ptr>
  make_dynamic(const char* filename, caf::actor_system_config& cfg) noexcept;

  /// Take ownership of a static plugin.
  /// @param instance The plugin instance.
  /// @param deleter A deleter for the plugin instance.
  /// @param version The version of the plugin.
  /// @param dependencies The plugin's dependencies.
  static plugin_ptr
  make_static(plugin* instance, void (*deleter)(plugin*), const char* version,
              std::vector<std::string> dependencies) noexcept;

  /// Take ownership of a builtin.
  /// @param instance The plugin instance.
  /// @param deleter A deleter for the plugin instance.
  /// @param version The version of the plugin.
  /// @param dependencies The plugin's dependencies.
  static plugin_ptr
  make_builtin(plugin* instance, void (*deleter)(plugin*), const char* version,
               std::vector<std::string> dependencies) noexcept;

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
                                                     "from 'tenzir::plugin'");
    return dynamic_cast<const Plugin*>(ctrl_->instance);
  }

  /// Downcast a plugin to a more specific plugin type.
  /// @tparam Plugin The specific plugin type to try to downcast to.
  /// @returns A pointer to the downcasted plugin, or 'nullptr' on failure.
  template <class Plugin>
  Plugin* as() {
    static_assert(std::is_base_of_v<plugin, Plugin>, "'Plugin' must be derived "
                                                     "from 'tenzir::plugin'");
    return dynamic_cast<Plugin*>(ctrl_->instance);
  }

  /// Returns the plugin version.
  [[nodiscard]] const char* version() const noexcept;

  /// Returns the plugin's dependencies.
  [[nodiscard]] const std::vector<std::string>& dependencies() const noexcept;

  /// Returns the plugins type.
  [[nodiscard]] enum type type() const noexcept;

  /// Bump the reference count of all dependencies.
  auto reference_dependencies() noexcept -> void;

  /// Compare two plugins.
  friend bool operator==(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept;
  friend std::strong_ordering
  operator<=>(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept;

  /// Compare a plugin by its name.
  friend bool operator==(const plugin_ptr& lhs, std::string_view rhs) noexcept;
  friend std::strong_ordering
  operator<=>(const plugin_ptr& lhs, std::string_view rhs) noexcept;

private:
  struct control_block {
    control_block(void* library, plugin* instance, void (*deleter)(plugin*),
                  const char* version, std::vector<std::string> dependencies,
                  enum type type) noexcept;
    ~control_block() noexcept;

    control_block(const control_block&) = delete;
    control_block& operator=(const control_block&) = delete;
    control_block(control_block&& other) noexcept = delete;
    control_block& operator=(control_block&& rhs) noexcept = delete;

    void* library = {};
    plugin* instance = {};
    void (*deleter)(plugin*) = {};
    const char* version = nullptr;
    std::vector<std::string> dependencies = {};
    std::vector<std::shared_ptr<control_block>> dependencies_ctrl = {};
    enum type type = {};
  };

  /// Create a plugin_ptr.
  explicit plugin_ptr(std::shared_ptr<control_block> ctrl) noexcept;

  /// The plugin's control block.
  std::shared_ptr<control_block> ctrl_ = {};
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
const Plugin* find(std::string_view name) noexcept {
  const auto& plugins = get();
  const auto found = std::find(plugins.begin(), plugins.end(), name);
  if (found == plugins.end()) {
    return nullptr;
  }
  return found->template as<Plugin>();
}

inline const operator_parser_plugin*
find_operator(std::string_view name) noexcept {
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
generator<const Plugin*> get() noexcept {
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

#  define TENZIR_REGISTER_PLUGIN(name)                                         \
    template <class>                                                           \
    struct auto_register_plugin;                                               \
    template <>                                                                \
    struct auto_register_plugin<name> {                                        \
      auto_register_plugin() {                                                 \
        static_cast<void>(flag);                                               \
      }                                                                        \
      static auto init() -> bool {                                             \
        ::tenzir::plugins::get_mutable().push_back(TENZIR_MAKE_PLUGIN(         \
          new (name), /* NOLINT(cppcoreguidelines-owning-memory) */            \
          +[](::tenzir::plugin* plugin) noexcept {                             \
            delete plugin; /* NOLINT(cppcoreguidelines-owning-memory) */       \
          },                                                                   \
          TENZIR_PLUGIN_VERSION));                                             \
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
            caf::init_global_meta_objects<::caf::id_block::name>();            \
          });                                                                  \
        return true;                                                           \
      }                                                                        \
      inline static auto flag = init();                                        \
    };

#  define TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_2(name1, name2)                 \
    TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name1)                              \
    TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_1(name2)

#else

#  define TENZIR_REGISTER_PLUGIN(name)                                         \
    extern "C" auto tenzir_plugin_create() -> ::tenzir::plugin* {              \
      /* NOLINTNEXTLINE(cppcoreguidelines-owning-memory) */                    \
      return new (name);                                                       \
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
      caf::init_global_meta_objects<::caf::id_block::name>();                  \
    }                                                                          \
    extern "C" auto tenzir_plugin_type_id_block()                              \
      -> ::tenzir::plugin_type_id_block {                                      \
      return {::caf::id_block::name::begin, ::caf::id_block::name::end};       \
    }

#  define TENZIR_REGISTER_PLUGIN_TYPE_ID_BLOCK_2(name1, name2)                 \
    extern "C" auto tenzir_plugin_register_type_id_block() -> void {           \
      caf::init_global_meta_objects<::caf::id_block::name1>();                 \
      caf::init_global_meta_objects<::caf::id_block::name2>();                 \
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
