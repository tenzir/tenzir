//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/context.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/source.hpp>
#include <tenzir/view.hpp>

#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir {

class ExternPrelude;

struct package_source final {
  std::string repository;
  std::string directory;
  std::string revision;

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_source>;

  friend auto inspect(auto& f, package_source& x) -> bool {
    return f.object(x)
      .pretty_name("package_source")
      .fields(f.field("repository", x.repository),
              f.field("directory", x.directory),
              f.field("revision", x.revision));
  }
};

struct package_config final {
  Option<package_source> source;
  Option<std::string> version;
  detail::flat_map<std::string, std::string> inputs;
  record metadata;  // opaque extra data that can be set at install time
  record overrides; // overrides for fields in the package definition

  bool disabled = {};

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_config>;

  friend auto inspect(auto& f, package_config& x) -> bool {
    return f.object(x)
      .pretty_name("package_config")
      .fields(f.field("source", x.source), f.field("inputs", x.inputs),
              f.field("version", x.version), f.field("metadata", x.metadata),
              f.field("overrides", x.overrides),
              f.field("disabled", x.disabled));
  }
};

struct package_input final {
  std::string name; // required to be non-empty
  std::string type; // optional type hint
  Option<std::string> description;
  Option<std::string> default_;

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_input>;

  friend auto inspect(auto& f, package_input& x) -> bool {
    return f.object(x)
      .pretty_name("package_input")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("type", x.type), f.field("default", x.default_));
  }
};

struct package_operator_parameter final {
  std::string name; // required to be non-empty
  Option<std::string> type;
  Option<std::string> description;
  Option<std::string> default_;

  auto to_record() const -> record;

  static auto parse(const view<record>& data)
    -> caf::expected<package_operator_parameter>;

  friend auto inspect(auto& f, package_operator_parameter& x) -> bool {
    return f.object(x)
      .pretty_name("package_operator_parameter")
      .fields(f.field("name", x.name), f.field("type", x.type),
              f.field("description", x.description),
              f.field("default", x.default_));
  }
};

struct package_operator_arguments final {
  std::vector<package_operator_parameter> positional;
  std::vector<package_operator_parameter> named;

  friend auto inspect(auto& f, package_operator_arguments& x) -> bool {
    return f.object(x)
      .pretty_name("package_operator_arguments")
      .fields(f.field("positional", x.positional), f.field("named", x.named));
  }
};

struct package_operator final {
  Option<std::string> description;
  package_operator_arguments args;
  /// Full source file content (frontmatter + TQL body). Always set.
  Option<Arc<const Source>> file_source;
  /// View into `file_source->text` covering the raw YAML frontmatter (between
  /// the `---` markers, not including them). Empty when there is no frontmatter.
  std::string_view frontmatter;
  /// View into `file_source->text` covering the TQL pipeline body.
  std::string_view tql_body;

  auto to_record() const -> record;

  static auto parse(const view<record>& data)
    -> caf::expected<package_operator>;
  static auto parse(Arc<const Source> source)
    -> caf::expected<package_operator>;

  friend auto inspect(auto& f, package_operator& x) -> bool {
    using Inspector = std::remove_reference_t<decltype(f)>;
    if constexpr (Inspector::is_loading) {
      auto tql_body_str = std::string{};
      if (! f.object(x)
              .pretty_name("package_pipeline")
              .fields(f.field("description", x.description),
                      f.field("definition", tql_body_str),
                      f.field("args", x.args))) {
        return false;
      }
      auto src
        = Source::new_source(std::move(tql_body_str), "<serialized>", false);
      x.file_source = src;
      x.tql_body = src->text;
      return true;
    } else {
      auto tql_body_str = std::string{x.tql_body};
      return f.object(x)
        .pretty_name("package_pipeline")
        .fields(f.field("description", x.description),
                f.field("definition", tql_body_str), f.field("args", x.args));
    }
  }
};

struct package_pipeline final {
  Option<std::string> name;
  Option<std::string> description;
  std::string definition; // required to be non-empty
  bool disabled = false;
  Option<duration> restart_on_error;
  bool unstoppable = false;

  auto to_record() const -> record;

  static auto parse(const view<record>& data)
    -> caf::expected<package_pipeline>;
  static auto parse(const view<record>& data, std::string_view package_path)
    -> caf::expected<package_pipeline>;

  static auto parse(std::string_view input) -> caf::expected<package_pipeline>;

  friend auto inspect(auto& f, package_pipeline& x) -> bool {
    return f.object(x)
      .pretty_name("package_pipeline")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("definition", x.definition),
              f.field("disabled", x.disabled),
              f.field("restart-on-error", x.restart_on_error),
              f.field("unstoppable", x.unstoppable));
  }
};

struct package_context final {
  std::string type
    = "string"; // A type hint for the frontend, ignored by the node.
  Option<std::string> description;
  context_parameter_map arguments = {};
  bool disabled = false;

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_context>;

  friend auto inspect(auto& f, package_context& x) -> bool {
    return f.object(x)
      .pretty_name("package_context")
      .fields(f.field("type", x.type), f.field("description", x.description),
              f.field("arguments", x.arguments),
              f.field("disabled", x.disabled));
  }
};

struct package_example final {
  Option<std::string> name;
  Option<std::string> description;
  std::string definition; // required to be non-empty

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_example>;
  static auto parse(std::string_view input) -> caf::expected<package_example>;

  friend auto inspect(auto& f, package_example& x) -> bool {
    return f.object(x)
      .pretty_name("package_example")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("definition", x.definition));
  }
};

struct package_inputs_map
  : public detail::flat_map<std::string, package_input> {
  using super = detail::flat_map<std::string, package_input>;
  using super::super;
};

struct package_operators_map
  : public detail::flat_map<std::vector<std::string>, package_operator> {
  using super = detail::flat_map<std::vector<std::string>, package_operator>;
  using super::super;
};

struct package_pipelines_map
  : public detail::flat_map<std::string, package_pipeline> {
  using super = detail::flat_map<std::string, package_pipeline>;
  using super::super;
};

struct package_contexts_map
  : public detail::flat_map<std::string, package_context> {
  using super = detail::flat_map<std::string, package_context>;
  using super::super;
};

using package_examples_list = std::vector<package_example>;

/// A package that a package uses, as its `dependencies` describe it: by a
/// requirement on its version, by a git repository, or by a path.
///
/// The engine needs only the names, which say what a package may use. Where a
/// dependency comes from, and which version of it, is for whoever resolves
/// and fetches packages.
struct PackageDependency final {
  /// The versions it accepts, such as `^1.2`.
  Option<std::string> version;
  /// The repository it comes from, when that is not the library.
  Option<std::string> git;
  /// The revision of `git` it is pinned to.
  Option<std::string> rev;
  /// Where it lives relative to the package, for one written alongside it.
  Option<std::string> path;

  auto to_record() const -> record;

  /// Parses a requirement string, such as `^1.2`, or a record.
  static auto parse(data_view const& data) -> caf::expected<PackageDependency>;

  friend auto inspect(auto& f, PackageDependency& x) -> bool {
    return f.object(x)
      .pretty_name("package_dependency")
      .fields(f.field("version", x.version), f.field("git", x.git),
              f.field("rev", x.rev), f.field("path", x.path));
  }
};

/// The packages a package uses, by their ids.
using PackageDependencies = detail::flat_map<std::string, PackageDependency>;

/// The `dependencies` of a `package.yaml`: a record from package ids to what
/// each one is, or nothing.
auto parse_package_dependencies(data_view const& data)
  -> caf::expected<PackageDependencies>;

/// The files of a package by their path relative to its directory, with `/`
/// separating the components, such as `operators/ingest.tql`.
using package_files = std::map<std::string, std::string, std::less<>>;

struct package final {
  std::string id;   // required to be non-empty
  std::string name; // required to be non-empty
  /// The version this package is, such as `1.2.0`.
  Option<std::string> version;
  /// The packages this package uses, by their ids. A package sees itself and
  /// these, and no other.
  PackageDependencies dependencies;
  Option<std::string> author;
  Option<std::string> description;
  Option<std::string> package_icon;
  Option<std::string> author_icon;
  std::vector<std::string> categories;

  package_inputs_map inputs;
  package_operators_map operators;
  package_pipelines_map pipelines;
  package_contexts_map contexts;
  package_examples_list examples;
  /// Raw contents of the package's `constants.tql` file. Compiled and
  /// const-evaluated into value entities (the `let` namespace) when the
  /// package is loaded.
  std::string lets;

  // Packages are kept in the library without a `config`. When installing a
  // package, both the package definition and a config must be available.
  // Different deployment methods achieve this in different ways: Either by
  // modifying the original package definition directly, by placing them next
  // to each other in a directory, or by including an `overrides` section in
  // the input.
  Option<package_config> config;

  static auto parse(const view<record>& data) -> caf::expected<package>;
  static auto parse(const view<record>& data, std::string_view package_path)
    -> caf::expected<package>;

  /// Loads the package in `dir`.
  static auto load(const std::filesystem::path& dir, diagnostic_handler& dh,
                   bool only_entities) -> failure_or<package>;

  /// Loads a package from its files, as they would be in its directory.
  /// `origin` names where they come from in diagnostics, and `default_id` is
  /// the package's id if its `package.yaml` has none.
  static auto load(package_files const& files, std::string_view origin,
                   std::string_view default_id, diagnostic_handler& dh,
                   bool only_entities) -> failure_or<package>;

  auto to_record() const -> record;

  friend auto inspect(auto& f, package& x) -> bool {
    return f.object(x).pretty_name("package").fields(
      f.field("id", x.id), f.field("name", x.name),
      f.field("version", x.version), f.field("dependencies", x.dependencies),
      f.field("author", x.author), f.field("description", x.description),
      f.field("package_icon", x.package_icon),
      f.field("author_icon", x.author_icon),
      f.field("categories", x.categories), f.field("inputs", x.inputs),
      f.field("operators", x.operators), f.field("pipelines", x.pipelines),
      f.field("contexts", x.contexts), f.field("examples", x.examples),
      f.field("lets", x.lets), f.field("config", x.config));
  }
};

} // namespace tenzir

namespace tenzir {

struct module_def;

auto build_package_operator_module(const package& pkg, diagnostic_handler& dh,
                                   registry const& reg,
                                   SourceMap* source_map = nullptr)
  -> failure_or<std::unique_ptr<module_def>>;

/// Compiles and const-evaluates the package's `constants.tql`, adding each
/// binding as a value entity to `pkg_mod`. Later bindings may reference earlier
/// ones.
auto build_package_lets(const package& pkg, module_def& pkg_mod,
                        diagnostic_handler& dh, registry const& reg,
                        SourceMap* source_map = nullptr) -> failure_or<void>;

/// Builds the combined registry module for a package: its operators and its
/// `let` bindings, merged under the package's module name. Operators and lets
/// occupy different namespaces within an entity set, so they coexist.
auto build_package_module(const package& pkg, diagnostic_handler& dh,
                          registry const& reg, SourceMap* source_map = nullptr)
  -> failure_or<std::unique_ptr<module_def>>;

auto package_module_name(std::string_view package_id) -> std::string;

/// What code that declares `dependencies` may name of the packages: each of
/// them, by its module name.
auto extern_prelude(PackageDependencies const& dependencies) -> ExternPrelude;

/// What the code of packages may name of the packages.
enum class PackageVisibility {
  /// Each package itself and what it declares.
  declared,
  /// Every package. Deprecated: for packages that do not declare what they
  /// use, whose code is then resolved only where it is used.
  all_deprecated,
};

/// Fails unless every package that `dependencies` names is among `packages`,
/// saying that `user` uses it.
auto require_dependencies(std::string_view user,
                          PackageDependencies const& dependencies,
                          std::span<package const> packages,
                          diagnostic_handler& dh) -> failure_or<void>;

/// Adds the operators and `let` bindings of `packages` to `reg`, each under its
/// module in the `packages` root.
///
/// With `PackageVisibility::declared`, each package may name itself and what
/// it declares, which has to be among `packages`. Packages are resolved here,
/// after the packages they use, and may not use each other.
///
/// Fails without changing `reg` if a package does not build or resolve, if a
/// dependency is missing, if packages use each other, or if two packages have
/// the same module name.
auto add_packages(registry& reg, std::span<package const> packages,
                  diagnostic_handler& dh, SourceMap* source_map,
                  PackageVisibility visibility) -> failure_or<void>;

} // namespace tenzir
