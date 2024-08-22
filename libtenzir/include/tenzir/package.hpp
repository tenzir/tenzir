//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/view.hpp>

#include <string>
#include <vector>

namespace tenzir {

struct package_source final {
  std::string repository = {};
  std::string directory = {};
  std::string revision = {};

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
  std::optional<package_source> source = {};
  detail::flat_map<std::string, std::string> inputs = {};
  // TODO: Add an `overrides` field.
  // package_overrides overrides = {};

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_config>;

  friend auto inspect(auto& f, package_config& x) -> bool {
    return f.object(x)
      .pretty_name("package_config")
      .fields(f.field("source", x.source), f.field("inputs", x.inputs));
  }
};

struct package_input final {
  std::string name = {}; // required to be non-empty
  std::string type = {}; // required to be non-empty
  std::optional<std::string> description = {};
  std::optional<std::string> default_ = {};

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_input>;

  friend auto inspect(auto& f, package_input& x) -> bool {
    return f.object(x)
      .pretty_name("package_input")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("type", x.type), f.field("default", x.default_));
  }
};

struct package_pipeline final {
  std::optional<std::string> name = {};
  std::optional<std::string> description = {};
  std::string definition = {}; // required to be non-empty
  bool disabled = false;
  std::optional<duration> restart_on_error = {};
  bool unstoppable = false;

  auto to_record() const -> record;

  static auto
  parse(const view<record>& data) -> caf::expected<package_pipeline>;

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
  std::string type = {};
  std::optional<std::string> description = {};
  tenzir::context::parameter_map arguments = {};
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

struct package_snippet final {
  std::optional<std::string> name = {};
  std::optional<std::string> description = {};
  std::string definition = {}; // required to be non-empty

  auto to_record() const -> record;

  static auto parse(const view<record>& data) -> caf::expected<package_snippet>;

  friend auto inspect(auto& f, package_snippet& x) -> bool {
    return f.object(x)
      .pretty_name("package_snippet")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("definition", x.definition));
  }
};

struct package_inputs_map
  : public detail::flat_map<std::string, package_input> {
  using super = detail::flat_map<std::string, package_input>;
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

using package_snippets_list = std::vector<package_snippet>;

struct package final {
  std::string id = {};   // required to be non-empty
  std::string name = {}; // required to be non-empty
  std::optional<std::string> author = {};
  std::optional<std::string> description = {};
  std::optional<std::string> package_icon = {};
  std::optional<std::string> author_icon = {};

  package_inputs_map inputs;
  package_pipelines_map pipelines;
  package_contexts_map contexts;
  package_snippets_list snippets;

  // Packages are kept in the library without a `config`. When installing a
  // package, both the package definition and a config must be available.
  // Different deployment methods achieve this in different ways: Either by
  // modifying the original package definition directly, by placing them next
  // to each other in a directory, or by including an `overrides` section in
  // the input.
  std::optional<package_config> config = {};

  static auto parse(const view<record>& data) -> caf::expected<package>;

  auto to_record() const -> record;

  friend auto inspect(auto& f, package& x) -> bool {
    return f.object(x).pretty_name("package").fields(
      f.field("id", x.id), f.field("name", x.name), f.field("author", x.author),
      f.field("description", x.description),
      f.field("package_icon", x.package_icon),
      f.field("author_icon", x.author_icon), f.field("inputs", x.inputs),
      f.field("pipelines", x.pipelines), f.field("contexts", x.contexts),
      f.field("snippets", x.snippets), f.field("config", x.config));
  }
};

} // namespace tenzir
