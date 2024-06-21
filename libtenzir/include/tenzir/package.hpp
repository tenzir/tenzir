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
#include <tenzir/view.hpp>

#include <string>
#include <vector>

namespace tenzir {

struct package_source final {
  std::string repository = {};
  std::string directory = {};
  std::string revision = {};

  static auto parse(const view<record>& data) -> caf::expected<package_source>;

  friend auto inspect(auto& f, package_source& x) -> bool {
    return f.object(x)
      .pretty_name("package_source")
      .fields(f.field("repository", x.repository),
              f.field("directory", x.directory),
              f.field("revision", x.revision));
  }
};

// TODO: Reconsider naming
struct package_deployment final {
  package_source source = {};
  detail::flat_map<std::string, std::string> inputs;

  static auto
  parse(const view<record>& data) -> caf::expected<package_deployment>;

  friend auto inspect(auto& f, package_deployment& x) -> bool {
    return f.object(x)
      .pretty_name("package_deployment")
      .fields(f.field("source", x.source), f.field("inputs", x.inputs));
  }
};

struct package_input final {
  std::string name = {}; // required
  std::string description = {};
  std::string type = {}; // required
  std::string default_ = {};

  static auto parse(const view<record>& data) -> caf::expected<package_input>;

  friend auto inspect(auto& f, package_input& x) -> bool {
    return f.object(x)
      .pretty_name("package_input")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("type", x.type), f.field("default", x.default_));
  }
};

struct package_pipeline final {
  std::string name = {};
  std::string description = {};
  std::string definition = {}; // required
  // TODO: Consider renaming to `disabled_by_default`.
  bool disabled = false;
  // TODO: Consider adding `do_not_start` and `unstoppable` fields
  // to disable autostart
  std::optional<duration> retry_on_error = {};

  static auto
  parse(const view<record>& data) -> caf::expected<package_pipeline>;

  friend auto inspect(auto& f, package_pipeline& x) -> bool {
    return f.object(x)
      .pretty_name("package_pipeline")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("definition", x.definition),
              // f.field("labels", x.labels),
              f.field("disabled", x.disabled),
              f.field("retry_on_error", x.retry_on_error));
  }
};

struct package_context final {
  std::string type = {};
  std::string description = {};
  detail::flat_map<std::string, std::string> arguments;

  static auto parse(const view<record>& data) -> caf::expected<package_context>;

  friend auto inspect(auto& f, package_context& x) -> bool {
    return f.object(x)
      .pretty_name("package_context")
      .fields(f.field("type", x.type), f.field("description", x.description),
              f.field("arguments", x.arguments));
  }
};

struct package_snippet final {
  std::string name = {};
  std::string description = {};
  std::string definition = {}; // required

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

struct package_contexts_map : detail::flat_map<std::string, package_context> {
  using super = detail::flat_map<std::string, package_context>;
  using super::super;
};

using package_snippets_list = std::vector<package_snippet>;

struct package final {
  std::string id = {};   // required
  std::string name = {}; // required
  std::string author = {};
  std::string description = {};

  package_inputs_map inputs;
  package_pipelines_map pipelines;
  package_contexts_map contexts;
  package_snippets_list snippets;

  package_deployment deployment = {};

  static auto parse(const view<record>& data) -> caf::expected<package>;

  friend auto inspect(auto& f, package& x) -> bool {
    return f.object(x).pretty_name("package").fields(
      f.field("id", x.id), f.field("source", x.name),
      f.field("author", x.author), f.field("inputs", x.inputs),
      f.field("pipelines", x.pipelines), f.field("contexts", x.contexts),
      f.field("snippets", x.snippets), f.field("deployment", x.deployment));
  }
};

} // namespace tenzir
