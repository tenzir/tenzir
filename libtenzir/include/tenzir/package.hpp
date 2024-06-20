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

struct package_autostart {
  bool created = false;
  bool completed = false;
  bool failed = false;

  friend auto inspect(auto& f, package_autostart& x) -> bool {
    return f.object(x)
      .pretty_name("package_autostart")
      .fields(f.field("created", x.created), f.field("failed", x.failed),
              f.field("completed", x.completed));
  }
};

struct package_autodelete {
  bool completed = false;
  bool failed = false;
  bool stopped = false;

  friend auto inspect(auto& f, package_autodelete& x) -> bool {
    return f.object(x)
      .pretty_name("package_autodelete")
      .fields(f.field("failed", x.failed), f.field("stopped", x.stopped),
              f.field("completed", x.completed));
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
  std::string name; // required
  std::string description;
  std::string type; // required
  std::string default_;

  static auto parse(const view<record>& data) -> caf::expected<package_input>;

  friend auto inspect(auto& f, package_input& x) -> bool {
    return f.object(x)
      .pretty_name("package_input")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("type", x.type), f.field("default", x.default_));
  }
};

struct package_pipeline final {
  struct label {
    std::string text;
    std::string color;

    friend auto inspect(auto& f, label& x) -> bool {
      return f.object(x).pretty_name("label").fields(f.field("text", x.text),
                                                     f.field("color", x.color));
    }
  };

  std::string name;
  std::string description;
  std::string definition; // required
  std::vector<label> labels;
  bool disabled;
  // FIXME: add these fields
  // duration ttl = caf::infinite;
  package_autostart autostart_params = {};
  package_autodelete autodelete_params = {};
  std::optional<duration> retry_delay = {};

  static auto
  parse(const view<record>& data) -> caf::expected<package_pipeline>;

  friend auto inspect(auto& f, package_pipeline& x) -> bool {
    return f.object(x)
      .pretty_name("package_pipeline")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("definition", x.definition), f.field("labels", x.labels),
              f.field("disabled", x.disabled));
  }
};

struct package_context final {
  std::string type;
  std::string description;
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
  std::string name;
  std::string description;
  std::string definition; // required

  static auto parse(const view<record>& data) -> caf::expected<package_snippet>;

  friend auto inspect(auto& f, package_snippet& x) -> bool {
    return f.object(x)
      .pretty_name("package_snippet")
      .fields(f.field("name", x.name), f.field("description", x.description),
              f.field("definition", x.definition));
  }
};

struct package final {
  std::string id = {};   // required
  std::string name = {}; // required
  std::string author = {};
  detail::flat_map<std::string, package_input> inputs;
  detail::flat_map<std::string, package_pipeline> pipelines;
  detail::flat_map<std::string, package_context> contexts;
  detail::flat_map<std::string, package_snippet> snippets;

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
