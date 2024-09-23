//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/collect.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

#include <memory>
#include <set>
#include <string>

namespace tenzir::plugins::show {

namespace {

class show_operator final : public crtp_operator<show_operator> {
public:
  show_operator() = default;

  explicit show_operator(std::string aspect_plugin)
    : aspect_plugin_{std::move(aspect_plugin)} {
  }

  auto operator()(exec_ctx ctx) const -> generator<table_slice> {
    if (auto plugin = get()) {
      return plugin->show(ctrl);
    }
    return
      [](exec_ctx ctx,
         std::vector<const aspect_plugin*> plugins) -> generator<table_slice> {
        for (const auto* plugin : plugins) {
          for (auto&& slice : plugin->show(ctrl)) {
            co_yield std::move(slice);
          }
        }
      }(ctrl, get_all());
  }

  auto name() const -> std::string override {
    return "show";
  }

  auto detached() const -> bool override {
    return true;
  }

  auto internal() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::remote;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, show_operator& x) -> bool {
    return f.apply(x.aspect_plugin_);
  }

private:
  auto get() const -> const aspect_plugin* {
    if (aspect_plugin_) {
      return plugins::find<aspect_plugin>(*aspect_plugin_);
    }
    return nullptr;
  }

  auto get_all() const -> std::vector<const aspect_plugin*> {
    return collect(plugins::get<aspect_plugin>());
  }

  std::optional<std::string> aspect_plugin_;
};

class plugin final : public virtual operator_plugin<show_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"show", "https://docs.tenzir.com/"
                                          "operators/show"};
    auto aspect = std::optional<located<std::string>>{};
    parser.add(aspect, "<aspect>");
    parser.parse(p);
    if (aspect) {
      // We're in the process of removing aspect plugins in favor of operators.
      // For backwards compatibility, this adds support for `show <aspect>` by
      // redirecting to the operator.
      static const auto removed_aspects
        = std::vector<std::pair<std::string, std::string>>{
          {"build", "version | set build.features = features | unflatten | "
                    "yield build | set #schema = \"tenzir.build\""},
          {"config", "config"},
          {"dependencies", "version | yield dependencies[] | set #schema = "
                           "\"tenzir.dependency\""},
          {"partitions", "partitions"},
          {"plugins", "plugins"},
          {"schemas", "schemas"},
          {"version",
           "version | drop features, build, dependencies | rename build=tag"},
          {"fields", "fields"},
        };
      for (const auto& [before, after] : removed_aspects) {
        if (aspect->inner == before) {
          return check(pipeline::internal_parse_as_operator(after));
        }
      }
      auto available = std::map<std::string, std::string>{};
      for (const auto& aspect : collect(plugins::get<aspect_plugin>()))
        available.emplace(aspect->aspect_name(), aspect->name());
      if (not available.contains(aspect->inner)) {
        auto aspects = std::vector<std::string>{};
        for (const auto& [aspect_name, plugin_name] : available)
          aspects.push_back(aspect_name);
        diagnostic::error("aspect `{}` could not be found", aspect->inner)
          .primary(aspect->source)
          .hint("must be one of {}", fmt::join(aspects, ", "))
          .throw_();
      }
      return std::make_unique<show_operator>(available[aspect->inner]);
    }
    return std::make_unique<show_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::show

TENZIR_REGISTER_PLUGIN(tenzir::plugins::show::plugin)
