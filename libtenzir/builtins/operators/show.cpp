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

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    if (auto plugin = get()) {
      return plugin->show(ctrl);
    }
    return
      [](operator_control_plane& ctrl,
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
      // We moved `show version` to `version` when we made the `show` operator
      // always report a remote location for consistency. This makes it less of
      // a breaking change.
      if (aspect->inner == "version") {
        auto op = pipeline::internal_parse_as_operator("version");
        if (not op) {
          diagnostic::error("failed to parse `version` operator: {}",
                            op.error())
            .throw_();
        }
        return std::move(*op);
      }
      if (aspect->inner == "partitions") {
        auto op = pipeline::internal_parse_as_operator("partitions");
        if (not op) {
          diagnostic::error("failed to parse `partitions` operator: {}",
                            op.error())
            .throw_();
        }
        return std::move(*op);
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
