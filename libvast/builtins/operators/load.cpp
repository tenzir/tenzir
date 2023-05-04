//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/string.hpp>
#include <vast/concept/parseable/vast/identifier.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>
#include <caf/error.hpp>

namespace vast::plugins::load_ {

namespace {

class load_operator final : public crtp_operator<load_operator> {
public:
  explicit load_operator(const loader_plugin& loader,
                         std::vector<std::string> args)
    : loader_plugin_{loader}, args_{std::move(args)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> caf::expected<generator<chunk_ptr>> {
    return loader_plugin_.make_loader(args_, ctrl);
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto to_string() const -> std::string override {
    return fmt::format("load {}{}{}", loader_plugin_.name(),
                       args_.empty() ? "" : " ", escape_operator_args(args_));
  }

protected:
  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    if (input.is<void>()) {
      return tag_v<chunk_ptr>;
    }
    // TODO: Fuse this check with crtp_operator::instantiate()
    return caf::make_error(ec::type_clash,
                           fmt::format("'{}' does not accept {} as input",
                                       to_string(), operator_type_name(input)));
  }

private:
  const loader_plugin& loader_plugin_;
  std::vector<std::string> args_;
};

class plugin final : public virtual operator_plugin {
public:
  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "load";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    auto parsed = parsers::name_args.apply(f, l);
    if (!parsed) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse load operator: '{}'",
                                    pipeline)),
      };
    }
    auto& [name, args] = *parsed;
    const auto* loader = plugins::find<loader_plugin>(name);
    if (!loader) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::lookup_error,
                        fmt::format("no loader found for '{}'", name)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<load_operator>(*loader, std::move(args)),
    };
  }
};

} // namespace

} // namespace vast::plugins::load_

VAST_REGISTER_PLUGIN(vast::plugins::load_::plugin)
