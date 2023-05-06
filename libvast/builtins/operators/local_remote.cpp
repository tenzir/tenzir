//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/string_literal.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>

#include <arrow/type.h>

namespace vast::plugins::local_remote {

namespace {

class local_remote_operator final : public operator_base {
public:
  explicit local_remote_operator(operator_ptr op, std::string name,
                                 operator_location location,
                                 bool allow_unsafe_pipelines)
    : op_{std::move(op)},
      name_{std::move(name)},
      location_{location},
      allow_unsafe_pipelines_{allow_unsafe_pipelines} {
    if (auto* op = dynamic_cast<local_remote_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    VAST_ASSERT(not dynamic_cast<const local_remote_operator*>(op_.get()));
  }

  auto to_string() const -> std::string override {
    return fmt::format("{} {}", name_, op_->to_string());
  }

  auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> override {
    return op_->predicate_pushdown(expr);
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    if (allow_unsafe_pipelines_
        || op_->location() == operator_location::anywhere
        || op_->location() == location_)
      return op_->instantiate(std::move(input), ctrl);
    return caf::make_error(ec::invalid_configuration,
                           "operator location overrides must be explicitly "
                           "allowed by setting "
                           "'vast.allow-unsafe-pipelines' to 'true'");
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<local_remote_operator>(
      op_->copy(), name_, location_, allow_unsafe_pipelines_);
  };

  auto location() const -> operator_location override {
    return location_;
  }

  auto detached() const -> bool override {
    return op_->detached();
  }

  auto infer_type_impl(operator_type input) const
    -> caf::expected<operator_type> override {
    return op_->infer_type(input);
  }

private:
  operator_ptr op_;
  std::string name_;
  operator_location location_;
  bool allow_unsafe_pipelines_;
};

template <detail::string_literal Name, operator_location Location>
class plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  const record& global_config) -> caf::error override {
    auto allow_unsafe_pipelines = try_get_or(
      global_config, "vast.allow-unsafe-pipelines", allow_unsafe_pipelines_);
    if (not allow_unsafe_pipelines) {
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("failed to parse vast.allow-unsafe-pipelines option: {}",
                    allow_unsafe_pipelines.error()));
    }
    allow_unsafe_pipelines_ = *allow_unsafe_pipelines;
    return {};
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    using namespace parser_literals;
    auto p = parsers::optional_ws_or_comment >> parsers::plugin_name;
    auto plugin_name = std::string{};
    if (!p(f, l, plugin_name)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "{} operator: '{}'",
                                                      Name.str(), pipeline)),
      };
    }
    const auto* plugin = plugins::find<operator_plugin>(plugin_name);
    if (!plugin) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator: operator "
                                    "'{}' does not exist",
                                    Name.str(), plugin_name)),
      };
    }
    auto result = plugin->make_operator({f, l});
    if (!result.second) {
      return result;
    }
    if (auto* pipe = dynamic_cast<class pipeline*>(result.second->get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<local_remote_operator>(std::move(op),
                                                     std::string{Name.str()},
                                                     Location,
                                                     allow_unsafe_pipelines_);
      }
      return {
        result.first,
        std::make_unique<class pipeline>(std::move(ops)),
      };
    }
    return {
      result.first,
      std::make_unique<local_remote_operator>(std::move(*result.second),
                                              std::string{Name.str()}, Location,
                                              allow_unsafe_pipelines_),
    };
  }

private:
  bool allow_unsafe_pipelines_ = false;
};

using local_plugin = plugin<"local", operator_location::local>;
using remote_plugin = plugin<"remote", operator_location::remote>;

} // namespace

} // namespace vast::plugins::local_remote

VAST_REGISTER_PLUGIN(vast::plugins::local_remote::local_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::local_remote::remote_plugin)
