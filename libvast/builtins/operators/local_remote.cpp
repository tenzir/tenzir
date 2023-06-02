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
#include <vast/tql/parser_interface.hpp>

#include <arrow/type.h>

namespace vast::plugins::local_remote {

namespace {

class local_remote_operator final : public operator_base {
public:
  local_remote_operator() = default;

  explicit local_remote_operator(operator_ptr op, operator_location location,
                                 bool allow_unsafe_pipelines)
    : op_{std::move(op)},
      location_{location},
      allow_unsafe_pipelines_{allow_unsafe_pipelines} {
    if (auto* op = dynamic_cast<local_remote_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    VAST_ASSERT(not dynamic_cast<const local_remote_operator*>(op_.get()));
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
    return std::make_unique<local_remote_operator>(op_->copy(), location_,
                                                   allow_unsafe_pipelines_);
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

  auto name() const -> std::string override {
    return "<local_remote>";
  }

  friend auto inspect(auto& f, local_remote_operator& x) -> bool {
    return plugin_inspect(f, x.op_) && f.apply(x.location_)
           && f.apply(x.allow_unsafe_pipelines_);
  }

private:
  operator_ptr op_;
  operator_location location_;
  bool allow_unsafe_pipelines_;
};

template <detail::string_literal Name, operator_location Location>
class plugin final : public virtual operator_parser_plugin {
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

  auto parse_operator(tql::parser_interface& p) const -> operator_ptr override {
    auto op_name = p.accept_identifier();
    if (!op_name) {
      diagnostic::error("expected operator name")
        .primary(p.current_span())
        .throw_();
    }
    const auto* plugin = plugins::find<operator_parser_plugin>(op_name->name);
    if (!plugin) {
      diagnostic::error("operator `{}` does not exist", op_name->name)
        .primary(op_name->source)
        .throw_();
    }
    auto result = plugin->parse_operator(p);
    if (auto* pipe = dynamic_cast<pipeline*>(result.get())) {
      auto ops = std::move(*pipe).unwrap();
      for (auto& op : ops) {
        op = std::make_unique<local_remote_operator>(std::move(op), Location,
                                                     allow_unsafe_pipelines_);
      }
      return std::make_unique<pipeline>(std::move(ops));
    }
    return std::make_unique<local_remote_operator>(std::move(result), Location,
                                                   allow_unsafe_pipelines_);
  }

private:
  bool allow_unsafe_pipelines_ = false;
};

using local_plugin = plugin<"local", operator_location::local>;
using remote_plugin = plugin<"remote", operator_location::remote>;
using serialization_plugin = operator_inspection_plugin<local_remote_operator>;

} // namespace

} // namespace vast::plugins::local_remote

VAST_REGISTER_PLUGIN(vast::plugins::local_remote::local_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::local_remote::remote_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::local_remote::serialization_plugin)
