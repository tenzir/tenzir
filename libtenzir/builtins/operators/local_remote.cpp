//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::local_remote {

namespace {

class local_remote_operator final : public operator_base {
public:
  local_remote_operator() = default;

  explicit local_remote_operator(operator_ptr op, operator_location location)
    : op_{std::move(op)}, location_{location} {
    if (auto* op = dynamic_cast<local_remote_operator*>(op_.get())) {
      op_ = std::move(op->op_);
    }
    TENZIR_ASSERT(not dynamic_cast<const local_remote_operator*>(op_.get()));
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    auto result = op_->optimize(filter, order);
    if (result.replacement) {
      result.replacement = std::make_unique<local_remote_operator>(
        std::move(result.replacement), location_);
    }
    return result;
  }

  auto instantiate(operator_input input, operator_control_plane& ctrl) const
    -> caf::expected<operator_output> override {
    if (ctrl.allow_unsafe_pipelines()
        || op_->location() == operator_location::anywhere
        || op_->location() == location_)
      return op_->instantiate(std::move(input), ctrl);
    return caf::make_error(ec::invalid_configuration,
                           "operator location overrides must be explicitly "
                           "allowed by setting "
                           "'tenzir.allow-unsafe-pipelines' to 'true'");
  }

  auto copy() const -> operator_ptr override {
    return std::make_unique<local_remote_operator>(op_->copy(), location_);
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
    return plugin_inspect(f, x.op_) && f.apply(x.location_);
  }

private:
  operator_ptr op_;
  operator_location location_;
};

template <detail::string_literal Name, operator_location Location>
class plugin final : public virtual operator_parser_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  const record& global_config) -> caf::error override {
    auto allow_unsafe_pipelines
      = try_get_or(global_config, "tenzir.allow-unsafe-pipelines", false);
    if (not allow_unsafe_pipelines) {
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("failed to parse tenzir.allow-unsafe-pipelines option: {}",
                    allow_unsafe_pipelines.error()));
    }
    return {};
  }

  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
      .sink = true,
    };
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  };

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
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
        op = std::make_unique<local_remote_operator>(std::move(op), Location);
      }
      return std::make_unique<pipeline>(std::move(ops));
    }
    return std::make_unique<local_remote_operator>(std::move(result), Location);
  }
};

using local_plugin = plugin<"local", operator_location::local>;
using remote_plugin = plugin<"remote", operator_location::remote>;
using serialization_plugin = operator_inspection_plugin<local_remote_operator>;

} // namespace

} // namespace tenzir::plugins::local_remote

TENZIR_REGISTER_PLUGIN(tenzir::plugins::local_remote::local_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::local_remote::remote_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::local_remote::serialization_plugin)
