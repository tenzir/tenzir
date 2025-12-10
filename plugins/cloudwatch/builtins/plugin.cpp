//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <aws/core/Aws.h>

#include "operator.hpp"

using namespace std::chrono_literals;

namespace tenzir::plugins::cloudwatch {

namespace {

class plugin final : public virtual operator_plugin2<from_cloudwatch_operator> {
public:
  auto make(operator_factory_plugin::invocation inv,
            session ctx) const -> failure_or<operator_ptr> override {
    auto args = connector_args{};
    TRY(argument_parser2::operator_(this->name())
          .positional("log_group", args.log_group)
          .named("filter", args.filter_pattern)
          .named("live", args.live)
          .parse(inv, ctx));

    if (args.log_group.inner.empty()) {
      diagnostic::error("log_group must not be empty")
        .primary(args.log_group.source)
        .hint("provide a CloudWatch log group name or ARN")
        .emit(ctx);
      return failure::promise();
    }

    return std::make_unique<from_cloudwatch_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::cloudwatch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cloudwatch::plugin)
