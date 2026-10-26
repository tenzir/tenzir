//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/startup_argument.hpp>
#include <tenzir/try.hpp>

#include <librdkafka/rdkafkacpp.h>

namespace tenzir::plugins::kafka {

/// Validates that Kafka options map strings to number/string/secret values.
inline auto validate_options(located<nova::Record> const& options,
                             diagnostic_handler& dh) -> failure_or<void> {
  for (auto const& [key, value] : options.inner) {
    auto valid = match(value, []<class T>(T const&) {
      return concepts::one_of<T, nova::Bool, nova::Int, nova::UInt, nova::Float,
                              nova::String, nova::Secret>;
    });
    if (not valid) {
      auto kind = match(value, []<class T>(T const&) {
        return nova::Type<T>::static_name;
      });
      diagnostic::error(
        "options must be a record `{{ string: number|string }}`")
        .primary(options.source, "key `{}` is `{}`", key, kind)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

/// Checks that explicit SASL settings are compatible with AWS IAM auth.
inline auto check_sasl_mechanism(located<nova::Record> const& options,
                                 diagnostic_handler& dh) -> failure_or<void> {
  for (auto key : {"sasl.mechanism", "sasl.mechanisms"}) {
    auto it = options.inner.find(key);
    if (it == options.inner.end()) {
      continue;
    }
    auto const* mechanism = try_as<nova::String>(&it->second);
    if (not mechanism) {
      diagnostic::error("option `{}` must be `string`", key)
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
    if (*mechanism != "OAUTHBEARER") {
      diagnostic::error("conflicting `{}`: `{}` but `aws_iam` "
                        "requires `OAUTHBEARER`",
                        key, *mechanism)
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

/// Evaluates the `options` argument and validates it. Returns an empty record
/// if the argument is absent. With `aws_iam`, explicit SASL settings must be
/// compatible with IAM auth.
inline auto
evaluate_options(Option<StartupArgument<nova::Record>> const& options,
                 bool aws_iam, OpCtx& ctx)
  -> Task<failure_or<located<nova::Record>>> {
  if (not options) {
    co_return located{nova::Record{}, location::unknown};
  }
  CO_TRY(auto evaluated, co_await options->evaluate(ctx));
  auto result = located{std::move(evaluated), options->get_location()};
  CO_TRY(validate_options(result, ctx.dh()));
  if (aws_iam) {
    CO_TRY(check_sasl_mechanism(result, ctx.dh()));
  }
  co_return result;
}

/// Returns a parser for symbolic and absolute Kafka offset expressions.
inline auto offset_parser() {
  using namespace parsers;
  using namespace parser_literals;
  auto beginning = "beginning"_p->*[] {
    return RdKafka::Topic::OFFSET_BEGINNING;
  };
  auto end = "end"_p->*[] {
    return RdKafka::Topic::OFFSET_END;
  };
  auto stored = "stored"_p->*[] {
    return RdKafka::Topic::OFFSET_STORED;
  };
  auto value = i64->*[](int64_t x) {
    return x >= 0 ? x : RdKafka::Consumer::OffsetTail(-x);
  };
  return beginning | end | stored | value;
}

} // namespace tenzir::plugins::kafka
