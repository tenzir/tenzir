//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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

#include <librdkafka/rdkafkacpp.h>

namespace tenzir::plugins::kafka {

/// Validates that Kafka options map strings to number/string/secret values.
inline auto validate_options(located<record> const& options,
                             diagnostic_handler& dh) -> failure_or<void> {
  for (auto const& [key, value] : options.inner) {
    auto validate = tenzir::detail::overload{
      [](concepts::arithmetic auto const&) -> failure_or<void> {
        return {};
      },
      [](std::string const&) -> failure_or<void> {
        return {};
      },
      [](secret const&) -> failure_or<void> {
        return {};
      },
      [](tenzir::pattern const&) -> failure_or<void> {
        TENZIR_UNREACHABLE();
      },
      [&]<typename T>(T const&) -> failure_or<void> {
        diagnostic::error(
          "options must be a record `{{ string: number|string }}`")
          .primary(options.source, "key `{}` is `{}`", key,
                   type_kind{tag_v<data_to_type_t<T>>})
          .emit(dh);
        return failure::promise();
      },
    };
    TRY(match(value, validate));
  }
  return {};
}

/// Checks that explicit SASL settings are compatible with AWS IAM auth.
inline auto check_sasl_mechanism(located<record> const& options,
                                 diagnostic_handler& dh) -> failure_or<void> {
  if (auto it = options.inner.find("sasl.mechanism");
      it != options.inner.end()) {
    auto mechanism = try_as<std::string>(it->second);
    if (not mechanism) {
      diagnostic::error("option `sasl.mechanism` must be `string`")
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
    if (*mechanism != "OAUTHBEARER") {
      diagnostic::error("conflicting `sasl.mechanism`: `{}` but `aws_iam` "
                        "requires `OAUTHBEARER`",
                        *mechanism)
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
  }
  if (auto it = options.inner.find("sasl.mechanisms");
      it != options.inner.end()) {
    auto mechanisms = try_as<std::string>(it->second);
    if (not mechanisms) {
      diagnostic::error("option `sasl.mechanisms` must be `string`")
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
    if (*mechanisms != "OAUTHBEARER") {
      diagnostic::error("conflicting `sasl.mechanisms`: `{}` but `aws_iam` "
                        "requires `OAUTHBEARER`",
                        *mechanisms)
        .primary(options)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
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
