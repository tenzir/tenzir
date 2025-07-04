//    _   _____   __________
//   | | / / _ | / __/_  __/    Visibility
//   | |/ / __ |_\ \  / /        Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
#include <tenzir/arrow_utils.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/builder.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <optional>
#include <string>
#include <variant>

namespace tenzir::plugins::uuid {

TENZIR_ENUM(uuid_version, nil, v1, v4, v6, v7);

namespace {

using uuid_generator_type = std::variant<boost::uuids::nil_generator,     // nil
                                         boost::uuids::time_generator_v1, // v1
                                         boost::uuids::random_generator,  // v4
                                         boost::uuids::time_generator_v6, // v6
                                         boost::uuids::time_generator_v7  // v7
                                         >;

class uuid final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.uuid";
  }

  auto is_deterministic() const -> bool override {
    return false;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto version_opt = std::optional<located<std::string>>{};
    TRY(argument_parser2::function("uuid")
          .named("version", version_opt)
          .parse(inv, ctx));
    auto version = std::string{"v4"};
    if (version_opt) {
      version = version_opt->inner;
    }
    auto opt = from_string<uuid_version>(version);
    if (not opt) {
      diagnostic::error("unsupported UUID version: `{}`", version)
        .primary(*version_opt)
        .hint("supported versions: `v1`, `v4`, `v6`, `v7`, `nil`")
        .emit(ctx);
      return failure::promise();
    }
    uuid_generator_type generator;
    switch (*opt) {
      case uuid_version::nil: {
        // default-constructed state;
        break;
      }
      case uuid_version::v1: {
        generator = boost::uuids::time_generator_v1{};
        break;
      }
      case uuid_version::v4: {
        generator = boost::uuids::random_generator{};
        break;
      }
      case uuid_version::v6: {
        generator = boost::uuids::time_generator_v6{};
        break;
      }
      case uuid_version::v7: {
        generator = boost::uuids::time_generator_v7{};
        break;
      }
    }
    return function_use::make(
      [gen = std::move(generator)](evaluator eval, session) mutable -> series {
        auto b = arrow::StringBuilder{};
        check(b.Reserve(eval.length()));
        auto generate_and_append = [&](auto& concrete_generator) {
          for (int64_t i = 0; i < eval.length(); ++i) {
            auto u = concrete_generator();
            check(b.Append(boost::uuids::to_string(u)));
          }
        };
        std::visit(generate_and_append, gen);
        return {string_type{}, finish(b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::uuid

TENZIR_REGISTER_PLUGIN(tenzir::plugins::uuid::uuid)
