//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
#include <tenzir/arrow_utils.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/builder.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

// Include our compatibility header for Boost < 1.86
#include <boost/version.hpp>
#if BOOST_VERSION < 108600 || defined(TENZIR_FORCE_BOOST_UUID_COMPAT)
#  include <tenzir/detail/boost_uuid_generators.hpp>
#endif

#include "tenzir/option.hpp"

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

struct UuidArgs {
  Option<located<std::string>> version;
  uuid_version parsed = uuid_version::v4;
};

struct UuidFunction {
  auto eval(UuidArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    auto generate = [&](auto generator) -> nova::Array<nova::Data> {
      auto builder = nova::ArrayBuilder<nova::String>{};
      nova::storage::for_each_true(frame.mask(), [&](auto row) {
        builder.skip_n(row - builder.length());
        builder.data(boost::uuids::to_string(generator()));
      });
      builder.skip_n(frame.length() - builder.length());
      return builder.finish();
    };
    switch (args.parsed) {
      case uuid_version::nil:
        return generate(boost::uuids::nil_generator{});
      case uuid_version::v1:
        return generate(boost::uuids::time_generator_v1{});
      case uuid_version::v4:
        return generate(boost::uuids::random_generator{});
      case uuid_version::v6:
        return generate(boost::uuids::time_generator_v6{});
      case uuid_version::v7:
        return generate(boost::uuids::time_generator_v7{});
    }
    TENZIR_UNREACHABLE();
  }
};

class uuid final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<UuidArgs, UuidFunction>{};
    d.named("version", &UuidArgs::version);
    d.validate([](UuidArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      if (not args.version) {
        return {};
      }
      auto parsed = from_string<uuid_version>(args.version->inner);
      if (not parsed) {
        diagnostic::error("unsupported UUID version: `{}`", args.version->inner)
          .primary(*args.version)
          .hint("supported versions: `v1`, `v4`, `v6`, `v7`, `nil`")
          .emit(dh);
        return failure::promise();
      }
      args.parsed = *parsed;
      return {};
    });
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "uuid";
  }

  auto is_deterministic() const -> bool override {
    return false;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto version_opt = Option<located<std::string>>{};
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
