//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::plugins::otherwise {

namespace {

struct OtherwiseArgs {
  nova::ValueArgument primary;
  nova::ValueArgument fallback;
};

class OtherwiseFunction final {
public:
  static auto eval(OtherwiseArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return match(
      args.primary.data,
      [&](nova::Array<nova::Null> const&) -> nova::Array<nova::Data> {
        return args.fallback.data;
      },
      [&]<nova::data_type Tag>(
        nova::Array<Tag> const&) -> nova::Array<nova::Data>
        requires(not std::same_as<Tag, nova::Null>)
      {
        return args.primary.data;
      },
      [&](nova::UnionArray const& primary) -> nova::Array<nova::Data> {
        auto fallback_rows
          = frame.mask() & primary.alternative_mask<nova::Null>();
        if (not fallback_rows.any()) {
          return args.primary.data;
        }
        auto primary_rows = frame.mask().and_not(fallback_rows);
        if (not primary_rows.any()) {
          return args.fallback.data;
        }
        return nova::with_merged({args.primary.data, std::move(primary_rows)},
                                 {args.fallback.data,
                                  std::move(fallback_rows)});
      });
  }
};

class otherwise final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "otherwise";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<OtherwiseArgs, OtherwiseFunction>{};
    d.positional("primary", &OtherwiseArgs::primary, "any");
    d.positional("fallback", &OtherwiseArgs::fallback, "any");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto primary = ast::expression{};
    auto fallback = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("primary", primary, "any")
          .positional("fallback", fallback, "any")
          .parse(inv, ctx));
    return function_use::make([primary = std::move(primary),
                               fallback = std::move(fallback),
                               loc = inv.call.get_location()](
                                evaluator eval, session ctx) -> multi_series {
      TENZIR_UNUSED(ctx);
      const auto ps = eval(primary);
      const auto fs = eval(fallback);
      return map_series(ps, fs, [](series ps, series fs) -> multi_series {
        TENZIR_ASSERT(ps.length() == fs.length());
        if (ps.type.kind().is<null_type>()) {
          return fs;
        }
        if (fs.type.kind().is<null_type>()) {
          return ps;
        }
        if (ps.type == fs.type) {
          // In the "easy" case, both have the same type, so we never split.
          auto b = ps.type.make_arrow_builder(arrow_memory_pool());
          check(b->Reserve(ps.array->length()));
          for (auto offset = int64_t{}; offset != ps.array->length();) {
            auto count = int64_t{1};
            const auto valid = ps.array->IsValid(offset);
            while (offset + count != ps.array->length()
                   and ps.array->IsValid(offset + count) == valid) {
              ++count;
            }
            check(append_array_slice(*b, ps.type, valid ? *ps.array : *fs.array,
                                     offset, count));
            offset += count;
          }
          return series{ps.type, finish(*b)};
        }
        // Otherwise, we split the series whenever the choice changes.
        auto length = ps.length();
        TENZIR_ASSERT(length > 0);
        auto parts = std::vector<series>{};
        auto begin = int64_t{0};
        auto current_valid = ps.array->IsValid(0);
        // We add an artificial index at the end that always causes a split.
        for (auto i = int64_t{0}; i < length + 1; ++i) {
          auto valid = std::invoke([&] {
            if (i < length) {
              return ps.array->IsValid(i);
            }
            return not current_valid;
          });
          if (current_valid != valid) {
            if (current_valid) {
              parts.push_back(ps.slice(begin, i));
            } else {
              parts.push_back(fs.slice(begin, i));
            }
            current_valid = valid;
            begin = i;
          }
        }
        return multi_series{std::move(parts)};
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::otherwise

TENZIR_REGISTER_PLUGIN(tenzir::plugins::otherwise::otherwise)
