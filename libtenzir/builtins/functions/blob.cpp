//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::blob {

namespace {

struct BlobArgs {
  nova::ValueArgument x;
  location call;
};

struct BlobFunction {
  static auto eval(BlobArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    if (args.x.data.try_as<nova::Blob>()) {
      return args.x.data;
    }
    return nova::apply_kernel<1>(
      frame, "blob", {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, nova::Null) -> Option<blob_view> {
          return None{};
        },
        [](diagnostic_handler&, blob_view value) -> Option<blob_view> {
          return value;
        },
        [](diagnostic_handler&, std::string_view value) -> Option<blob_view> {
          return blob_view{as_bytes(value)};
        }});
  }
};

class blob final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<BlobArgs, BlobFunction>{};
    d.positional("x", &BlobArgs::x, "blob|string");
    d.call_location(&BlobArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "blob";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "blob|string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> multi_series {
        return map_series(eval(expr), [&](series value) -> series {
          return match(
            *value.array,
            [](arrow::NullArray const& array) -> series {
              return series::null(blob_type{}, array.length());
            },
            [](arrow::BinaryArray const& array) -> series {
              return series{blob_type{},
                            std::make_shared<arrow::BinaryArray>(array.data())};
            },
            [](arrow::StringArray const& array) -> series {
              // UTF-8 and binary arrays share their offsets and value buffers.
              auto data = array.data()->Copy();
              data->type = blob_type{}.to_arrow_type();
              return series{blob_type{}, arrow::MakeArray(std::move(data))};
            },
            [&](auto const&) -> series {
              diagnostic::warning("expected `string` or `blob`, got `{}`",
                                  value.type.kind())
                .primary(expr)
                .emit(ctx);
              return series::null(blob_type{}, value.length());
            });
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::blob

TENZIR_REGISTER_PLUGIN(tenzir::plugins::blob::blob)
