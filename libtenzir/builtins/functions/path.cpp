//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::path {

namespace {

struct PathArgs {
  nova::ValueArgument path;
  location call;
};

// TODO: We don't know whether this is a Windows or POSIX path, and string
// might not be a good type for paths because of invalid UTF-8.
auto file_name_of(std::string_view path) -> std::string_view {
  auto pos = path.find_last_of("/\\");
  if (pos == std::string_view::npos) {
    return path;
  }
  return path.substr(pos + 1);
}

auto parent_dir_of(std::string_view path) -> std::string_view {
  auto pos = path.find_last_of("/\\");
  if (pos == std::string_view::npos) {
    return path;
  }
  return path.substr(0, pos);
}

template <bool FileName>
struct PathFunction {
  static auto eval(PathArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::apply_kernel<1>(
      frame, FileName ? "file_name" : "parent_dir", {args.path}, args.call,
      detail::overload{
        [](diagnostic_handler&, nova::Null) -> Option<std::string_view> {
          return None{};
        },
        [](diagnostic_handler&,
           std::string_view path) -> Option<std::string_view> {
          return FileName ? file_name_of(path) : parent_dir_of(path);
        },
      });
  }
};

template <bool FileName>
auto describe_path_function() -> nova::FunctionDescription {
  auto d = nova::FunctionDescriber<PathArgs, PathFunction<FileName>>{};
  d.positional("path", &PathArgs::path, "string");
  d.call_location(&PathArgs::call);
  return std::move(d).finish();
}

class file_name final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    return describe_path_function<true>();
  }

  auto name() const -> std::string override {
    return "file_name";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("file_name")
          .positional("path", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::StringBuilder{};
        check(b.Reserve(eval.length()));
        for (auto& arg : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::NullArray& arg) {
              check(b.AppendNulls(arg.length()));
            },
            [&](const arrow::StringArray& arg) {
              for (auto row = int64_t{0}; row < arg.length(); ++row) {
                if (arg.IsNull(row)) {
                  check(b.AppendNull());
                  continue;
                }
                auto path = arg.GetView(row);
                // TODO: We don't know whether this is a windows/posix path.
                // TODO: Also, btw, string might not be a good type for paths.
                auto pos = path.find_last_of("/\\");
                // TODO: Trailing sep.
                if (pos == std::string::npos) {
                  check(b.Append(path));
                } else {
                  check(b.Append(path.substr(pos + 1)));
                }
              }
            },
            [&](const auto&) {
              diagnostic::warning("`file_name` expected `string`, but got `{}`",
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(arg.length()));
            },
          };
          match(*arg.array, f);
        }
        return series{string_type{}, finish(b)};
      });
  }
};

class parent_dir final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    return describe_path_function<false>();
  }

  auto name() const -> std::string override {
    return "parent_dir";
  }

  auto is_deterministic() const -> bool final {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("file_name")
          .positional("path", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::StringBuilder{};
        check(b.Reserve(eval.length()));
        for (auto& arg : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::NullArray& arg) {
              check(b.AppendNulls(arg.length()));
            },
            [&](const arrow::StringArray& arg) {
              for (auto row = int64_t{0}; row < arg.length(); ++row) {
                if (arg.IsNull(row)) {
                  check(b.AppendNull());
                  continue;
                }
                auto path = arg.GetView(row);
                // TODO: We don't know whether this is a windows/posix path.
                // TODO: Also, string might not be a good type for paths because
                // of invalid UTF-8.
                auto pos = path.find_last_of("/\\");
                // TODO: Trailing separator.
                if (pos == std::string::npos) {
                  // TODO: What should we do here?
                  check(b.Append(path));
                  continue;
                }
                check(b.Append(path.substr(0, pos)));
              }
            },
            [&](const auto&) {
              diagnostic::warning("`file_name` expected `string`, but got `{}`",
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(arg.length()));
            },
          };
          match(*arg.array, f);
        }
        return series{string_type{}, finish(b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::path

TENZIR_REGISTER_PLUGIN(tenzir::plugins::path::file_name)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::path::parent_dir)
