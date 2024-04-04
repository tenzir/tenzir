//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/exec.hpp"

#include "tenzir/bitmap.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/check_type.hpp"
#include "tenzir/tql2/context.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/tql2/tokens.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api.h>
#include <arrow/util/utf8.h>

#include <chrono>
#include <ranges>

namespace tenzir::tql2 {

namespace {

using namespace ast;

/// A diagnostic handler that remembers when it emits an error.
class diagnostic_handler_wrapper final : public diagnostic_handler {
public:
  explicit diagnostic_handler_wrapper(std::unique_ptr<diagnostic_handler> inner)
    : inner_{std::move(inner)} {
  }

  void emit(diagnostic d) override {
    if (d.severity == severity::error) {
      error_ = true;
    }
    inner_->emit(std::move(d));
  }

  auto error() const -> bool {
    return error_;
  }

  auto inner() && -> std::unique_ptr<diagnostic_handler> {
    return std::move(inner_);
  }

private:
  bool error_ = false;
  std::unique_ptr<diagnostic_handler> inner_;
};

#if 0
TENZIR_ENUM(sort_direction, asc, desc);

struct sort_expr {
  sort_expr(expression expr, sort_direction dir)
    : expr{std::move(expr)}, dir{dir} {
  }

  expression expr;
  sort_direction dir;

  friend auto inspect(auto& f, sort_expr& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr), f.field("dir", x.dir));
  }
};

class sort_use final : public operator_use {
public:
  explicit sort_use(entity self, std::vector<sort_expr> exprs)
    : self_{std::move(self)}, exprs_{std::move(exprs)} {
  }

  // void, byte, chunk, event
  // void, chunk_ptr, vector<chunk_ptr>, table_slice

  auto debug(debug_writer& f) -> bool override {
    return f.object(*this).fields(f.field("self", self_),
                                  f.field("exprs", exprs_));
  }

private:
  entity self_;
  std::vector<sort_expr> exprs_;
};

class sort_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<expression> args, context& ctx) const
    -> std::unique_ptr<operator_use> override {
    auto exprs = std::vector<sort_expr>{};
    exprs.reserve(args.size());
    for (auto& arg : args) {
      arg.match(
        [&](tql2::ast::unary_expr& un_expr) {
          if (un_expr.op.inner == tql2::ast::unary_op::neg) {
            exprs.emplace_back(std::move(un_expr.expr), sort_direction::desc);
          } else {
            exprs.emplace_back(std::move(arg), sort_direction::asc);
          }
        },
        [&](assignment& x) {
          diagnostic::error("we don't like assignments around here")
            .primary(x.get_location())
            .emit(ctx.dh());
        },
        [&](auto&) {
          exprs.emplace_back(std::move(arg), sort_direction::asc);
        });
    }
    for (auto& expr : exprs) {
      type_checker{ctx}.visit(expr.expr);
    }
    return std::make_unique<sort_use>(std::move(self), std::move(exprs));
  }
};

class plugin_operator_def : public operator_def {
public:
  explicit plugin_operator_def(const operator_factory_plugin* plugin)
    : plugin_{plugin} {
  }

  auto make(ast::entity self, std::vector<expression> args, context& ctx) const
    -> std::unique_ptr<operator_use> override {
    TENZIR_UNUSED(args);
    diagnostic::error("todo: plugin_operator_def")
      .primary(self.get_location())
      .emit(ctx);
    return nullptr;
  }

private:
  const operator_factory_plugin* plugin_;
};

class from_use final : public operator_use {
public:
  explicit from_use(located<std::string> url) {
    TENZIR_UNUSED(url);
  }
};

class from_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    (void)self;
    if (args.size() != 1) {
      diagnostic::error("`from` expects exactly one argument")
        .primary(args.empty() ? self.get_location() : args[1].get_location())
        .emit(ctx.dh());
      if (args.empty()) {
        return nullptr;
      }
    }
    auto arg = std::move(args[0]);
    // TODO: We don't need this, do we?
    // auto ty = type_checker{ctx}.visit(arg);
    // if (ty && ty != string_type{}) {
    //   diagnostic::error("expected `string` but got `{}`", *ty)
    //     .primary(arg.get_location())
    //     .emit(ctx.dh());
    //   return nullptr;
    // }
    auto url_data = evaluate(arg, ctx);
    if (not url_data) {
      return nullptr;
    }
    auto url = caf::get_if<std::string>(&*url_data);
    if (not url) {
      diagnostic::error("expected a string")
        .primary(arg.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    return std::make_unique<from_use>(
      located{std::move(*url), arg.get_location()});
  }
};

class load_file_use final : public operator_use {
public:
  explicit load_file_use(std::optional<ast::expression> path)
    : path_{std::move(path)} {
  }

private:
  std::optional<ast::expression> path_;
};

class load_file_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    (void)self;
    (void)args;
    auto usage = "load_file path, follow=false, mmap=false, timeout=null";
    auto docs = "https://docs.tenzir.com/operators/load_file";
    if (args.empty()) {
      diagnostic::error("expected at least one argument")
        .primary(self.get_location())
        .usage(usage)
        .docs(docs)
        .emit(ctx.dh());
    }
    // assume we want `"foo.json"` and `path="foo.json"`.
    auto path = std::optional<ast::expression>{};
    for (auto& arg : args) {
      arg.match(
        [&](assignment& x) {
          auto ty = type_checker{ctx}.visit(x.right);
          if (not x.left.this_ && x.left.path.size() == 1
              && x.left.path[0].name == "path") {
            if (ty && ty != type{string_type{}}) {
              diagnostic::error("expected `string` but got `{}`", *ty)
                .primary(x.right.get_location())
                .usage(usage)
                .docs(docs)
                .emit(ctx.dh());
            }
            path = std::move(x.right);
          } else {
            diagnostic::error("unknown named argument")
              .primary(x.left.get_location())
              .usage(usage)
              .docs(docs)
              .emit(ctx.dh());
          }
        },
        [&](auto& x) {
          auto ty = type_checker{ctx}.visit(x);
          if (path) {
            diagnostic::error("path was already specified")
              .secondary(path->get_location(), "previous definition")
              .primary(x.get_location(), "new definition")
              .usage(usage)
              .docs(docs)
              .emit(ctx.dh());
            return;
          }
          if (ty && ty != type{string_type{}}) {
            diagnostic::error("expected `string`, got `{}`", *ty)
              .primary(x.get_location())
              .usage(usage)
              .docs(docs)
              .emit(ctx.dh());
          }
          path = std::move(x);
        });
    };
    return std::make_unique<load_file_use>(std::move(path));
  }
};

class drop_use final : public operator_use {
public:
  explicit drop_use(std::vector<selector> selectors)
    : selectors_{std::move(selectors)} {
  }

  auto debug(debug_writer& f) -> bool override {
    return f.apply(selectors_);
  }

private:
  std::vector<selector> selectors_;
};

class drop_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    (void)self;
    auto selectors = std::vector<selector>{};
    for (auto& arg : args) {
      arg.match(
        [&](selector& x) {
          selectors.push_back(std::move(x));
        },
        [&](auto& x) {
          diagnostic::error("expected selector")
            .primary(x.get_location())
            .emit(ctx.dh());
        });
    }
    return std::make_unique<drop_use>(std::move(selectors));
  }
};

struct pipeline_use {
  pipeline_use() = default;

  explicit pipeline_use(std::vector<std::unique_ptr<operator_use>> ops)
    : ops{std::move(ops)} {
  }

  std::vector<std::unique_ptr<operator_use>> ops;

  friend auto inspect(auto& f, pipeline_use& x) -> bool {
    if (not f.begin_sequence(x.ops.size())) {
      return false;
    }
    for (auto& op : x.ops) {
      if (not f.apply(*op)) {
        return false;
      }
    }
    return f.end_sequence();
  }
};

class if_use final : public operator_use {
public:
  explicit if_use(expression condition, pipeline_use then,
                  std::optional<pipeline_use> else_)
    : condition_{std::move(condition)},
      then_{std::move(then)},
      else_{std::move(else_)} {
  }

private:
  expression condition_;
  pipeline_use then_;
  std::optional<pipeline_use> else_;
};

class head_use final : public operator_use {
public:
  explicit head_use(uint64_t count) : count_{count} {
  }

private:
  uint64_t count_;
};

class head_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    if (args.empty()) {
      diagnostic::error("expected number")
        .primary(self.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    if (args.size() > 1) {
      diagnostic::error("expected only one argument")
        .primary(args[1].get_location())
        .emit(ctx.dh());
    }
    // TODO: We want to have better errors here.
    auto value = evaluate(args[0], ctx);
    if (not value) {
      return nullptr;
    }
    // TODO: Better promotion?
    auto count = caf::get_if<int64_t>(&*value);
    if (not count) {
      diagnostic::error("expected integer")
        .primary(args[0].get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    if (*count < 0) {
      diagnostic::error("expected count to be non-negative but got {}", *count)
        .primary(args[0].get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    return std::make_unique<head_use>(detail::narrow<uint64_t>(*count));
  }
};

class group_use final : public operator_use {
public:
};

class group_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    if (args.empty()) {
      diagnostic::error("expected at least one argument")
        .primary(self.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    for (auto& arg : args) {
      type_checker{ctx}.visit(arg);
    }
    // evaluate(args.back(), ctx);
    diagnostic::error("not implemented yet")
      .primary(self.get_location())
      .emit(ctx.dh());
    return std::make_unique<group_use>();
  }
};

class where_use_test final : public operator_base {
public:
  where_use_test() = default;

  auto name() const -> std::string override {
    return "where2";
  }
};

class where_use final : public operator_use {
public:
  // TODO: Better solution?
  where_use() = default;

  explicit where_use(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto name() const -> std::string {
    return "where2";
  }

  friend auto inspect(auto& f, where_use& x) -> bool {
    return f.apply(x.expr_);
  }

private:
  ast::expression expr_;
};

class plugin : public virtual serialization_plugin<operator_base> {
public:
  auto serialize(serializer f, const operator_base& x) const -> bool override {
    return std::visit(
      [&](auto& f) -> bool {
        TENZIR_TODO();
      },
      f);
  }

  void deserialize(deserializer f,
                   std::unique_ptr<operator_base>& x) const override {
    x = nullptr;
    std::visit(
      [&](auto& f) {
        auto name = std::string{};
        if (not f.get().apply(name)) {
          return;
        }
        // TODO: Find `operator_def`?
        static_cast<context*>(nullptr);
        auto def = static_cast<operator_def*>(nullptr);
        if (not def) {
          // TODO: error message
          return;
        }
        auto use = def->deserialize(f);
      },
      f);
  }
};

class where_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    if (args.size() != 1) {
      diagnostic::error("expected exactly one argument")
        .primary(self.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    auto ty = type_checker{ctx}.visit(args[0]);
    if (ty && ty->kind().is_not<bool_type>()) {
      diagnostic::error("expected `bool`, got `{}`", *ty)
        .primary(args[0].get_location())
        .emit(ctx.dh());
    }
    return std::make_unique<where_use>(std::move(args[0]));
  }
};
#endif

#if 1
// evaluation model !?!

// data
// arrow::Array
// bitmap (?) <-- probably not as function input?

// maybe input a `ids` for which we care about the output?
// or for which we want to `&&` the result?

// if evaluation "fails" -> use `null` for now (for whole expression??)
// and emit diagnostic

struct eval_input {
  location fn;
  std::span<located<variant<data, std::shared_ptr<arrow::Array>, bitmap>>> args;
};

class my_special_sqrt {
public:
  void signature() {
    // sqrt(double)

    // foo(bar, baz, qux=true)
  }

  auto eval(arrow::DoubleArray& input, context& ctx)
    -> std::shared_ptr<arrow::DoubleArray> {
    auto builder = arrow::DoubleBuilder{};
    auto length = input.length();
    (void)builder.Reserve(length);
    for (auto idx = int64_t{0}; idx < length; ++idx) {
      if (input.IsNull(idx)) {
        builder.UnsafeAppendNull();
        continue;
      }
      auto value = input.Value(idx);
      if (value < 0.0) [[unlikely]] {
        // TODO: We only want to emit the diagnostic once... for this batch?
        // No, for the whole input, but with the given location.
        // Irrespective of schema?
        if (not complained_) {
          diagnostic::warning("tried to take `sqrt` of negative value")
            .primary(fn_)
            .note("happened with `{}`", value)
            .emit(ctx.dh());
          complained_ = true;
        }
        builder.UnsafeAppendNull();
        continue;
      }
      builder.UnsafeAppend(std::sqrt(input.Value(0)));
    }
    auto result = std::shared_ptr<arrow::DoubleArray>();
    (void)builder.Finish(&result);
    return result;
  }

  auto eval_one(std::optional<double> input, context& ctx)
    -> std::optional<double> {
    auto builder = arrow::DoubleBuilder{};
    (void)builder.AppendOrNull(input);
    auto result = std::shared_ptr<arrow::DoubleArray>();
    (void)builder.Finish(&result);
    auto output = eval(*result, ctx);
    TENZIR_ASSERT(output->length() == 1);
    return (*output)[0];
  }

private:
  location fn_;
  bool complained_ = false;
};

auto eval_sqrt(eval_input input, context& ctx) -> bool {
  TENZIR_ASSERT(input.args.size() == 1);
  auto& arg = input.args[0];
  auto constant = std::get_if<data>(&arg.inner);
  if (constant) {
  }
}
#endif

class sqrt_def final : public function_def {
public:
  auto check(check_info info, context& ctx) const
    -> std::optional<type> override {
    auto dbl = type{double_type{}};
    if (info.arg_count() == 0) {
      diagnostic::error("`sqrt` expects one argument")
        .primary(info.fn_loc())
        .emit(ctx.dh());
      return dbl;
    }
    auto& ty = info.arg_type(0);
    if (ty && ty != dbl) {
      // TODO: Use name of function?
      diagnostic::error("`sqrt` expected `{}` but got `{}`", dbl, *ty)
        .primary(info.arg_loc(0), "this is `{}`", *ty)
        .secondary(info.fn_loc(), "this expected `{}`", dbl)
        .emit(ctx.dh());
    }
    if (info.arg_count() > 1) {
      diagnostic::error("`sqrt` expects only one argument")
        .primary(info.arg_loc(1))
        .emit(ctx.dh());
    }
    return dbl;
  }

  // std::vector<located<data | arrow::Array>>{};
  // get("MUST_BE_CONSTANT")

  auto evaluate(location fn, std::vector<located<data>> args,
                context& ctx) const -> std::optional<data> override {
    // TODO: integrate this with `check`?
    (void)fn;
    TENZIR_ASSERT(args.size() == 1);
    auto arg = caf::get_if<double>(&args[0].inner);
    if (not arg) {
      // TODO
      auto kind = caf::visit(detail::overload{
                               []<class T>(const T&) {
                                 return type_kind::of<data_to_type_t<T>>;
                               },
                               [](const pattern&) -> type_kind {
                                 TENZIR_UNREACHABLE();
                               },
                             },
                             args[0].inner);
      diagnostic::error("`sqrt` expected `double` but got `{}`", kind)
        .primary(args[0].source)
        .emit(ctx.dh());
      return std::nullopt;
    }
    // TODO: nan, inf, negative, etc?
    auto val = *arg;
    if (val < 0.0) {
      diagnostic::error("`sqrt` received negative value `{}`", val)
        .primary(args[0].source)
        .emit(ctx.dh());
      return std::nullopt;
    }
    return std::sqrt(*arg);
  }

  auto test(const arrow::DoubleArray& x) const
    -> std::shared_ptr<arrow::DoubleArray> {
    // TODO: This is a test!!
    {
      // TODO: This is UB and useless. Do not copy. Do not even read. Stop!
#if 0
      auto alloc = arrow::AllocateBuffer(x.length() * sizeof(double));
      TENZIR_ASSERT(alloc.ok());
      auto buffer = std::move(*alloc);
      TENZIR_ASSERT(buffer);
      auto target = new (buffer->mutable_data()) double[x.length()];
      auto begin = x.raw_values();
      auto end = begin + x.length();
      auto null_alloc = arrow::AllocateBuffer((x.length() + 7) / 8);
      TENZIR_ASSERT(null_alloc.ok());
      auto null_buffer = std::move(*null_alloc);
      TENZIR_ASSERT(null_buffer);
      auto null_target = null_buffer->mutable_data();
      if (auto nulls = x.null_bitmap()) {
        std::memcpy(null_target, nulls->data(), nulls->size());
      } else {
        std::memset(null_target, 0xFF, (x.length() + 7) / 8);
      }
      while (begin != end) {
        auto val = *begin;
        if (val < 0.0) [[unlikely]] {
          auto mask = 0xFF ^ (0x01 << ((begin - end) % 8));
          null_target[(begin - end) / 8] &= mask;
          // TODO: Emit warning/error?
        }
        *target = std::sqrt(*begin);
        ++begin;
        ++target;
      }
      return std::make_shared<arrow::DoubleArray>(x.length(), std::move(buffer),
                                                  std::move(null_buffer));
#endif
    }
    // TODO
    auto b = arrow::DoubleBuilder{};
    (void)b.Reserve(x.length());
    for (auto y : x) {
      if (not y) {
        // TODO: Warning?
        b.UnsafeAppendNull();
        continue;
      }
      auto z = *y;
      if (z < 0.0) {
        // TODO: Warning?
        b.UnsafeAppendNull();
        continue;
      }
      b.UnsafeAppend(std::sqrt(z));
    }
    auto result = std::shared_ptr<arrow::DoubleArray>{};
    (void)b.Finish(&result);
    return result;
  }
};

class random_def final : public function_def {
public:
  auto check(check_info info, context& ctx) const
    -> std::optional<type> override {
    // TODO
    TENZIR_UNUSED(info, ctx);
    return type{double_type{}};
  }

  auto evaluate(location fn, std::vector<located<data>> args,
                context& ctx) const -> std::optional<data> override {
    // TODO
    TENZIR_UNUSED(fn, args, ctx);
    return 123.45;
  }
};

class now_def final : public function_def {
public:
  auto check(check_info info, context& ctx) const
    -> std::optional<type> override {
    if (info.arg_count() > 0) {
      diagnostic::error("`now` does not expect any arguments")
        .primary(info.arg_loc(0))
        .emit(ctx.dh());
    }
    return type{duration_type{}};
  }

  auto evaluate(location fn, std::vector<located<data>> args,
                context& ctx) const -> std::optional<data> override {
    TENZIR_UNUSED(fn, ctx);
    TENZIR_ASSERT(args.empty());
    return time{time::clock::now()};
  }
};

auto prepare_pipeline(pipeline&& pipe, context& ctx) -> tenzir::pipeline {
  auto ops = std::vector<operator_ptr>{};
  for (auto& stmt : pipe.body) {
    // let_stmt, if_stmt, match_stmt
    stmt.match(
      [&](invocation& x) {
        if (not x.op.ref.resolved()) {
          // This was already reported. We don't know how the operator would
          // interpret its arguments, hence we make no attempt of reporting
          // additional errors for them.
          return;
        }
        // TODO: Where do we check that this succeeds?
        auto def = std::get_if<const tql2::operator_factory_plugin*>(
          &ctx.reg().get(x.op.ref));
        TENZIR_ASSERT(def);
        TENZIR_ASSERT(*def);
        auto op = (*def)->make_operator(x.op, std::move(x.args), ctx);
        if (op) {
          ops.push_back(std::move(op));
        } else {
          // We assume we emitted an error.
        }
      },
      [&](assignment& x) {
        check_assignment(x, ctx);
        auto assignments = std::vector<assignment>();
        assignments.push_back(std::move(x));
        ops.push_back(std::make_unique<set_operator>(std::move(assignments)));
      },
      [&](if_stmt& x) {
        auto ty = check_type(x.condition, ctx);
        if (ty && *ty != type{bool_type{}}) {
          diagnostic::error("condition type must be `bool`, but is `{}`", *ty)
            .primary(x.condition.get_location())
            .emit(ctx.dh());
        }
        auto then = prepare_pipeline(std::move(x.then), ctx);
        auto else_ = std::optional<tenzir::pipeline>{};
        if (x.else_) {
          else_ = prepare_pipeline(std::move(*x.else_), ctx);
        }
        TENZIR_TODO();
        // ops.push_back(std::make_unique<if_use>(
        //   std::move(x.condition), std::move(then), std::move(else_)));
      },
      [&](auto& x) {
        diagnostic::error("statement not implemented yet")
          .primary(x.get_location())
          .emit(ctx.dh());
      });
  }
  return tenzir::pipeline{std::move(ops)};
}

} // namespace

auto exec(std::string content, std::unique_ptr<diagnostic_handler> diag,
          const exec_config& cfg, caf::actor_system& sys) -> bool {
  (void)sys;
  auto content_view = std::string_view{content};
  auto tokens = tql2::tokenize(content);
  auto diag_wrapper = diagnostic_handler_wrapper{std::move(diag)};
  // TODO: Refactor this.
  arrow::util::InitializeUTF8();
  if (not arrow::util::ValidateUTF8(content)) {
    // Figure out the exact token.
    auto last = size_t{0};
    for (auto& token : tokens) {
      if (not arrow::util::ValidateUTF8(content_view.substr(last, token.end))) {
        // TODO: We can't really do this directly, unless we handle invalid
        // UTF-8 in diagnostics.
        diagnostic::error("invalid UTF8")
          .primary(location{last, token.end})
          .emit(diag_wrapper);
      }
      last = token.end;
    }
    return false;
  }
  if (cfg.dump_tokens) {
    auto last = size_t{0};
    auto has_error = false;
    for (auto& token : tokens) {
      fmt::print("{:>15} {:?}\n", token.kind,
                 content_view.substr(last, token.end - last));
      last = token.end;
      has_error |= token.kind == tql2::token_kind::error;
    }
    return not has_error;
  }
  for (auto& token : tokens) {
    if (token.kind == tql2::token_kind::error) {
      auto begin = size_t{0};
      if (&token != tokens.data()) {
        begin = (&token - 1)->end;
      }
      diagnostic::error("could not parse token")
        .primary(location{begin, token.end})
        .emit(diag_wrapper);
    }
  }
  if (diag_wrapper.error()) {
    return false;
  }
  auto parsed = tql2::parse(tokens, content, diag_wrapper);
  if (diag_wrapper.error()) {
    return false;
  }
  auto reg = registry{};
  // operators
  // reg.add("drop", std::make_unique<drop_def>());
  // reg.add("from", std::make_unique<from_def>());
  // reg.add("group", std::make_unique<group_def>());
  // reg.add("head", std::make_unique<head_def>());
  // reg.add("load_file", std::make_unique<load_file_def>());
  // reg.add("set", std::make_unique<set_def>());
  // reg.add("sort", std::make_unique<sort_def>());
  // reg.add("where", std::make_unique<where_def>());
  // TODO: While we want to be able to operator definitions in plugins, we do
  // not want the mere *existence* to be dependant on which plugins are loaded.
  // Instead, we should always register all operators and then emit an helpful
  // error if the corresponding plugin is not loaded.
  for (auto plugin : plugins::get<tql2::operator_factory_plugin>()) {
    auto name = plugin->name();
    // TODO
    TENZIR_ASSERT(std::string_view{name}.substr(0, 5) == "tql2.");
    reg.add(name.substr(5), plugin);
  }
  // functions
  reg.add("now", std::make_unique<now_def>());
  reg.add("sqrt", std::make_unique<sqrt_def>());
  tql2::resolve_entities(parsed, reg, diag_wrapper);
  if (cfg.dump_ast) {
    with_thread_local_registry(reg, [&] {
      fmt::println("{:#?}", parsed);
    });
    return not diag_wrapper.error();
  }
  // TODO
  auto ctx = context{reg, diag_wrapper};
  auto pipe = prepare_pipeline(std::move(parsed), ctx);
  // TENZIR_WARN("{:#?}", use_default_formatter(pipe));
  if (diag_wrapper.error()) {
    return false;
  }
  exec_pipeline(std::move(pipe), std::move(diag_wrapper).inner(), cfg, sys);
  // diagnostic::warning(
  //   "pipeline is syntactically valid, but execution is not yet
  //   implemented") .hint("use `--dump-ast` to show AST")
  //   .emit(diag_wrapper);
  return true;
}

} // namespace tenzir::tql2
