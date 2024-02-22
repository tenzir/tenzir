#include "tenzir/detail/assert.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

class operator_definition {
public:
  virtual ~operator_definition() = default;

  virtual auto name() const -> std::string_view;
};

class function_definition {
public:
  virtual ~function_definition() = default;
};

class entity_registry {
public:
  void add(std::unique_ptr<operator_definition> x) {
  }
};

class tql2_plugin : public plugin {
public:
  virtual void register_entities(entity_registry& r) {
  }
};

} // namespace tenzir

namespace tenzir::plugins::tql2_test {

namespace {

class collect_operator_def final : public operator_definition {
public:
  // take 5
  // group xyz

  // my_op sum
  //       ^^^ associate with scope. resolving is done later.
  // collect sum(x), foo(y, $z)

  // By default, nulls are always last!
  // sort x == null, -x

  // Operators after parsing
  // Input type kind can be known quickly, or pretty late
  // Exact environment
};

class sort_operator_use {
public:
};

class sort_operator_def final : public operator_definition {
public:
  auto name() const -> std::string_view override {
    return "sort2";
  } // namespace

  void use_operator(std::vector<tql2::ast::expression> args) {
    for (auto& arg : args) {
      arg.match(
        [&](tql2::ast::unary_expr& un_expr) {
          if (un_expr.op.inner == tql2::ast::unary_op::neg) {
            // Constructing an intermediate expression object is necessary
            // because the variant will first destroy the contained value when
            // move-assigned.
            // TODO
            arg = std::move(un_expr.expr);
            // and invert ...
          }
        },
        [](auto&) {});
      auto check_and_maybe_compile = [](auto&&) {};
      // TODO
      check_and_maybe_compile(arg);
    }
  }
};

class plugin final : public tql2_plugin {
public:
  auto name() const -> std::string override {
    return "tenzir.sort_operator";
  }

  void register_entities(entity_registry& r) override {
    r.add(std::make_unique<sort_operator_def>());
  }
};

} // namespace

} // namespace tenzir::plugins::tql2_test

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tql2_test::plugin)
