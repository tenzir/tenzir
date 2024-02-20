#include "tenzir/plugin.hpp"

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
  void add(std::unique_ptr<operator_definition> x);
};

class tql2_plugin : public plugin {
public:
  virtual void register_entities(entity_registry& r);
};

} // namespace tenzir

namespace tenzir::plugins::tql2_test {

namespace {

class collect_operator_def final : public operator_definition {
public:
  // my_op sum
  //       ^^^ associate with scope. resolving is done later.
  // collect sum(x), foo(y, $z)

  // Operators after parsing
  // Input type kind can be known quickly, or pretty late
  // Exact environment
};

class sort_operator_def final : public operator_definition {
public:
  auto name() const -> std::string_view override {
    return "sort2";
  };
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
