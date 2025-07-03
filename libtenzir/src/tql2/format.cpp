//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/format.hpp"

#include "tenzir/tql2/ast.hpp"
#include "tenzir/variant.hpp"

#include <fmt/core.h>

namespace tenzir {

namespace {

class formatter : public ast::visitor<formatter> {
private:
  struct data_formatter {
    std::string& result;
    auto operator()(const auto& v) const -> bool {
      result += fmt::format("{}", v);
      return true;
    }
    auto operator()(const std::string& s) const -> bool {
      result += fmt::format("\"{}\"", s);
      return true;
    }
    auto operator()(const caf::none_t&) const -> bool {
      result += "null";
      return true;
    }
    auto operator()(const pattern& p) const -> bool {
      result += fmt::format("/{}/", p.string());
      return true;
    }
    auto operator()(const ip& addr) const -> bool {
      result += fmt::format("{}", addr);
      return true;
    }
    auto operator()(const subnet& net) const -> bool {
      result += fmt::format("{}", net);
      return true;
    }
    auto operator()(const blob& b) const -> bool {
      result += fmt::format("<blob:{} bytes>", b.size());
      return true;
    }
    auto operator()(const secret& s) const -> bool {
      result += fmt::format("{}", s);
      return true;
    }
    auto operator()(const list& xs) const -> bool {
      result += "[";
      for (auto it = xs.begin(); it != xs.end(); ++it) {
        auto f = data_formatter{result};
        it->get_data().match([&](const auto& v) { return f(v); });
        if (std::next(it) != xs.end()) {
          result += ", ";
        }
      }
      result += "]";
      return true;
    }
  };

public:
  auto format(const ast::pipeline& pipeline) -> std::string {
    visit(const_cast<ast::pipeline&>(pipeline));
    return std::move(result_);
  }

  void visit(ast::pipeline& x) {
    for (auto it = x.body.begin(); it != x.body.end(); ++it) {
      visit(*it);
      if (std::next(it) != x.body.end()) {
        result_ += "\n";
      }
    }
  }

  void visit(ast::invocation& x) {
    visit(x.op);
    if (x.op.path.size() == 1 && x.op.path[0].name == "export") {
      // Do nothing, no parentheses for 'export' command
    } else {
      result_ += "(";
      for (auto it = x.args.begin(); it != x.args.end(); ++it) {
        visit(*it);
        if (std::next(it) != x.args.end()) {
          result_ += ", ";
        }
      }
      result_ += ")";
    }
  }

  void visit(ast::entity& x) {
    for (auto it = x.path.begin(); it != x.path.end(); ++it) {
      visit(*it);
      if (std::next(it) != x.path.end()) {
        result_ += ".";
      }
    }
  }

  void visit(ast::identifier& x) {
    result_ += x.name;
  }

  void visit(ast::record& x) {
    if (x.items.empty()) {
      result_ += "{}";
      return;
    }
    result_ += "{\n";
    indent_ += 2;
    for (auto it = x.items.begin(); it != x.items.end(); ++it) {
      indent();
      it->match(
        [&](ast::record::field& field) {
          visit(field.name);
          result_ += ": ";
          visit(field.expr);
        },
        [&](ast::spread& spread) {
          result_ += "...";
          visit(spread.expr);
        });
      result_ += ",\n";
    }
    indent_ -= 2;
    indent();
    result_ += "}";
  }

  void visit(ast::list& x) {
    result_ += "[";
    for (auto it = x.items.begin(); it != x.items.end(); ++it) {
      if (it != x.items.begin()) {
        result_ += ", ";
      }
      it->match(
        [&](ast::expression& expr) {
          visit(expr);
        },
        [&](ast::spread& spread) {
          result_ += "...";
          visit(spread.expr);
        });
    }
    result_ += "]";
  }

  void visit(ast::field_access& x) {
    visit(x.left);
    result_ += ".";
    if (x.has_question_mark) {
      result_ += "?";
    }
    visit(x.name);
  }

  void visit(ast::index_expr& x) {
    visit(x.expr);
    result_ += "[";
    visit(x.index);
    result_ += "]";
    if (x.has_question_mark) {
      result_ += "?";
    }
  }

  void visit(ast::binary_expr& x) {
    visit(x.left);
    result_ += " ";
    
    // Map binary operators to their symbols
    switch (x.op.inner) {
      case ast::binary_op::add: result_ += "+"; break;
      case ast::binary_op::sub: result_ += "-"; break;
      case ast::binary_op::mul: result_ += "*"; break;
      case ast::binary_op::div: result_ += "/"; break;
      case ast::binary_op::eq: result_ += "=="; break;
      case ast::binary_op::neq: result_ += "!="; break;
      case ast::binary_op::gt: result_ += ">"; break;
      case ast::binary_op::geq: result_ += ">="; break;
      case ast::binary_op::lt: result_ += "<"; break;
      case ast::binary_op::leq: result_ += "<="; break;
      case ast::binary_op::and_: result_ += "and"; break;
      case ast::binary_op::or_: result_ += "or"; break;
      case ast::binary_op::in: result_ += "in"; break;
      case ast::binary_op::if_: result_ += "if"; break;
      case ast::binary_op::else_: result_ += "else"; break;
    }
    
    result_ += " ";
    visit(x.right);
  }

  void visit(ast::unary_expr& x) {
    // Map unary operators to their symbols
    switch (x.op.inner) {
      case ast::unary_op::pos: result_ += "+"; break;
      case ast::unary_op::neg: result_ += "-"; break;
      case ast::unary_op::not_: result_ += "not "; break;
      case ast::unary_op::move: result_ += "move "; break;
    }
    visit(x.expr);
  }

  void visit(ast::function_call& x) {
    if (x.method && !x.args.empty()) {
      // Format as method call: object.method(remaining_args)
      visit(x.args[0]);
      result_ += ".";
      visit(x.fn);
      result_ += "(";
      for (auto it = x.args.begin() + 1; it != x.args.end(); ++it) {
        if (it != x.args.begin() + 1) {
          result_ += ", ";
        }
        visit(*it);
      }
      result_ += ")";
    } else {
      // Format as regular function call: function(args)
      visit(x.fn);
      result_ += "(";
      for (auto it = x.args.begin(); it != x.args.end(); ++it) {
        if (it != x.args.begin()) {
          result_ += ", ";
        }
        visit(*it);
      }
      result_ += ")";
    }
  }

  void visit(ast::lambda_expr& x) {
    visit(x.left);
    result_ += " -> ";
    visit(x.right);
  }

  void visit(ast::assignment& x) {
    visit(x.left);
    result_ += " = ";
    visit(x.right);
  }

  void visit(ast::pipeline_expr& x) {
    result_ += "(";
    visit(x.inner);
    result_ += ")";
  }

  void visit(ast::expression& x) {
    x.match(
      [&](ast::constant& c) {
        auto f = data_formatter{result_};
        c.value.match([&](const auto& v) { return f(v); });
      },
      [&](auto& y) { visit(y); });
  }

  void visit(ast::selector& x) {
    x.match([&](auto& y) { visit(y); });
  }

  void visit(ast::field_path& x) {
    if (x.has_this()) {
      result_ += "this";
    }
    
    bool first = !x.has_this();
    for (const auto& segment : x.path()) {
      if (!first) {
        result_ += ".";
        if (segment.has_question_mark) {
          result_ += "?";
        }
      }
      result_ += segment.id.name;
      first = false;
    }
  }

  void visit(ast::meta& x) {
    result_ += "@";
    switch (x.kind) {
      case ast::meta::name:
        result_ += "name";
        break;
      case ast::meta::import_time:
        result_ += "import_time";
        break;
      case ast::meta::internal:
        result_ += "internal";
        break;
    }
  }

  void visit(ast::this_& x) {
    (void)x; // suppress unused parameter warning
    result_ += "this";
  }

  void visit(ast::root_field& x) {
    if (x.has_question_mark) {
      result_ += "?";
    }
    visit(x.id);
  }

  void visit(ast::underscore& x) {
    (void)x; // suppress unused parameter warning
    result_ += "_";
  }

  void visit(ast::dollar_var& x) {
    result_ += x.id.name;
  }

  void visit(ast::spread& x) {
    result_ += "...";
    visit(x.expr);
  }

  void visit(ast::unpack& x) {
    result_ += "...";
    visit(x.expr);
  }

  void visit(ast::constant& x) {
    auto f = data_formatter{result_};
    x.value.match([&](const auto& v) { return f(v); });
  }

  void visit(ast::format_expr& x) {
    result_ += "f\"";
    for (auto& segment : x.segments) {
      segment.match(
        [&](const std::string& str) {
          result_ += str;
        },
        [&](ast::format_expr::replacement& repl) {
          result_ += "{";
          visit(repl.expr);
          result_ += "}";
        });
    }
    result_ += "\"";
  }

  void visit(ast::let_stmt& x) {
    result_ += "let ";
    visit(x.name);
    result_ += " = ";
    visit(x.expr);
  }

  void visit(ast::if_stmt& x) {
    result_ += "if ";
    visit(x.condition);
    result_ += " {\n";
    visit(x.then);
    result_ += "\n}";
    if (x.else_) {
      result_ += " else {\n";
      visit(x.else_->pipe);
      result_ += "\n}";
    }
  }

  void visit(ast::match_stmt& x) {
    result_ += "match ";
    visit(x.expr);
    result_ += " {\n";
    for (auto it = x.arms.begin(); it != x.arms.end(); ++it) {
      for (auto filter_it = it->filter.begin(); filter_it != it->filter.end(); ++filter_it) {
        if (filter_it != it->filter.begin()) {
          result_ += ", ";
        }
        visit(*filter_it);
      }
      result_ += " => {\n";
      visit(it->pipe);
      result_ += "\n}";
      if (std::next(it) != x.arms.end()) {
        result_ += "\n";
      }
    }
    result_ += "\n}";
  }

  // Fallback template method - calls enter for default traversal
  template <class T>
  void visit(T& x) {
    enter(x);
  }

private:
  void indent() {
    for (int i = 0; i < indent_; ++i) {
      result_ += " ";
    }
  }

  std::string result_;
  int indent_ = 0;
};

} // namespace

auto format_pipeline(const ast::pipeline& pipeline) -> std::string {
  return formatter{}.format(pipeline);
}

} // namespace tenzir