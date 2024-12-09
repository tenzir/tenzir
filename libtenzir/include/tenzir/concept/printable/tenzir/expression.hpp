//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/numeric.hpp"
#include "tenzir/concept/printable/string.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/tenzir/none.hpp"
#include "tenzir/concept/printable/tenzir/offset.hpp"
#include "tenzir/concept/printable/tenzir/operator.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/expression.hpp"

#include <caf/none.hpp>

namespace tenzir {

struct expression_printer : printer_base<expression_printer> {
  using attribute = expression;

  template <class Iterator>
  struct visitor {
    explicit visitor(Iterator& out) : out_{out} {
    }

    bool operator()(caf::none_t) const {
      return tenzir::print(out_, caf::none);
    }

    bool operator()(const conjunction& c) const {
      auto p = '(' << (expression_printer{} % " and ") << ')';
      return p(out_, c);
    }

    bool operator()(const disjunction& d) const {
      auto p = '(' << (expression_printer{} % " or ") << ')';
      return p(out_, d);
    }

    bool operator()(const negation& n) const {
      auto p = "not " << expression_printer{};
      return p(out_, n.expr());
    }

    bool operator()(const predicate& p) const {
      auto op = ' ' << make_printer<relational_operator>{} << ' ';
      return match(p.lhs, *this) && op(out_, p.op) && match(p.rhs, *this);
    }

    bool operator()(const operand& op) const {
      return match(op, *this);
    }

    bool operator()(const meta_extractor& e) const {
      switch (e.kind) {
        case meta_extractor::schema:
          return printers::str(out_, "#schema");
        case meta_extractor::schema_id:
          return printers::str(out_, "#schema_id");
        case meta_extractor::import_time:
          return printers::str(out_, "#import_time");
        case meta_extractor::internal:
          return printers::str(out_, "#internal");
      }
      TENZIR_UNREACHABLE();
    }

    bool operator()(const type_extractor& e) const {
      // FIXME: Use {n} for nameonly
      out_ = fmt::format_to(out_, ":{}", e.type);
      return true;
    }

    bool operator()(const field_extractor& e) const {
      return printers::str(out_, e.field);
    }

    bool operator()(const data_extractor& e) const {
      out_ = fmt::format_to(out_, "{}@{}", e.type, e.column);
      return true;
    }

    bool operator()(const data& d) const {
      return printers::data(out_, d);
    }

    Iterator& out_;
  };

  template <class Iterator, class T>
    requires(
      detail::is_any_v<T, meta_extractor, field_extractor, data_extractor,
                       operand, predicate, conjunction, disjunction, negation>)
  auto print(Iterator& out, const T& x) const -> bool {
    return visitor<Iterator>{out}(x);
  }

  template <class Iterator>
  bool print(Iterator& out, const expression& e) const {
    return match(e, visitor<Iterator>{out});
  }
};

template <class T>
  requires(
    detail::is_any_v<T, meta_extractor, field_extractor, data_extractor, operand,
                     predicate, conjunction, disjunction, negation, expression>)
struct printer_registry<T> {
  using type = expression_printer;
};

} // namespace tenzir
