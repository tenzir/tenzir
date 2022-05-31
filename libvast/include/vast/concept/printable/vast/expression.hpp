//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/none.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/data.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/expression.hpp"

#include <caf/none.hpp>

namespace vast {

struct expression_printer : printer_base<expression_printer> {
  using attribute = expression;

  template <class Iterator>
  struct visitor {
    visitor(Iterator& out) : out_{out} {
    }

    bool operator()(caf::none_t) const {
      using vast::print;
      return print(out_, caf::none);
    }

    bool operator()(const conjunction& c) const {
      auto p = '(' << (expression_printer{} % " && ") << ')';
      return p(out_, c);
    }

    bool operator()(const disjunction& d) const {
      auto p = '(' << (expression_printer{} % " || ") << ')';
      return p(out_, d);
    }

    bool operator()(const negation& n) const {
      auto p = "! " << expression_printer{};
      return p(out_, n.expr());
    }

    bool operator()(const predicate& p) const {
      auto op = ' ' << make_printer<relational_operator>{} << ' ';
      return caf::visit(*this, p.lhs) && op(out_, p.op)
             && caf::visit(*this, p.rhs);
    }

    bool operator()(const selector& e) const {
      return printers::str(out_, e.kind == selector::type    ? "#type"
                                 : e.kind == selector::field ? "#field"
                                                             : "#import_time");
    }

    bool operator()(const type_extractor& e) const {
      // FIXME: Use {n} for nameonly
      out_ = fmt::format_to(out_, ":{}", e.type);
      return true;
    }

    bool operator()(const extractor& e) const {
      return printers::str(out_, e.value);
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
    requires(detail::is_any_v<T, selector, extractor, data_extractor, predicate,
                              conjunction, disjunction, negation>)
  auto print(Iterator& out, const T& x) const -> bool {
    return visitor<Iterator>{out}(x);
  }

  template <class Iterator>
  bool print(Iterator& out, const expression& e) const {
    return caf::visit(visitor<Iterator>{out}, e);
  }
};

template <class T>
  requires(detail::is_any_v<T, selector, extractor, data_extractor, predicate,
                            conjunction, disjunction, negation, expression>)
struct printer_registry<T> {
  using type = expression_printer;
};

} // namespace vast
