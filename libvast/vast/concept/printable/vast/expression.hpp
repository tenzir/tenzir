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
#include "vast/concept/printable/vast/none.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/data.hpp"
#include "vast/expression.hpp"
#include "vast/fmt_integration.hpp"

#include <caf/none.hpp>

namespace vast {

struct expression_printer : printer<expression_printer> {
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

    bool operator()(const meta_extractor& e) const {
      return printers::str(out_,
                           e.kind == meta_extractor::type ? "#type" : "#field");
    }

    bool operator()(const type_extractor& e) const {
      return (':' << printers::type<policy::name_only>) (out_, e.type);
    }

    bool operator()(const field_extractor& e) const {
      return printers::str(out_, e.field);
    }

    bool operator()(const data_extractor& e) const {
      auto p = printers::type<policy::name_only> << ~('@' << printers::offset);
      return p(out_, e.type, e.offset);
    }

    bool operator()(const data& d) const {
      out_ = fmt::format_to(out_, "{:a}", d);
      return true;
    }

    Iterator& out_;
  };

  template <class Iterator, class T>
  auto print(Iterator& out, const T& x) const -> std::enable_if_t<
    std::disjunction_v<std::is_same<T, meta_extractor>,
                       std::is_same<T, field_extractor>,
                       std::is_same<T, data_extractor>,
                       std::is_same<T, predicate>, std::is_same<T, conjunction>,
                       std::is_same<T, disjunction>, std::is_same<T, negation>>,
    bool> {
    return visitor<Iterator>{out}(x);
  }

  template <class Iterator>
  bool print(Iterator& out, const expression& e) const {
    return caf::visit(visitor<Iterator>{out}, e);
  }
};

template <class T>
struct printer_registry<
  T, std::enable_if_t<std::disjunction_v<
       std::is_same<T, meta_extractor>, std::is_same<T, field_extractor>,
       std::is_same<T, data_extractor>, std::is_same<T, predicate>,
       std::is_same<T, conjunction>, std::is_same<T, disjunction>,
       std::is_same<T, negation>, std::is_same<T, expression>>>> {
  using type = expression_printer;
};

} // namespace vast
