/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_CONCEPT_PRINTABLE_VAST_EXPRESSION_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_EXPRESSION_HPP

#include "vast/data.hpp"
#include "vast/expression.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/key.hpp"
#include "vast/concept/printable/vast/none.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/concept/printable/vast/type.hpp"

namespace vast {

struct expression_printer : printer<expression_printer> {
  using attribute = expression;

  template <typename Iterator>
  struct visitor {
    visitor(Iterator& out) : out_{out} {
    }

    bool operator()(none) const {
      using vast::print;
      return print(out_, nil);
    }

    bool operator()(const conjunction& c) const {
      auto p = '{' << (expression_printer{} % " && ") << '}';
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
      return visit(*this, p.lhs) && op(out_, p.op) && visit(*this, p.rhs);
    }

    bool operator()(const attribute_extractor& e) const {
      return ('&' << printers::str)(out_, e.attr);
    }

    bool operator()(const type_extractor& e) const {
      return (':' << printers::type<policy::name_only>)(out_, e.type);
    }

    bool operator()(const key_extractor& e) const {
      return printers::key(out_, e.key);
    }

    bool operator()(const data_extractor& e) const {
      auto p = printers::type<policy::name_only> << ~('@' << printers::offset);
      return p(out_, e.type, e.offset);
    }

    bool operator()(const data& d) const {
      return printers::data(out_, d);
    }

    Iterator& out_;
  };

  template <typename Iterator, typename T>
  auto print(Iterator& out, const T& x) const
    -> std::enable_if_t<
         std::is_same<T, attribute_extractor>::value
         || std::is_same<T, key_extractor>::value
         || std::is_same<T, data_extractor>::value
         || std::is_same<T, predicate>::value
         || std::is_same<T, conjunction>::value
         || std::is_same<T, disjunction>::value
         || std::is_same<T, negation>::value,
         bool
       >
  {
    return visitor<Iterator>{out}(x);
  }

  template <typename Iterator>
  bool print(Iterator& out, const expression& e) const
  {
    return visit(visitor<Iterator>{out}, e);
  }
};

template <typename T>
struct printer_registry<
  T,
  std::enable_if_t<
    std::is_same<T, attribute_extractor>::value
    || std::is_same<T, key_extractor>::value
    || std::is_same<T, data_extractor>::value
    || std::is_same<T, predicate>::value
    || std::is_same<T, conjunction>::value
    || std::is_same<T, disjunction>::value
    || std::is_same<T, negation>::value
    || std::is_same<T, expression>::value
  >
>
{
  using type = expression_printer;
};

} // namespace vast

#endif
