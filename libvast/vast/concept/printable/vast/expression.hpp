#ifndef VAST_CONCEPT_PRINTABLE_VAST_EXPRESSION_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_EXPRESSION_HPP

#include "vast/data.hpp"
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

    bool operator()(conjunction const& c) const {
      auto p = '{' << (expression_printer{} % " && ") << '}';
      return p.print(out_, c);
    }

    bool operator()(disjunction const& d) const {
      auto p = '(' << (expression_printer{} % " || ") << ')';
      return p.print(out_, d);
    }

    bool operator()(negation const& n) const {
      auto p = "! " << expression_printer{};
      return p.print(out_, n.expr());
    }

    bool operator()(predicate const& p) const {
      auto op = ' ' << make_printer<relational_operator>{} << ' ';
      return visit(*this, p.lhs) && op.print(out_, p.op) && visit(*this, p.rhs);
    }

    bool operator()(attribute_extractor const& e) const {
      auto p = '&' << printers::str;
      return p.print(out_, e.attr);
    }

    bool operator()(key_extractor const& e) const {
      return printers::key.print(out_, e.key);
    }

    bool operator()(data_extractor const& e) const {
      auto p = printers::type<policy::name_only> << ~('@' << printers::offset);
      return p.print(out_, std::tie(e.type, e.offset));
    }

    bool operator()(data const& d) const {
      return printers::data.print(out_, d);
    }

    Iterator& out_;
  };

  template <typename Iterator, typename T>
  auto print(Iterator& out, T const& x) const
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
  bool print(Iterator& out, expression const& e) const
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
