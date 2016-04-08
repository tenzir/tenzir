#ifndef VAST_CONCEPT_PRINTABLE_VAST_EXPRESSION_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_EXPRESSION_HPP

#include "vast/data.hpp"
#include "vast/util/string.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/key.hpp"
#include "vast/concept/printable/vast/none.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/concept/printable/vast/operator.hpp"

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
      using vast::print;
      return print(out_, '{')
             && detail::print_delimited(c.begin(), c.end(), out_, " && ")
             && print(out_, '}');
    }

    bool operator()(disjunction const& d) const {
      using vast::print;
      return print(out_, '(')
             && detail::print_delimited(d.begin(), d.end(), out_, " || ")
             && print(out_, ')');
    }

    bool operator()(negation const& n) const {
      using vast::print;
      return print(out_, "! ") && print(out_, n.expression());
    }

    bool operator()(predicate const& p) const {
      using vast::print;
      return visit(*this, p.lhs) && print(out_, ' ') && print(out_, p.op)
             && print(out_, ' ') && visit(*this, p.rhs);
    }

    bool operator()(event_extractor const&) const {
      using vast::print;
      return print(out_, "&type");
    }

    bool operator()(time_extractor const&) const {
      using vast::print;
      return print(out_, "&time");
    }

    bool operator()(type_extractor const& e) const {
      using vast::print;
      return print(out_, e.type);
    }

    bool operator()(schema_extractor const& e) const {
      using vast::print;
      return print(out_, e.key);
    }

    bool operator()(data_extractor const& e) const {
      using vast::print;
      if (!print(out_, e.type))
        return false;
      return e.offset.empty() || (print(out_, '@') && print(out_, e.offset));
    }

    bool operator()(data const& d) const {
      using vast::print;
      return print(out_, d);
    }

    Iterator& out_;
  };

  template <typename Iterator, typename T>
  auto print(Iterator& out, T const& x) const
    -> std::enable_if_t<
         std::is_same<T, event_extractor>::value
         || std::is_same<T, time_extractor>::value
         || std::is_same<T, type_extractor>::value
         || std::is_same<T, schema_extractor>::value
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
    std::is_same<T, event_extractor>::value
    || std::is_same<T, time_extractor>::value
    || std::is_same<T, type_extractor>::value
    || std::is_same<T, schema_extractor>::value
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
