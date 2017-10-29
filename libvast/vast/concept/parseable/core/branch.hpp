#ifndef VAST_CONCEPT_PARSEABLE_CORE_BRANCH_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_BRANCH_HPP

#include <tuple>

#include "vast/concept/parseable/core/choice.hpp"
#include "vast/concept/parseable/core/epsilon.hpp"
#include "vast/concept/parseable/core/not.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <class Function, class If, class Else>
class branch_parser : public parser<branch_parser<Function, If, Else>> {
public:
  using attribute = typename choice_parser<If, Else>::attribute;

  branch_parser(Function f, If i, Else e) : f_{f}, if_{i}, else_{e} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f_())
      return if_(f, l, a);
    else
      return else_(f, l, a);
  }

  Function f_;
  If if_;
  Else else_;
};

template <class Function, class If>
auto branch(Function f, If i) {
  auto fail = !parsers::eps;
  return branch_parser<Function, If, decltype(fail)>(f, i, fail);
}

template <class Function, class If, class Else>
auto branch(Function f, If i, Else e) {
  return branch_parser<Function, If, Else>(f, i, e);
}

template <class F0, class P0, class F1, class P1, class... Tail>
auto branch(F0 f0, P0 p0, F1 f1, P1 p1, Tail&&... t) {
  return branch(f0, p0, branch(f1, p1, std::forward<Tail>(t)...));
}

template <class T>
auto when(T&) {
  return !parsers::eps;
}

template <class T, class V, class If, class... Else>
auto when(T& x, V&& v, If i, Else&&... e) {
  auto f = [&] { return x == v; };
  return branch(f, i, when(x, std::forward<Else>(e)...));
}

} // namespace vast

#endif
