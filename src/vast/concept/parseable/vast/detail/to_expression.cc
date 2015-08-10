#include "vast/concept/parseable/vast/detail/expression.h"

namespace vast {
namespace detail {

trial<expression> to_expression(std::string const& str) {
  std::string err;
  using iterator_type = std::string::const_iterator;
  auto f = str.begin();
  auto l = str.end();
  detail::parser::error_handler<iterator_type> on_error{f, l, err};
  detail::parser::query<iterator_type> grammar{on_error};
  detail::parser::skipper<iterator_type> skipper;
  detail::ast::query::query_expr q;
  if (!phrase_parse(f, l, grammar, skipper, q))
    return error{std::move(err)};
  auto t = detail::expression_factory{}(q);
  if (!t)
    return t.error();
  auto expr = std::move(*t);
  auto v = visit(expr::validator{}, expr);
  if (!v)
    return v.error();
  return visit(expr::hoister{}, expr);
}

} // namespace detail
} // namespace vast
