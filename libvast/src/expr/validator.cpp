#include "vast/expression.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/expr/validator.hpp"

namespace vast {
namespace expr {

maybe<void> validator::operator()(none) const {
  return fail("nil expression");
}

maybe<void> validator::operator()(conjunction const& c) const {
  for (auto& op : c) {
    auto m = visit(*this, op);
    if (!m)
      return m;
  }
  return {};
}

maybe<void> validator::operator()(disjunction const& d) const {
  for (auto& op : d) {
    auto m = visit(*this, op);
    if (!m)
      return m;
  }
  return {};
}

maybe<void> validator::operator()(negation const& n) const {
  return visit(*this, n.expression());
}

maybe<void> validator::operator()(predicate const& p) const {
  auto valid = [&](predicate::operand const& lhs,
                   predicate::operand const& rhs) -> maybe<void> {
    auto rhs_data = get<data>(rhs);
    if (is<event_extractor>(lhs) && rhs_data) {
      if (!compatible(type::string{}, p.op, type::derive(*rhs_data)))
        return fail<ec::type_clash>("invalid event extractor: ", *rhs_data,
                                    " under ", p.op);
    } else if (is<time_extractor>(lhs) && rhs_data) {
      if (!compatible(type::time_point{}, p.op, type::derive(*rhs_data)))
        return fail<ec::type_clash>("invalid time extractor: ", *rhs_data, 
                                    " under ", p.op);
    } else {
      auto t = get<type_extractor>(lhs);
      if (t && rhs_data) {
        if (!compatible(t->type, p.op, type::derive(*rhs_data)))
          return fail<ec::type_clash>("invalid type extractor: ",
                                      t->type, ' ', p.op, ' ', *rhs_data);
      } else if (is<schema_extractor>(lhs) && rhs_data) {
        return {};
      } else {
        return fail<ec::invalid_query>("invalid extractor");
      }
    }
    return {};
  };
  auto tl = valid(p.lhs, p.rhs);
  auto tr = valid(p.rhs, p.lhs);
  if (tl || tr)
    return {};
  else if (tl)
    return tr;
  else
    return tl;
}

} // namespace expr
} // namespace vast
