#include <algorithm>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/key.hpp"
#include "vast/concept/printable/vast/operator.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/die.hpp"
#include "vast/event.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/type.hpp"

namespace vast {

expression hoister::operator()(none) const {
  return expression{};
}

expression hoister::operator()(conjunction const& c) const {
  conjunction hoisted;
  for (auto& op : c)
    if (auto inner = get_if<conjunction>(op))
      for (auto& inner_op : *inner)
        hoisted.push_back(visit(*this, inner_op));
    else
      hoisted.push_back(visit(*this, op));
  return hoisted.size() == 1 ? hoisted[0] : hoisted;
}

expression hoister::operator()(disjunction const& d) const {
  disjunction hoisted;
  for (auto& op : d)
    if (auto inner = get_if<disjunction>(op))
      for (auto& inner_op : *inner)
        hoisted.push_back(visit(*this, inner_op));
    else
      hoisted.push_back(visit(*this, op));
  return hoisted.size() == 1 ? hoisted[0] : hoisted;
}

expression hoister::operator()(negation const& n) const {
  return {negation{visit(*this, n.expr())}};
}

expression hoister::operator()(predicate const& p) const {
  return {p};
}


expression aligner::operator()(none) const {
  return nil;
}

expression aligner::operator()(conjunction const& c) const {
  conjunction result;
  for (auto& op : c)
    result.push_back(visit(*this, op));
  return result;
}

expression aligner::operator()(disjunction const& d) const {
  disjunction result;
  for (auto& op : d)
    result.push_back(visit(*this, op));
  return result;
}

expression aligner::operator()(negation const& n) const {
  return {negation{visit(*this, n.expr())}};
}

expression aligner::operator()(predicate const& p) const {
  auto is_extractor = [](auto& operand) { return !is<data>(operand); };
  // Already aligned if LHS is an extractor or no extractor present.
  if (is_extractor(p.lhs) || !is_extractor(p.rhs))
    return p;
  return predicate{p.rhs, flip(p.op), p.lhs};
}


denegator::denegator(bool negate) : negate_{negate} {
}

expression denegator::operator()(none) const {
  return nil;
}

expression denegator::operator()(conjunction const& c) const {
  auto add = [&](auto x) -> expression {
    for (auto& op : c)
      x.push_back(visit(*this, op));
    return x;
  };
  return negate_ ? add(disjunction{}) : add(conjunction{});
}

expression denegator::operator()(disjunction const& d) const {
  auto add = [&](auto x) -> expression {
    for (auto& op : d)
      x.push_back(visit(*this, op));
    return x;
  };
  return negate_ ? add(conjunction{}) : add(disjunction{});
}

expression denegator::operator()(negation const& n) const {
  // Step through double negations.
  if (auto inner = get_if<negation>(n.expr()))
    return visit(*this, inner->expr());
  // Apply De Morgan from here downward.
  return visit(denegator{!negate_}, n.expr());
}

expression denegator::operator()(predicate const& p) const {
  return predicate{p.lhs, negate_ ? negate(p.op) : p.op, p.rhs};
}


expression deduplicator::operator()(none) const {
  return nil;
}

expression deduplicator::operator()(conjunction const& c) const {
  conjunction result;
  for (auto& op : c) {
    auto p = visit(*this, op);
    if (std::count(result.begin(), result.end(), p) == 0)
      result.push_back(p);
  }
  return result;
}

expression deduplicator::operator()(disjunction const& d) const {
  disjunction result;
  for (auto& op : d) {
    auto p = visit(*this, op);
    if (std::count(result.begin(), result.end(), p) == 0)
      result.push_back(p);
  }
  return result;
}

expression deduplicator::operator()(negation const& n) const {
  return visit(*this, n.expr());
}

expression deduplicator::operator()(predicate const& p) const {
  return p;
}


namespace {

template <class Ts, class Us>
auto inplace_union(Ts& xs, const Us& ys) {
  auto mid = xs.size();
  std::copy(ys.begin(), ys.end(), std::back_inserter(xs));
  std::inplace_merge(xs.begin(), xs.begin() + mid, xs.end());
  xs.erase(std::unique(xs.begin(), xs.end()), xs.end());
}

} // namespace <anonymous>

std::vector<predicate> predicatizer::operator()(none) const {
  return {};
}

std::vector<predicate> predicatizer::operator()(conjunction const& con) const {
  std::vector<predicate> result;
  for (auto& op : con) {
    auto ps = visit(*this, op);
    inplace_union(result, ps);
  }
  return result;
}

std::vector<predicate> predicatizer::operator()(disjunction const& dis) const {
  std::vector<predicate> result;
  for (auto& op : dis) {
    auto ps = visit(*this, op);
    inplace_union(result, ps);
  }
  return result;
}

std::vector<predicate> predicatizer::operator()(negation const& n) const {
  return visit(*this, n.expr());
}

std::vector<predicate> predicatizer::operator()(predicate const& pred) const {
  return {pred};
}


expected<void> validator::operator()(none) {
  return make_error(ec::syntax_error, "nil expression is invalid");
}

expected<void> validator::operator()(conjunction const& c) {
  for (auto& op : c) {
    auto m = visit(*this, op);
    if (!m)
      return m;
  }
  return no_error;
}

expected<void> validator::operator()(disjunction const& d) {
  for (auto& op : d) {
    auto m = visit(*this, op);
    if (!m)
      return m;
  }
  return no_error;
}

expected<void> validator::operator()(negation const& n) {
  return visit(*this, n.expr());
}

expected<void> validator::operator()(predicate const& p) {
  op_ = p.op;
  return visit(*this, p.lhs, p.rhs);
}

expected<void> validator::operator()(attribute_extractor const& ex,
                                     data const& d) {
  if (ex.attr == "type" && !is<std::string>(d))
    return make_error(ec::syntax_error,
                      "type attribute extractor requires string operand",
                      ex.attr, op_, d);
  else if (ex.attr == "time" && !is<timestamp>(d))
    return make_error(ec::syntax_error,
                      "time attribute extractor requires timestamp operand",
                      ex.attr, op_, d);
  return no_error;
}

expected<void> validator::operator()(type_extractor const& ex, data const& d) {
  if (!type_check(ex.type, d))
    return make_error(ec::syntax_error, "type extractor type check failure",
                      ex.type, op_, d);
  return no_error;
}

expected<void> validator::operator()(key_extractor const&, data const&) {
  // Validity of a key extractor requires a specific schema, which we don't
  // have in this context.
  return no_error;
}

time_restrictor::time_restrictor(timestamp first, timestamp last)
  : first_{first}, last_{last} {
}

bool time_restrictor::operator()(none) const {
  die("should never happen");
  return false;
}

bool time_restrictor::operator()(conjunction const& con) const {
  for (auto& op : con)
    if (!visit(*this, op))
      return false;
  return true;
}

bool time_restrictor::operator()(disjunction const& dis) const {
  for (auto& op : dis)
    if (visit(*this, op))
      return true;
  return false;
}

bool time_restrictor::operator()(negation const& n) const {
  // We can only apply a negation if it sits directly on top of a time
  // extractor, because only then we can negate the meaning of the temporal
  // constraint.
  auto r = visit(*this, n.expr());
  if (auto p = get_if<predicate>(n.expr()))
    if (auto a = get_if<attribute_extractor>(p->lhs))
      if (a->attr == "time")
        return !r;
  return r;
}

bool time_restrictor::operator()(predicate const& p) const {
  if (auto a = get_if<attribute_extractor>(p.lhs)) {
    if (a->attr == "time") {
      auto d = get_if<data>(p.rhs);
      VAST_ASSERT(d && is<timestamp>(*d)); // require validation
      return evaluate(first_, p.op, *d) || evaluate(last_, p.op, *d);
    }
  }
  return true; // nothing to retrict.
}


type_resolver::type_resolver(type const& t) : type_{t} {
}

expected<expression> type_resolver::operator()(none) {
  return expression{};
}

expected<expression> type_resolver::operator()(conjunction const& c) {
  conjunction result;
  for (auto& op : c) {
    auto r = visit(*this, op);
    if (!r)
      return r;
    else if (is<none>(*r))
      return expression{};
    else
      result.push_back(std::move(*r));
  }

  if (result.empty())
    return expression{};
  if (result.size() == 1)
    return {std::move(result[0])};
  else
    return {std::move(result)};
}

expected<expression> type_resolver::operator()(disjunction const& d) {
  disjunction result;
  for (auto& op : d) {
    auto r = visit(*this, op);
    if (!r)
      return r;
    else if (!is<none>(*r))
      result.push_back(std::move(*r));
  }
  if (result.empty())
    return expression{};
  if (result.size() == 1)
    return {std::move(result[0])};
  else
    return {std::move(result)};
}


expected<expression> type_resolver::operator()(negation const& n) {
  auto r = visit(*this, n.expr());
  if (!r)
    return r;
  else if (!is<none>(*r))
    return {negation{std::move(*r)}};
  else
    return expression{};
}

expected<expression> type_resolver::operator()(predicate const& p) {
  op_ = p.op;
  return visit(*this, p.lhs, p.rhs);
}

expected<expression> type_resolver::operator()(type_extractor const& ex,
                                               data const& d) {
  disjunction dis;
  if (auto r = get_if<record_type>(type_)) {
    for (auto& f : record_type::each{*r}) {
      auto& value_type = f.trace.back()->type;
      if (congruent(value_type, ex.type)) {
        auto x = data_extractor{type_, f.offset};
        dis.emplace_back(predicate{std::move(x), op_, d});
      }
    }
  } else if (congruent(type_, ex.type)) {
    auto x = data_extractor{type_, offset{}};
    dis.emplace_back(predicate{std::move(x), op_, d});
  }
  if (dis.empty())
    return expression{}; // did not resolve
  else if (dis.size() == 1)
    return {std::move(dis[0])};
  else
    return {std::move(dis)};
}

expected<expression> type_resolver::operator()(data const& d,
                                               type_extractor const& ex) {
  return (*this)(ex, d);
}

expected<expression> type_resolver::operator()(key_extractor const& ex,
                                               data const& d) {
  disjunction dis;
  // First, interpret the key as a suffix of a record field name.
  if (auto r = get_if<record_type>(type_)) {
    auto suffixes = r->find_suffix(ex.key);
    // All suffixes must pass the type check, otherwise the RHS of a
    // predicate would be ambiguous.
    for (auto& pair : suffixes) {
      auto t = r->at(pair.first);
      VAST_ASSERT(t);
      if (!compatible(*t, op_, d))
        return make_error(ec::type_clash, *t, op_, d);
    }
    for (auto& pair : suffixes) {
      auto x = data_extractor{type_, std::move(pair.first)};
      dis.emplace_back(predicate{std::move(x), op_, d});
    }
  // Second, try to interpret the key as the name of a single type.
  } else if (ex.key[0] == type_.name()) {
    if (!compatible(type_, op_, d)) {
      return make_error(ec::type_clash, type_, op_, d);
    }
    auto x = data_extractor{type_, {}};
    dis.emplace_back(predicate{std::move(x), op_, d});
  }
  if (dis.empty())
    return expression{}; // did not resolve
  else if (dis.size() == 1)
    return {std::move(dis[0])};
  else
    return {std::move(dis)};
}

expected<expression> type_resolver::operator()(data const& d,
                                               key_extractor const& ex) {
  return (*this)(ex, d);
}


type_pruner::type_pruner(type const& event_type) : type_{event_type} {
}

expression type_pruner::operator()(none) {
  return {};
}

expression type_pruner::operator()(conjunction const& c) {
  conjunction result;
  for (auto& op : c) {
    auto e = visit(*this, op);
    if (is<none>(e))
      // If any operand of the conjunction is not a viable type resolver, the
      // entire conjunction is not viable. For example, if we have an event
      // consisting only of numeric types and the conjunction (int == +42 &&
      // string == "foo"), then the entire conjunction is not viable.
      return {};
    else
      result.push_back(std::move(e));
  }
  if (result.empty())
    return {};
  if (result.size() == 1)
    return result[0];
  return result;
}

expression type_pruner::operator()(disjunction const& d) {
  disjunction result;
  for (auto& op : d) {
    auto e = visit(*this, op);
    if (!is<none>(e))
      result.push_back(std::move(e));
  }
  if (result.empty())
    return {};
  if (result.size() == 1)
    return result[0];
  return result;
}

expression type_pruner::operator()(negation const& n) {
  auto e = visit(*this, n.expr());
  if (is<none>(e))
    return e;
  return negation{std::move(e)};
}

expression type_pruner::operator()(predicate const& p) {
  if (auto lhs = get_if<type_extractor>(p.lhs)) {
    if (auto r = get_if<record_type>(type_)) {
      disjunction result;
      for (auto& e : record_type::each{*r})
        if (congruent(e.trace.back()->type, lhs->type)) {
          auto de = data_extractor{type_, e.offset};
          result.emplace_back(predicate{std::move(de), p.op, p.rhs});
        }
      if (result.empty())
        return {};
      if (result.size() == 1)
        return result[0];
      return result;
    } else if (congruent(type_, lhs->type)) {
      return predicate{data_extractor{type_, {}}, p.op, p.rhs};
    }
  } else if (auto lhs = get_if<data_extractor>(p.lhs)) {
    if (lhs->type != type_)
      return {};
  }
  return p;
}

event_evaluator::event_evaluator(event const& e) : event_{e} {
}

bool event_evaluator::operator()(none) {
  return false;
}

bool event_evaluator::operator()(conjunction const& c) {
  for (auto& op : c)
    if (!visit(*this, op))
      return false;
  return true;
}

bool event_evaluator::operator()(disjunction const& d) {
  for (auto& op : d)
    if (visit(*this, op))
      return true;
  return false;
}

bool event_evaluator::operator()(negation const& n) {
  return !visit(*this, n.expr());
}

bool event_evaluator::operator()(predicate const& p) {
  op_ = p.op;
  return visit(*this, p.lhs, p.rhs);
}

bool event_evaluator::operator()(attribute_extractor const& e, data const& d) {
  // FIXME: perform a transformation on the AST that replaces the attribute
  // with the corresponding function object.
  if (e.attr == "type")
    return evaluate(event_.type().name(), op_, d);
  if (e.attr == "time")
    return evaluate(event_.timestamp(), op_, d);
  return false;
}

bool event_evaluator::operator()(type_extractor const&, data const&) {
  die("type extractor should have been resolved at this point");
}

bool event_evaluator::operator()(key_extractor const&, data const&) {
  die("key extractor should have been resolved at this point");
}

bool event_evaluator::operator()(data_extractor const& e, data const& d) {
  if (e.type != event_.type())
    return false;
  if (e.offset.empty())
    return evaluate(event_.data(), op_, d);
  VAST_ASSERT(is<record_type>(event_.type())); // offset wouldn't be empty
  if (auto r = get_if<vector>(event_.data()))
    if (auto x = get(*r, e.offset))
      return evaluate(*x, op_, d);
  return false;
}

} // namespace vast
