#include <algorithm>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
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
  return {is<data>(p.rhs) ? p : predicate{p.rhs, flip(p.op), p.lhs}};
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


std::vector<predicate> predicatizer::operator()(disjunction const& dis) const {
  std::vector<predicate> preds;
  for (auto& op : dis) {
    auto ps = visit(*this, op);
    std::move(ps.begin(), ps.end(), std::back_inserter(preds));
  }
  return preds;
}

std::vector<predicate> predicatizer::operator()(none) const {
  return {};
}

std::vector<predicate> predicatizer::operator()(conjunction const& con) const {
  std::vector<predicate> preds;
  for (auto& op : con) {
    auto ps = visit(*this, op);
    std::move(ps.begin(), ps.end(), std::back_inserter(preds));
  }
  return preds;
}

std::vector<predicate> predicatizer::operator()(negation const& n) const {
  return visit(*this, n.expr());
}

std::vector<predicate> predicatizer::operator()(predicate const& pred) const {
  return {pred};
}


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
  return visit(*this, n.expr());
}

maybe<void> validator::operator()(predicate const& p) const {
  auto valid = [&](predicate::operand const& lhs,
                   predicate::operand const& rhs) -> maybe<void> {
    // If a single-element key exists and matches a built-in type, the RHS
    // data must have the matching type.
    // TODO: we could provide the validator an entire schema for more detailed
    // AST analysis including custom types.
    if (auto k = get_if<key_extractor>(lhs)) {
      if (k->key.size() == 1) {
        // If we don't have data on the RHS, we cannot perform a type check.
        auto d = get_if<data>(rhs);
        if (!d)
          return {};
        // Try to parse the key type.
        if (auto t = to<type>(k->key[0]))
          if (!type_check(*t, *d))
            return fail<ec::type_clash>("invalid predicate: ",
                                        *t, ' ', p.op, ' ', *d);
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


key_resolver::key_resolver(type const& t) : type_{t} {
}

maybe<expression> key_resolver::operator()(none) {
  return expression{};
}

maybe<expression> key_resolver::operator()(conjunction const& c) {
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

maybe<expression> key_resolver::operator()(disjunction const& d) {
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


maybe<expression> key_resolver::operator()(negation const& n) {
  auto r = visit(*this, n.expr());
  if (!r)
    return r;
  else if (!is<none>(*r))
    return {negation{std::move(*r)}};
  else
    return expression{};
}

maybe<expression> key_resolver::operator()(predicate const& p) {
  op_ = p.op;
  return visit(*this, p.lhs, p.rhs);
}

maybe<expression> key_resolver::operator()(key_extractor const& e,
                                           data const& d) {
  disjunction dis;
  auto r = get_if<record_type>(type_);
  if (!r) {
    // If we're not dealing with a record, the only possible match is a
    // single-element key which represents the event name.
    if (e.key.size() == 1 && type_.name() == e.key[0])
      dis.emplace_back(predicate{data_extractor{type_, {}}, op_, d});
  } else {
    auto trace = r->find_suffix(e.key);
    if (trace.size() > 1) {
      // Make sure that all found keys resolve to arguments with the same type.
      auto first_type = r->at(trace.front().first);
      for (auto& p : trace)
        if (!p.first.empty())
          if (!congruent(*r->at(p.first), *first_type))
            return fail<ec::type_clash>(type_, *r->at(p.first));
    }
    // Add all offsets from the trace to the disjunction, which will
    // eventually replace this node.
    for (auto& p : trace)
      dis.emplace_back(
        predicate{data_extractor{type_, std::move(p.first)}, op_, d});
  }
  if (dis.empty())
    return expression{};
  else if (dis.size() == 1)
    return {std::move(dis[0])};
  else
    return {std::move(dis)};
}

maybe<expression> key_resolver::operator()(data const& d,
                                           key_extractor const& e) {
  return (*this)(e, d);
}


type_resolver::type_resolver(type const& event_type) : type_{event_type} {
}

expression type_resolver::operator()(none) {
  return {};
}

expression type_resolver::operator()(conjunction const& c) {
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

expression type_resolver::operator()(disjunction const& d) {
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

expression type_resolver::operator()(negation const& n) {
  auto e = visit(*this, n.expr());
  if (is<none>(e))
    return e;
  return negation{std::move(e)};
}

expression type_resolver::operator()(predicate const& p) {
  if (auto lhs = get_if<key_extractor>(p.lhs)) {
    // Try to parse a single key.
    if (lhs->key.size() == 1) {
      if (auto t = to<type>(lhs->key[0])) {
        if (auto r = get_if<record_type>(type_)) {
          disjunction result;
          for (auto& e : record_type::each{*r})
            if (congruent(e.trace.back()->type, *t)) {
              auto de = data_extractor{type_, e.offset};
              result.emplace_back(predicate{std::move(de), p.op, p.rhs});
            }
          if (result.empty())
            return {};
          if (result.size() == 1)
            return result[0];
          return result;
        } else if (congruent(type_, *t)) {
          return predicate{data_extractor{type_, {}}, p.op, p.rhs};
        }
      }
    }
    return {};
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
