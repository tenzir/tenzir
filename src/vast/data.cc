#include "vast/data.h"
#include "vast/concept/printable/vast/data.h"
#include "vast/util/assert.h"

namespace vast {

data const& record::each::range_state::data() const {
  VAST_ASSERT(!trace.empty());
  return *trace.back();
}

record::each::each(record const& r) {
  if (r.empty())
    return;
  auto rec = &r;
  do {
    records_.push_back(rec);
    state_.trace.push_back(&(*rec)[0]);
    state_.offset.push_back(0);
  } while ((rec = get<record>(*state_.trace.back())));
}

bool record::each::next() {
  if (records_.empty())
    return false;
  while (++state_.offset.back() == records_.back()->size()) {
    records_.pop_back();
    state_.trace.pop_back();
    state_.offset.pop_back();
    if (records_.empty())
      return false;
  }
  auto f = &(*records_.back())[state_.offset.back()];
  state_.trace.back() = f;
  while (auto r = get<record>(*f)) {
    f = &(*r)[0];
    records_.push_back(r);
    state_.trace.push_back(f);
    state_.offset.push_back(0);
  }
  return true;
}

data const* record::at(offset const& o) const {
  record const* r = this;
  for (size_t i = 0; i < o.size(); ++i) {
    auto& idx = o[i];
    if (idx >= r->size())
      return nullptr;
    auto v = &(*r)[idx];
    if (i + 1 == o.size())
      return v;
    r = get<record>(*v);
    if (!r)
      return nullptr;
  }
  return nullptr;
}

trial<record> record::unflatten(type::record const& t) const {
  auto i = begin();
  size_t depth = 1;
  record result;
  record* r = &result;
  for (auto& e : type::record::each{t}) {
    if (i == end())
      return error{"not enough data"};
    if (e.depth() > depth) {
      for (size_t j = 0; j < e.depth() - depth; ++j) {
        ++depth;
        r->push_back(record{});
        r = get<record>(r->back());
      }
    } else if (e.depth() < depth) {
      r = &result;
      depth = e.depth();
      for (size_t j = 0; j < depth - 1; ++j)
        r = get<record>(r->back());
    }
    auto& field_type = e.trace.back()->type;
    if (is<none>(*i) || field_type.check(*i))
      r->push_back(*i++);
    else
      return error{"data/type mismatch: ", *i, '/', field_type};
  }
  return std::move(result);
}

namespace {

struct match_visitor {
  bool operator()(std::string const& lhs, pattern const& rhs) const {
    return rhs.match(lhs);
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) const {
    return false;
  }
};

struct in_visitor {
  bool operator()(std::string const& lhs, std::string const& rhs) const {
    return rhs.find(lhs) != std::string::npos;
  }

  bool operator()(std::string const& lhs, pattern const& rhs) const {
    return rhs.search(lhs);
  }

  bool operator()(address const& lhs, subnet const& rhs) const {
    return rhs.contains(lhs);
  }

  template <typename T>
  bool operator()(T const& lhs, set const& rhs) const {
    return std::find(rhs.begin(), rhs.end(), lhs) != rhs.end();
  }

  template <typename T>
  bool operator()(T const& lhs, vector const& rhs) const {
    return std::find(rhs.begin(), rhs.end(), lhs) != rhs.end();
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) const {
    return false;
  }
};

} // namespace <anonymous>

bool data::evaluate(data const& lhs, relational_operator op, data const& rhs) {
  switch (op) {
    default:
      VAST_ASSERT(!"missing case");
      return false;
    case match:
      return visit(match_visitor{}, lhs, rhs);
    case not_match:
      return !visit(match_visitor{}, lhs, rhs);
    case in:
      return visit(in_visitor{}, lhs, rhs);
    case not_in:
      return !visit(in_visitor{}, lhs, rhs);
    case ni:
      return visit(in_visitor{}, rhs, lhs);
    case not_ni:
      return !visit(in_visitor{}, rhs, lhs);
    case equal:
      return lhs == rhs;
    case not_equal:
      return lhs != rhs;
    case less:
      return lhs < rhs;
    case less_equal:
      return lhs <= rhs;
    case greater:
      return lhs > rhs;
    case greater_equal:
      return lhs >= rhs;
  }
}

data::variant_type& expose(data& d) {
  return d.data_;
}

data::variant_type const& expose(data const& d) {
  return d.data_;
}

bool operator==(data const& lhs, data const& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator<(data const& lhs, data const& rhs) {
  return lhs.data_ < rhs.data_;
}

} // namespace vast
