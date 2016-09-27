#include "vast/data.hpp"
#include "vast/json.hpp"

#include "vast/concept/printable/vast/data.hpp"
#include "vast/detail/assert.hpp"

namespace vast {

data::data(none) {
}

data const* at(offset const& o, vector const& v) {
  vector const* x = &v;
  for (size_t i = 0; i < o.size(); ++i) {
    auto& idx = o[i];
    if (idx >= x->size())
      return nullptr;
    auto d = &(*x)[idx];
    if (i + 1 == o.size())
      return d;
    x = get_if<vector>(*d);
    if (!x)
      return nullptr;
  }
  return nullptr;
}

vector flatten(vector const& v) {
  vector result;
  result.reserve(v.size());
  for (auto& x : v)
    if (auto rec = get_if<vector>(x)) {
      auto flat = flatten(*rec);
      result.insert(result.end(),
                    std::make_move_iterator(flat.begin()),
                    std::make_move_iterator(flat.end()));
    } else {
      result.push_back(x);
    }
  return result;
}

data flatten(data const& d) {
  auto v = get_if<vector>(d);
  return v ? flatten(*v) : d;
}

optional<vector> unflatten(vector const& v, record_type const& t) {
  auto i = v.begin();
  size_t depth = 1;
  vector result;
  vector* x = &result;
  for (auto& e : record_type::each{t}) {
    if (i == v.end())
      return {};
    if (e.depth() > depth) {
      for (size_t j = 0; j < e.depth() - depth; ++j) {
        ++depth;
        x->push_back(vector{});
        x = get_if<vector>(x->back());
      }
    } else if (e.depth() < depth) {
      x = &result;
      depth = e.depth();
      for (size_t j = 0; j < depth - 1; ++j)
        x = get_if<vector>(x->back());
    }
    auto& field_type = e.trace.back()->type;
    if (is<none>(*i) || type_check(field_type, *i))
      x->push_back(*i++);
    else
      return {};
  }
  return result;
}

optional<vector> unflatten(data const& d, type const& t) {
  auto v = get_if<vector>(d);
  auto rt = get_if<record_type>(t);
  return v && rt ? unflatten(*v, *rt) : optional<vector>{};
}

detail::data_variant& expose(data& d) {
  return d.data_;
}

bool operator==(data const& lhs, data const& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator<(data const& lhs, data const& rhs) {
  return lhs.data_ < rhs.data_;
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

bool evaluate(data const& lhs, relational_operator op, data const& rhs) {
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

namespace {

struct jsonizer {
  jsonizer(json& j) : j_{j} { }

  bool operator()(none) const {
    return true;
  }

  bool operator()(std::string const& str) const {
    j_ = str;
    return true;
  }

  template <typename T>
  bool operator()(T const& x) const {
    return convert(x, j_);
  }

  json& j_;
};

} // namespace <anonymous>

bool convert(vector const& v, json& j) {
  json::array a(v.size());
  for (auto i = 0u; i < v.size(); ++i)
    if (!visit(jsonizer{a[i]}, v[i]))
      return false;
  j = std::move(a);
  return true;
}

bool convert(set const& s, json& j) {
  json::array a(s.size());
  for (auto i = 0u; i < s.size(); ++i)
    if (!visit(jsonizer{a[i]}, s[i]))
      return false;
  j = std::move(a);
  return true;
}

bool convert(table const& t, json& j) {
  json::array values;
  for (auto& p : t) {
    json::array a;
    json j;
    if (!visit(jsonizer{j}, p.first))
      return false;
    a.push_back(std::move(j));
    if (!visit(jsonizer{j}, p.second))
      return false;
    a.push_back(std::move(j));
    values.emplace_back(std::move(a));
  };
  j = std::move(values);
  return true;
}

bool convert(data const& d, json& j) {
  return visit(jsonizer{j}, d);
}

bool convert(data const& d, json& j, type const& t) {
  auto v = get_if<vector>(d);
  auto rt = get_if<record_type>(t);
  if (v && rt) {
    if (v->size() != rt->fields.size())
      return false;
    json::object o;
    for (auto i = 0u; i < v->size(); ++i) {
      auto& f = rt->fields[i];
      if (! convert((*v)[i], o[f.name], f.type))
        return false;
    }
    j = std::move(o);
    return true;
  }
  return convert(d, j);
}

} // namespace vast
