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

#include "vast/data.hpp"
#include "vast/json.hpp"

#include "vast/concept/printable/vast/data.hpp"
#include "vast/detail/assert.hpp"

namespace vast {
namespace {

struct adder {
  template <class T>
  std::enable_if_t<
    !(std::is_same<T, none>{}
      || std::is_same<T, vector>{}
      || std::is_same<T, set>{})
  >
  operator()(none, const T& x) const {
    self = x;
  }

  void operator()(none, none) const {
    // nop
  }

  template <class T>
  std::enable_if_t<
    !(std::is_same<T, vector>{} || std::is_same<T, set>{})
      && (std::is_same<T, boolean>{}
           || std::is_same<T, integer>{}
           || std::is_same<T, count>{}
           || std::is_same<T, real>{}
           || std::is_same<T, timespan>{}
           || std::is_same<T, std::string>{})
  >
  operator()(T& x, const T& y) const {
    x += y;
  }

  template <class T, class U>
  std::enable_if_t<
    !(std::is_same<T, U>{} || std::is_same<T, vector>{}
                           || std::is_same<T, set>{})
      && (std::is_same<T, boolean>{}
          || std::is_same<T, integer>{}
          || std::is_same<T, count>{}
          || std::is_same<T, real>{}
          || std::is_same<T, timespan>{}
          || std::is_same<T, std::string>{})
  >
  operator()(T&, const U&) const {
    // impossible
  }

  void operator()(timestamp&, timestamp) const {
    // impossible
  }

  void operator()(timestamp& ts, timespan x) const {
    ts += x;
  }

  template <class T>
  std::enable_if_t<
    !(std::is_same<T, timestamp>{}
      || std::is_same<T, timespan>{}
      || std::is_same<T, vector>{}
      || std::is_same<T, set>{})
  >
  operator()(timestamp&, const T&) const {
    // impossible
  }

  template <class T, class U>
  std::enable_if_t<
    !(std::is_same<U, vector>{} || std::is_same<U, set>{})
      && (std::is_same<T, pattern>{}
          || std::is_same<T, address>{}
          || std::is_same<T, subnet>{}
          || std::is_same<T, port>{}
          || std::is_same<T, enumeration>{}
          || std::is_same<T, table>{})
  >
  operator()(T&, const U&) const {
    // impossible
  }

  template <class T>
  std::enable_if_t<
    !(std::is_same<T, vector>{} || std::is_same<T, set>{})
  >
  operator()(vector& lhs, const T& rhs) const {
    lhs.emplace_back(rhs);
  }

  void operator()(vector& lhs, const vector& rhs) const {
    std::copy(rhs.begin(), rhs.end(), std::back_inserter(lhs));
  }

  template <class T>
  std::enable_if_t<
    !(std::is_same<T, vector>{} || std::is_same<T, set>{})
  >
  operator()(set& lhs, const T& rhs) const {
    lhs.emplace(rhs);
  }

  void operator()(set& lhs, const set& rhs) const {
    std::copy(rhs.begin(), rhs.end(), std::inserter(lhs, lhs.end()));
  }

  template <class T>
  std::enable_if_t<!std::is_same<T, vector>{}>
  operator()(T&, const vector& rhs) const {
    vector v;
    v.reserve(rhs.size() + 1);
    v.push_back(std::move(self));
    std::copy(rhs.begin(), rhs.end(), std::back_inserter(v));
    self = std::move(v);
  }

  template <class T>
  std::enable_if_t<!std::is_same<T, set>{}>
  operator()(T&, const set& rhs) const {
    set s;
    s.insert(std::move(self));
    std::copy(rhs.begin(), rhs.end(), std::inserter(s, s.end()));
    self = std::move(s);
  }

  data& self;
};

struct match_visitor {
  bool operator()(const std::string& lhs, const pattern& rhs) const {
    return rhs.match(lhs);
  }

  template <class T, class U>
  bool operator()(const T&, const U&) const {
    return false;
  }
};

struct in_visitor {
  bool operator()(const std::string& lhs, const std::string& rhs) const {
    return rhs.find(lhs) != std::string::npos;
  }

  bool operator()(const std::string& lhs, const pattern& rhs) const {
    return rhs.search(lhs);
  }

  bool operator()(const address& lhs, const subnet& rhs) const {
    return rhs.contains(lhs);
  }

  bool operator()(const subnet& lhs, const subnet& rhs) const {
    return rhs.contains(lhs);
  }

  template <class T>
  bool operator()(const T& lhs, const set& rhs) const {
    return std::find(rhs.begin(), rhs.end(), lhs) != rhs.end();
  }

  template <class T>
  bool operator()(const T& lhs, const vector& rhs) const {
    return std::find(rhs.begin(), rhs.end(), lhs) != rhs.end();
  }

  template <class T, class U>
  bool operator()(const T&, const U&) const {
    return false;
  }
};

} // namespace <anonymous>

data::data(none) {
}

data& data::operator+=(const data& rhs) {
  visit(adder{*this}, *this, rhs);
  return *this;
}

detail::data_variant& expose(data& d) {
  return d.data_;
}

bool operator==(const data& lhs, const data& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator<(const data& lhs, const data& rhs) {
  return lhs.data_ < rhs.data_;
}

bool evaluate(const data& lhs, relational_operator op, const data& rhs) {
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

const data* get(const vector& v, const offset& o) {
  const vector* x = &v;
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

const data* get(const data& d, const offset& o) {
  if (auto v = get_if<vector>(d))
    return get(*v, o);
  return nullptr;
}

namespace {

template <class Iterator>
vector flatten(Iterator& f, Iterator l) {
  vector xs;
  xs.reserve(l - f);
  for (; f != l; ++f)
    if (auto v = get_if<vector>(*f)) {
      auto begin = Iterator{v->begin()};
      auto end = Iterator{v->end()};
      auto ys = flatten(begin, end);
      xs.insert(xs.end(),
                std::make_move_iterator(ys.begin()),
                std::make_move_iterator(ys.end()));
    } else {
      xs.push_back(*f);
    }
  return xs;
}

template <class Iterator>
optional<vector> unflatten(Iterator& f, Iterator l, const record_type& rec) {
  vector xs;
  xs.reserve(rec.fields.size());
  for (auto& field : rec.fields)
    if (f == l) {
      return {};
    } else if (auto rt = get_if<record_type>(field.type)) {
      auto ys = unflatten(f, l, *rt);
      if (!ys)
        return ys;
      xs.push_back(std::move(*ys));
    } else {
      xs.push_back(*f++);
    }
  return xs;
}

} // namespace <anonymous>

vector flatten(const vector& xs) {
  auto f = xs.begin();
  auto l = xs.end();
  return flatten(f, l);
}

vector flatten(vector&& xs) {
  auto f = std::make_move_iterator(xs.begin());
  auto l = std::make_move_iterator(xs.end());
  return flatten(f, l);
}

data flatten(const data& x) {
  auto xs = get_if<vector>(x);
  return xs ? flatten(*xs) : x;
}

data flatten(data&& x) {
  auto xs = get_if<vector>(x);
  return xs ? flatten(std::move(*xs)) : x;
}

optional<vector> unflatten(const vector& xs, const record_type& rt) {
  auto first = xs.begin();
  auto last = xs.end();
  return unflatten(first, last, rt);
}

optional<vector> unflatten(vector&& xs, const record_type& rt) {
  auto first = std::make_move_iterator(xs.begin());
  auto last = std::make_move_iterator(xs.end());
  return unflatten(first, last, rt);
}

optional<vector> unflatten(const data& x, const type& t) {
  auto xs = get_if<vector>(x);
  auto rt = get_if<record_type>(t);
  return xs && rt ? unflatten(*xs, *rt) : optional<vector>{};
}

optional<vector> unflatten(data&& x, const type& t) {
  auto xs = get_if<vector>(x);
  auto rt = get_if<record_type>(t);
  return xs && rt ? unflatten(std::move(*xs), *rt) : optional<vector>{};
}

namespace {

struct jsonizer {
  jsonizer(json& j) : j_{j} { }

  bool operator()(none) const {
    return true;
  }

  bool operator()(const std::string& str) const {
    j_ = str;
    return true;
  }

  template <class T>
  bool operator()(const T& x) const {
    return convert(x, j_);
  }

  json& j_;
};

} // namespace <anonymous>

bool convert(const vector& v, json& j) {
  json::array a(v.size());
  for (auto i = 0u; i < v.size(); ++i)
    if (!visit(jsonizer{a[i]}, v[i]))
      return false;
  j = std::move(a);
  return true;
}

bool convert(const set& s, json& j) {
  json::array a(s.size());
  auto i = 0u;
  for (auto& x : s)
    if (!visit(jsonizer{a[i++]}, x))
      return false;
  j = std::move(a);
  return true;
}

bool convert(const table& t, json& j) {
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

bool convert(const data& d, json& j) {
  return visit(jsonizer{j}, d);
}

bool convert(const data& d, json& j, const type& t) {
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
