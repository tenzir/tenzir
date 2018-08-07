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

#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"

namespace vast {
namespace {

struct adder {
  template <class T>
  std::enable_if_t<
    !(std::is_same<T, caf::none_t>{}
      || std::is_same<T, vector>{}
      || std::is_same<T, set>{})
  >
  operator()(caf::none_t, const T& x) const {
    self = x;
  }

  void operator()(caf::none_t, caf::none_t) const {
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
          || std::is_same<T, map>{})
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

} // namespace <anonymous>

data& data::operator+=(const data& rhs) {
  caf::visit(adder{*this}, *this, rhs);
  return *this;
}

bool operator==(const data& lhs, const data& rhs) {
  return lhs.data_ == rhs.data_;
}

bool operator<(const data& lhs, const data& rhs) {
  return lhs.data_ < rhs.data_;
}

bool evaluate(const data& lhs, relational_operator op, const data& rhs) {
  auto check_match = [](const auto& x, const auto& y) {
    return caf::visit(detail::overload(
      [](const auto&, const auto&) {
        return false;
      },
      [](const std::string& lhs, const pattern& rhs) {
        return rhs.match(lhs);
      }
    ), x, y);
  };
  auto check_in = [](const auto& x, const auto& y) {
    return caf::visit(detail::overload(
      [](const auto&, const auto&) {
        return false;
      },
      [](const std::string& lhs, const std::string& rhs) {
        return rhs.find(lhs) != std::string::npos;
      },
      [](const std::string& lhs, const pattern& rhs) {
        return rhs.search(lhs);
      },
      [](const address& lhs, const subnet& rhs) {
        return rhs.contains(lhs);
      },
      [](const subnet& lhs, const subnet& rhs) {
        return rhs.contains(lhs);
      },
      [](const auto& lhs, const vector& rhs) {
        return std::find(rhs.begin(), rhs.end(), lhs) != rhs.end();
      },
      [](const auto& lhs, const set& rhs) {
        return std::find(rhs.begin(), rhs.end(), lhs) != rhs.end();
      }
    ), x, y);
  };
  switch (op) {
    default:
      VAST_ASSERT(!"missing case");
      return false;
    case match:
      return check_match(lhs, rhs);
    case not_match:
      return !check_match(lhs, rhs);
    case in:
      return check_in(lhs, rhs);
    case not_in:
      return !check_in(lhs, rhs);
    case ni:
      return check_in(rhs, lhs);
    case not_ni:
      return !check_in(rhs, lhs);
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

bool is_basic(const data& x) {
  return caf::visit(detail::overload(
    [](const auto&) { return true; },
    [](const enumeration&) { return false; },
    [](const vector&) { return false; },
    [](const set&) { return false; },
    [](const map&) { return false; }
  ), x);
}

bool is_complex(const data& x) {
  return !is_basic(x);
}

bool is_recursive(const data& x) {
  return caf::visit(detail::overload(
    [](const auto&) { return false; },
    [](const vector&) { return true; },
    [](const set&) { return true; },
    [](const map&) { return true; }
  ), x);
}

bool is_container(const data& x) {
  return is_recursive(x);
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
    x = caf::get_if<vector>(d);
    if (!x)
      return nullptr;
  }
  return nullptr;
}

const data* get(const data& d, const offset& o) {
  if (auto v = caf::get_if<vector>(&d))
    return get(*v, o);
  return nullptr;
}

caf::optional<vector> flatten(const vector& xs, const record_type& t) {
  if (xs.size() != t.fields.size())
    return caf::none;
  vector result;
  for (size_t i = 0; i < xs.size(); ++i) {
    if (auto u = caf::get_if<record_type>(&t.fields[i].type)) {
      if (caf::holds_alternative<caf::none_t>(xs[i])) {
        result.reserve(result.size() + u->fields.size());
        for (size_t j = 0; j < u->fields.size(); ++j)
          result.emplace_back(caf::none);
      } else if (auto ys = caf::get_if<vector>(&xs[i])) {
        auto sub_result = flatten(*ys, *u);
        if (!sub_result)
          return caf::none;
        result.insert(result.end(),
                      std::make_move_iterator(sub_result->begin()),
                      std::make_move_iterator(sub_result->end()));
      } else {
        return caf::none;
      }
    } else {
      // We could perform another type-check here, but that's technically
      // orthogonal to the objective of this function.
      result.emplace_back(xs[i]);
    }
  }
  return result;
}

caf::optional<data> flatten(const data& x, type t) {
  auto xs = caf::get_if<vector>(&x);
  auto r = caf::get_if<record_type>(&t);
  if (xs && r)
    return flatten(*xs, *r);
  return caf::none;
}

namespace {

bool consume(const record_type& t, const vector& xs, size_t& i, vector& res) {
  for (auto& field : t.fields) {
    if (auto u = caf::get_if<record_type>(&field.type)) {
      vector sub_res;
      if (!consume(*u, xs, i, sub_res))
        return false;
      auto is_none = [](const auto& x) {
        return caf::holds_alternative<caf::none_t>(x);
      };
      if (std::all_of(sub_res.begin(), sub_res.end(), is_none))
        res.emplace_back(caf::none);
      else
        res.emplace_back(std::move(sub_res));
    } else if (i == xs.size()) {
      return false; // too few elements in vector
    } else {
      res.emplace_back(xs[i++]);
    }
  }
  return true;
}

} // namespace <anonymous>

caf::optional<vector> unflatten(const vector& xs, const record_type& t) {
  vector result;
  size_t i = 0;
  if (consume(t, xs, i, result))
    return result;
  return caf::none;
}

caf::optional<data> unflatten(const data& x, type t) {
  auto xs = caf::get_if<vector>(&x);
  auto r = caf::get_if<record_type>(&t);
  if (xs && r)
    return unflatten(*xs, *r);
  return caf::none;
}

namespace {

json jsonize(const data& x) {
  return caf::visit(detail::overload(
    [&](const auto& y) { return to_json(y); },
    [&](caf::none_t) { return json{}; },
    [&](const std::string& str) { return json{str}; }
  ), x);
}

} // namespace <anonymous>

bool convert(const vector& v, json& j) {
  json::array a(v.size());
  for (size_t i = 0; i < v.size(); ++i)
    a[i] = jsonize(v[i]);
  j = std::move(a);
  return true;
}

bool convert(const set& s, json& j) {
  json::array a(s.size());
  size_t i = 0;
  for (auto& x : s)
    a[i++] = jsonize(x);
  j = std::move(a);
  return true;
}

bool convert(const map& t, json& j) {
  json::array values;
  for (auto& p : t) {
    json::array a;
    a.emplace_back(jsonize(p.first));
    a.emplace_back(jsonize(p.second));
    values.emplace_back(std::move(a));
  };
  j = std::move(values);
  return true;
}

bool convert(const data& d, json& j) {
  j = jsonize(d);
  return true;
}

bool convert(const data& d, json& j, const type& t) {
  auto v = caf::get_if<vector>(&d);
  auto rt = caf::get_if<record_type>(&t);
  if (v && rt) {
    if (v->size() != rt->fields.size())
      return false;
    json::object o;
    for (size_t i = 0; i < v->size(); ++i) {
      auto& f = rt->fields[i];
      if (!convert((*v)[i], o[f.name], f.type))
        return false;
    }
    j = std::move(o);
    return true;
  }
  return convert(d, j);
}

} // namespace vast
