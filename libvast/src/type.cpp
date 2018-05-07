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

#include <tuple>
#include <utility>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/data.hpp"
#include "vast/json.hpp"
#include "vast/pattern.hpp"
#include "vast/type.hpp"
#include "vast/schema.hpp"

namespace vast {

type::type() : ptr_{new impl} {
}

type& type::name(std::string str) {
  visit([s=std::move(str)](auto& x) { x.name(std::move(s)); }, *this);
  return *this;
}

const std::string& type::name() const {
  return *visit([](auto& x) { return &x.name(); }, *this);
}

std::vector<attribute>& type::attributes() {
  return *visit([](auto& x) { return &x.attributes(); }, *this);
}

const std::vector<attribute>& type::attributes() const {
  return *visit([](auto& x) { return &x.attributes(); }, *this);
}

type& type::attributes(std::initializer_list<attribute> list) {
  attributes() = std::move(list);
  return *this;
}

namespace {

struct equal_to {
  template <class T, class U>
  bool operator()(const T&, const U&) const noexcept {
    return false;
  }

  template <class T>
  bool operator()(const T& x, const T& y) const noexcept {
    return x == y;
  }
};

struct less_than {
  template <class T, class U>
  bool operator()(const T&, const U&) const noexcept {
    return false;
  }

  template <class T>
  bool operator()(const T& x, const T& y) const noexcept {
    return x < y;
  }
};

} // namespace

bool operator==(const type& x, const type& y) {
  return visit(equal_to{}, x, y);
}

bool operator<(const type& x, const type& y) {
  return visit(less_than{}, x, y);
}

enumeration_type::enumeration_type(std::vector<std::string> fields)
  : fields{std::move(fields)} {
}

bool operator==(const enumeration_type& x, const enumeration_type& y) {
  return static_cast<const enumeration_type::base_type&>(x) ==
         static_cast<const enumeration_type::base_type&>(y) &&
           x.fields == y.fields;
}

bool operator<(const enumeration_type& x, const enumeration_type& y) {
  return x.fields < y.fields;
}

vector_type::vector_type(type t) : value_type{std::move(t)} {
}

bool operator==(const vector_type& x, const vector_type& y) {
  return static_cast<const vector_type::base_type&>(x) ==
         static_cast<const vector_type::base_type&>(y) &&
    x.value_type == y.value_type;
}

bool operator<(const vector_type& x, const vector_type& y) {
  return x.value_type < y.value_type;
}

set_type::set_type(type t) : value_type{std::move(t)} {
}

bool operator==(const set_type& x, const set_type& y) {
  return static_cast<const set_type::base_type&>(x) ==
    static_cast<const set_type::base_type&>(y) &&
    x.value_type == y.value_type;
}

bool operator<(const set_type& x, const set_type& y) {
  return x.value_type < y.value_type;
}

table_type::table_type(type key, type value)
  : key_type{std::move(key)},
    value_type{std::move(value)} {
}

bool operator==(const table_type& x, const table_type& y) {
  return static_cast<const table_type::base_type&>(x) ==
         static_cast<const table_type::base_type&>(y) &&
    x.key_type == y.key_type && x.value_type == y.value_type;
}

bool operator<(const table_type& x, const table_type& y) {
  return std::tie(x.key_type, x.value_type) <
    std::tie(y.key_type, y.value_type);
}

record_field::record_field(std::string name, vast::type type)
  : name{std::move(name)},
    type{std::move(type)} {
}

bool operator==(const record_field& x, const record_field& y) {
  return x.name == y.name && x.type == y.type;
}

bool operator<(const record_field& x, const record_field& y) {
  return std::tie(x.name, x.type) < std::tie(y.name, y.type);
}

record_type::record_type(std::vector<record_field> fields)
  : fields{std::move(fields)} {
}

record_type::record_type(std::initializer_list<record_field> list)
  : fields{std::move(list)} {
}

key record_type::each::range_state::key() const {
  vast::key k(trace.size());
  for (size_t i = 0; i < trace.size(); ++i)
    k[i] = trace[i]->name;
  return k;
}

size_t record_type::each::range_state::depth() const {
  return trace.size();
}

record_type::each::each(const record_type& r) {
  if (r.fields.empty())
    return;
  auto rec = &r;
  do {
    records_.push_back(rec);
    state_.trace.push_back(&rec->fields[0]);
    state_.offset.push_back(0);
  } while ((rec = get_if<record_type>(state_.trace.back()->type)));
}

void record_type::each::next() {
  while (++state_.offset.back() == records_.back()->fields.size()) {
    records_.pop_back();
    state_.trace.pop_back();
    state_.offset.pop_back();
    if (records_.empty())
      return;
  }
  auto f = &records_.back()->fields[state_.offset.back()];
  state_.trace.back() = f;
  while (auto r = get_if<record_type>(f->type)) {
    f = &r->fields[0];
    records_.emplace_back(r);
    state_.trace.push_back(f);
    state_.offset.push_back(0);
  }
}

bool record_type::each::done() const {
  return records_.empty();
}

const record_type::each::range_state& record_type::each::get() const {
  return state_;
}

expected<offset> record_type::resolve(const key& k) const {
  if (k.empty())
    return make_error(ec::unspecified, "empty symbol sequence");
  offset off;
  auto found = true;
  auto rec = this;
  for (auto id = k.begin(); id != k.end() && found; ++id) {
    found = false;
    for (size_t i = 0; i < rec->fields.size(); ++i) {
      if (rec->fields[i].name == *id) {
        // If the name matches, we have to check whether we're continuing with
        // an intermediate record or have reached the last symbol.
        rec = get_if<record_type>(rec->fields[i].type);
        if (!(rec || id + 1 == k.end()))
          return make_error(ec::unspecified,
                            "intermediate fields must be records");
        off.push_back(i);
        found = true;
        break;
      }
    }
  }
  if (!found)
    return make_error(ec::unspecified, "non-existant field name");
  return off;
}

expected<key> record_type::resolve(const offset& o) const {
  if (o.empty())
    return make_error(ec::unspecified, "empty offset sequence");
  key k;
  auto r = this;
  for (size_t i = 0; i < o.size(); ++i) {
    if (o[i] >= r->fields.size())
      return make_error(ec::unspecified, "offset index ", i, " out of bounds");
    k.push_back(r->fields[o[i]].name);
    if (i != o.size() - 1) {
      r = get_if<record_type>(r->fields[o[i]].type);
      if (!r)
        return make_error(ec::unspecified,
                          "intermediate fields must be records");
    }
  }
  return k;
}

namespace {

struct finder {
  enum mode { prefix, suffix, exact, any };

  finder(key  k, mode m, const std::string& init = "")
    : mode_{m}, key_{std::move(k)} {
    VAST_ASSERT(!key_.empty());
    if (!init.empty())
      trace_.push_back(init);
  }

  template <class T>
  std::vector<std::pair<offset, key>> operator()(const T&) const {
    std::vector<std::pair<offset, key>> r;
    if (off_.empty() || key_.size() > trace_.size())
      return r;
    if (mode_ == prefix || mode_ == exact) {
      if (mode_ == exact && key_.size() != trace_.size())
        return r;
      for (size_t i = 0; i < key_.size(); ++i)
        if (!match(key_[i], trace_[i]))
          return r;
    } else if (mode_ == suffix) {
      for (size_t i = 0; i < key_.size(); ++i)
        if (!match(key_[i], trace_[i + trace_.size() - key_.size()]))
          return r;
    } else {
      for (size_t run = 0; run < trace_.size() - key_.size(); ++run) {
        auto found = true;
        for (size_t i = 0; i < key_.size(); ++i)
          if (!match(key_[i], trace_[i + run])) {
            found = false;
            break;
          }
        if (found)
          break;
      }
      return r;
    }
    r.emplace_back(off_, trace_);
    return r;
  }

  std::vector<std::pair<offset, key>> operator()(const record_type& r) {
    std::vector<std::pair<offset, key>> result;
    off_.push_back(0);
    for (auto& f : r.fields) {
      trace_.push_back(f.name);
      for (auto& p : visit(*this, f.type))
        result.push_back(std::move(p));
      trace_.pop_back();
      ++off_.back();
    }
    off_.pop_back();
    return result;
  }

  static bool match(const std::string& key, const std::string& trace) {
    return pattern::glob(key).match(trace);
  }

  mode mode_;
  key key_;
  key trace_;
  offset off_;
};

} // namespace <anonymous>

std::vector<std::pair<offset, key>> record_type::find(const key& k) const {
  return finder{k, finder::exact, name()}(*this);
}

std::vector<std::pair<offset, key>>
record_type::find_prefix(const key& k) const {
  return finder{k, finder::prefix, name()}(*this);
}

std::vector<std::pair<offset, key>>
record_type::find_suffix(const key& k) const {
  return finder{k, finder::suffix, name()}(*this);
}

const type* record_type::at(const key& k) const {
  auto r = this;
  for (size_t i = 0; i < k.size(); ++i) {
    auto& id = k[i];
    const record_field* f = nullptr;
    for (auto& a : r->fields)
      if (a.name == id) {
        f = &a;
        break;
      }
    if (!f)
      return nullptr;
    if (i + 1 == k.size())
      return &f->type;
    r = get_if<record_type>(f->type);
    if (!r)
      return nullptr;
  }
  return nullptr;
}

const type* record_type::at(const offset& o) const {
  auto r = this;
  for (size_t i = 0; i < o.size(); ++i) {
    auto& idx = o[i];
    if (idx >= r->fields.size())
      return nullptr;
    auto t = &r->fields[idx].type;
    if (i + 1 == o.size())
      return t;
    r = get_if<record_type>(*t);
    if (!r)
      return nullptr;
  }
  return nullptr;
}

record_type flatten(const record_type& rec) {
  record_type result;
  for (auto& outer : rec.fields)
    if (auto r = get_if<record_type>(outer.type)) {
      auto flat = flatten(*r);
      for (auto& inner : flat.fields)
        result.fields.emplace_back(outer.name + "." + inner.name, inner.type);
    } else {
      result.fields.push_back(outer);
    }
  return result;
}

type flatten(const type& t) {
  auto r = get_if<record_type>(t);
  return r ? flatten(*r) : t;
}

record_type unflatten(const record_type& rec) {
  record_type result;
  for (auto& f : rec.fields) {
    auto names = detail::split(f.name, ".");
    VAST_ASSERT(!names.empty());
    record_type* r = &result;
    for (size_t i = 0; i < names.size() - 1; ++i) {
      if (r->fields.empty() || r->fields.back().name != names[i])
        r->fields.emplace_back(std::string(names[i]), record_type{});
      r = get_if<record_type>(r->fields.back().type);
    }
    r->fields.emplace_back(std::string{names.back()}, f.type);
  }
  std::vector<std::vector<record_type*>> rs(1);
  rs.back().push_back(&result);
  auto more = true;
  while (more) {
    std::vector<record_type*> next;
    for (auto& current : rs.back())
      for (auto& f : current->fields)
        if (auto r = get_if<record_type>(f.type))
          next.push_back(r);
    if (next.empty())
      more = false;
    else
      rs.push_back(std::move(next));
  }
  return result;
}

type unflatten(const type& t) {
  auto r = get_if<record_type>(t);
  return r ? unflatten(*r) : t;
}

bool operator==(const record_type& x, const record_type& y) {
  return static_cast<const record_type::base_type&>(x) ==
         static_cast<const record_type::base_type&>(y) &&
    x.fields == y.fields;
}

bool operator<(const record_type& x, const record_type& y) {
  return x.fields < y.fields;
}

alias_type::alias_type(type t) : value_type{std::move(t)} {
}

bool operator==(const alias_type& x, const alias_type& y) {
  return static_cast<const alias_type::base_type&>(x) ==
         static_cast<const alias_type::base_type&>(y) &&
    x.value_type == y.value_type;
}

bool operator<(const alias_type& x, const alias_type& y) {
  return x.value_type < y.value_type;
}

bool is_recursive(const type& t) {
  return expose(t).index() >= 13;
}

bool is_container(const type& t) {
  auto i = expose(t).index();
  return i >= 13 && i <= 16;
}

bool is_container(const data& x) {
  return is<vector>(x) || is<set>(x) || is<table>(x);
}

namespace {

struct type_congruence_checker {
  template <class T>
  bool operator()(const T&, const T&) const {
    return true;
  }

  template <class T, class U>
  bool operator()(const T&, const U&) const {
    return false;
  }

  template <class T>
  bool operator()(const T& x, const alias_type& a) const {
    using namespace std::placeholders;
    return visit(std::bind(std::cref(*this), std::cref(x), _1), a.value_type);
  }

  template <class T>
  bool operator()(const alias_type& a, const T& x) const {
    return (*this)(x, a);
  }

  bool operator()(const alias_type& x, const alias_type& y) const {
    return visit(*this, x.value_type, y.value_type);
  }

  bool operator()(const enumeration_type& x, const enumeration_type& y) const {
    return x.fields.size() == y.fields.size();
  }

  bool operator()(const vector_type& x, const vector_type& y) const {
    return visit(*this, x.value_type, y.value_type);
  }

  bool operator()(const set_type& x, const set_type& y) const {
    return visit(*this, x.value_type, y.value_type);
  }

  bool operator()(const table_type& x, const table_type& y) const {
    return visit(*this, x.key_type, y.key_type) &&
        visit(*this, x.value_type, y.value_type);
  }

  bool operator()(const record_type& x, const record_type& y) const {
    if (x.fields.size() != y.fields.size())
      return false;
    for (size_t i = 0; i < x.fields.size(); ++i)
      if (!visit(*this, x.fields[i].type, y.fields[i].type))
        return false;
    return true;
  }
};

struct data_congruence_checker {
  template <class T, class U>
  bool operator()(const T&, const U&) const {
    return false;
  }

  bool operator()(const none_type&, none) const {
    return true;
  }

  bool operator()(const boolean_type&, boolean) const {
    return true;
  }

  bool operator()(const integer_type&, integer) const {
    return true;
  }

  bool operator()(const count_type&, count) const {
    return true;
  }

  bool operator()(const real_type&, real) const {
    return true;
  }

  bool operator()(const timespan_type&, timespan) const {
    return true;
  }

  bool operator()(const timestamp_type&, timestamp) const {
    return true;
  }

  bool operator()(const string_type&, const std::string&) const {
    return true;
  }

  bool operator()(const pattern_type&, const pattern&) const {
    return true;
  }

  bool operator()(const address_type&, const address&) const {
    return true;
  }

  bool operator()(const subnet_type&, const subnet&) const {
    return true;
  }

  bool operator()(const port_type&, const port&) const {
    return true;
  }

  bool operator()(const enumeration_type& x, const std::string& y) const {
    return std::find(x.fields.begin(), x.fields.end(), y) != x.fields.end();
  }

  bool operator()(const vector_type&, const vector&) const {
    return true;
  }

  bool operator()(const set_type&, const set&) const {
    return true;
  }

  bool operator()(const table_type&, const table&) const {
    return true;
  }

  bool operator()(const record_type& x, const vector& y) const {
    if (x.fields.size() != y.size())
      return false;
    for (size_t i = 0; i < x.fields.size(); ++i)
      if (!visit(*this, x.fields[i].type, y[i]))
        return false;
    return true;
  }

  template <class T>
  bool operator()(const alias_type& t, const T& x) const {
    using namespace std::placeholders;
    return visit(std::bind(std::cref(*this), _1, std::cref(x)), t.value_type);
  }
};

} // namespace <anonymous>

bool congruent(const type& x, const type& y) {
  return visit(type_congruence_checker{}, x, y);
}

bool congruent(const type& x, const data& y) {
  return visit(data_congruence_checker{}, x, y);
}

bool congruent(const data& x, const type& y) {
  return visit(data_congruence_checker{}, y, x);
}

expected<void> replace_if_congruent(std::initializer_list<type*> xs,
                                    const schema& with) {
  for (auto x : xs)
    if (auto t = with.find(x->name()); t != nullptr) {
      if (!congruent(*x, *t))
        return make_error(ec::type_clash, "incongruent type:", x->name());
      *x = *t;
    }
  return no_error;
}

bool compatible(const type& lhs, relational_operator op, const type& rhs) {
  switch (op) {
    default:
      return false;
    case match:
    case not_match:
      return is<string_type>(lhs) && is<pattern_type>(rhs);
    case equal:
    case not_equal:
      return is<none_type>(lhs) || is<none_type>(rhs) || congruent(lhs, rhs);
    case less:
    case less_equal:
    case greater:
    case greater_equal:
      return congruent(lhs, rhs);
    case in:
    case not_in:
      if (is<string_type>(lhs))
        return is<string_type>(rhs) || is_container(rhs);
      else if (is<address_type>(lhs) || is<subnet_type>(lhs))
        return is<subnet_type>(rhs) || is_container(rhs);
      else
        return is_container(rhs);
    case ni:
      return compatible(rhs, in, lhs);
    case not_ni:
      return compatible(rhs, not_in, lhs);
  }
}

bool compatible(const type& lhs, relational_operator op, const data& rhs) {
  switch (op) {
    default:
      return false;
    case match:
    case not_match:
      return is<string_type>(lhs) && is<pattern>(rhs);
    case equal:
    case not_equal:
      return is<none_type>(lhs) || is<none>(rhs) || congruent(lhs, rhs);
    case less:
    case less_equal:
    case greater:
    case greater_equal:
      return congruent(lhs, rhs);
    case in:
    case not_in:
      if (is<string_type>(lhs))
        return is<std::string>(rhs) || is_container(rhs);
      else if (is<address_type>(lhs) || is<subnet_type>(lhs))
        return is<subnet>(rhs) || is_container(rhs);
      else
        return is_container(rhs);
    case ni:
    case not_ni:
      if (is<std::string>(rhs))
        return is<string_type>(lhs) || is_container(lhs);
      else if (is<address>(rhs) || is<subnet>(rhs))
        return is<subnet_type>(lhs) || is_container(lhs);
      else
        return is_container(lhs);
  }
}

bool compatible(const data& lhs, relational_operator op, const type& rhs) {
  return compatible(rhs, flip(op), lhs);
}

namespace {

template <class T>
struct data_to_type;

#define VAST_SPECIALIZE_DATA_TO_TYPE(DATA, TYPE)                               \
  template <>                                                                  \
  struct data_to_type<DATA> {                                                  \
    using type = TYPE;                                                         \
  };

VAST_SPECIALIZE_DATA_TO_TYPE(none, none_type)
VAST_SPECIALIZE_DATA_TO_TYPE(boolean, boolean_type)
VAST_SPECIALIZE_DATA_TO_TYPE(integer, integer_type)
VAST_SPECIALIZE_DATA_TO_TYPE(count, count_type)
VAST_SPECIALIZE_DATA_TO_TYPE(real, real_type)
VAST_SPECIALIZE_DATA_TO_TYPE(timespan, timespan_type)
VAST_SPECIALIZE_DATA_TO_TYPE(timestamp, timestamp_type)
VAST_SPECIALIZE_DATA_TO_TYPE(std::string, string_type)
VAST_SPECIALIZE_DATA_TO_TYPE(pattern, pattern_type)
VAST_SPECIALIZE_DATA_TO_TYPE(address, address_type)
VAST_SPECIALIZE_DATA_TO_TYPE(subnet, subnet_type)
VAST_SPECIALIZE_DATA_TO_TYPE(port, port_type)

#undef VAST_SPECIALIZE_DATA_TO_TYPE

struct data_checker {
  data_checker(const type& t) : type_{t} { }

  template <class T>
  bool operator()(const T&) const {
    return is<typename data_to_type<T>::type>(type_);
  }

  bool operator()(const enumeration& e) const {
    auto t = get_if<enumeration_type>(type_);
    return t && e < t->fields.size();
  }

  bool operator()(const vector& v) const {
    auto r = get_if<record_type>(type_);
    if (r) {
      if (r->fields.size() != v.size())
        return false;
      for (size_t i = 0; i < r->fields.size(); ++i)
        if (!type_check(r->fields[i].type, v[i]))
          return false;
      return true;
    }
    if (v.empty())
      return true;
    auto t = get_if<vector_type>(type_);
    return t && type_check(t->value_type, v[0]);
  }

  bool operator()(const set& s) const {
    if (s.empty())
      return true;
    auto t = get_if<set_type>(type_);
    return t && type_check(t->value_type, *s.begin());
  }

  bool operator()(const table& x) const {
    if (x.empty())
      return true;
    auto t = get_if<table_type>(type_);
    if (!t)
      return false;
    return type_check(t->key_type, x.begin()->first) &&
      type_check(t->value_type, x.begin()->second);
  }

  const type& type_;
};

} // namespace <anonymous>

bool type_check(const type& t, const data& d) {
  return is<none>(d) || visit(data_checker{t}, d);
}

namespace {

struct default_constructor {
  data operator()(none_type) const {
    return nil;
  }

  template <class T>
  data operator()(const T&) const {
    return type_to_data<T>{};
  }

  data operator()(const record_type& r) const {
    vector v;
    for (auto& f : r.fields)
      v.push_back(visit(*this, f.type));
    return v;
  }

  data operator()(const alias_type& a) const {
    return construct(a.value_type);
  }
};

} // namespace <anonymous>

data construct(const type& t) {
  return visit(default_constructor{}, t);
}

namespace {

struct kind_printer {
  using result_type = std::string;

  result_type operator()(const none_type&) const {
    return "none";
  }

  result_type operator()(const boolean_type&) const {
    return "bool";
  }

  result_type operator()(const integer_type&) const {
    return "integer";
  }

  result_type operator()(const count_type&) const {
    return "count";
  }

  result_type operator()(const real_type&) const {
    return "real";
  }

  result_type operator()(const timespan_type&) const {
    return "timespan";
  }

  result_type operator()(const timestamp_type&) const {
    return "timestamp";
  }

  result_type operator()(const string_type&) const {
    return "string";
  }

  result_type operator()(const pattern_type&) const {
    return "pattern";
  }

  result_type operator()(const address_type&) const {
    return "address";
  }

  result_type operator()(const subnet_type&) const {
    return "subnet";
  }

  result_type operator()(const port_type&) const {
    return "port";
  }

  result_type operator()(const enumeration_type&) const {
    return "enumeration";
  }

  result_type operator()(const vector_type&) const {
    return "vector";
  }

  result_type operator()(const set_type&) const {
    return "set";
  }

  result_type operator()(const table_type&) const {
    return "table";
  }

  result_type operator()(const record_type&) const {
    return "record";
  }

  result_type operator()(const alias_type&) const {
    return "alias";
  }
};

struct jsonizer {
  jsonizer(json& j) : json_{j} { }

  template <class T>
  bool operator()(const T&) {
    json_ = {};
    return true;
  }

  bool operator()(const enumeration_type& e) {
    json::array a;
    std::transform(e.fields.begin(),
                   e.fields.end(),
                   std::back_inserter(a),
                   [](auto& x) { return json{x}; });
    json_ = std::move(a);
    return true;
  }

  bool operator()(const vector_type& v) {
    json::object o;
    if (!convert(v.value_type, o["value_type"]))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(const set_type& s) {
    json::object o;
    if (!convert(s.value_type, o["value_type"]))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(const table_type& t) {
    json::object o;
    if (!convert(t.key_type, o["key"]))
      return false;
    if (!convert(t.value_type, o["value"]))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(const record_type& r) {
    json::object o;
    for (auto& field : r.fields)
      if (!convert(field.type, o[to_string(field.name)]))
        return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(const alias_type& a) {
    return convert(a.value_type, json_);
  }

  json& json_;
};

} // namespace <anonymous>

bool convert(const type& t, json& j) {
  json::object o;
  o["name"] = t.name();
  o["kind"] = visit(kind_printer{}, t);
  if (!visit(jsonizer{o["structure"]}, t))
    return false;
  json::object attrs;
  for (auto& a : t.attributes())
    attrs.emplace(a.key, a.value ? json{*a.value} : json{});
  o["attributes"] = std::move(attrs);
  j = std::move(o);
  return true;
}

} // namespace vast
