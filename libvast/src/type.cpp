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

namespace vast {

type::type() : ptr_{new impl} {
}

type& type::name(std::string str) {
  visit([s=std::move(str)](auto& x) { x.name(std::move(s)); }, *this);
  return *this;
}

std::string const& type::name() const {
  return *visit([](auto& x) { return &x.name(); }, *this);
}

std::vector<attribute>& type::attributes() {
  return *visit([](auto& x) { return &x.attributes(); }, *this);
}

std::vector<attribute> const& type::attributes() const {
  return *visit([](auto& x) { return &x.attributes(); }, *this);
}

type& type::attributes(std::initializer_list<attribute> list) {
  attributes() = std::move(list);
  return *this;
}

namespace {

struct equal_to {
  template <class T, class U>
  bool operator()(T const&, U const&) const noexcept {
    return false;
  }

  template <class T>
  bool operator()(T const& x, T const& y) const noexcept {
    return x == y;
  }
};

struct less_than {
  template <class T, class U>
  bool operator()(T const&, U const&) const noexcept {
    return false;
  }

  template <class T>
  bool operator()(T const& x, T const& y) const noexcept {
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

record_type::each::each(record_type const& r) {
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

record_type::each::range_state const& record_type::each::get() const {
  return state_;
}

expected<offset> record_type::resolve(key const& k) const {
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

expected<key> record_type::resolve(offset const& o) const {
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

  finder(key  k, mode m, std::string const& init = "")
    : mode_{m}, key_{std::move(k)} {
    VAST_ASSERT(!key_.empty());
    if (!init.empty())
      trace_.push_back(init);
  }

  template <typename T>
  std::vector<std::pair<offset, key>> operator()(T const&) const {
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

  std::vector<std::pair<offset, key>> operator()(record_type const& r) {
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

  static bool match(std::string const& key, std::string const& trace) {
    return pattern::glob(key).match(trace);
  }

  mode mode_;
  key key_;
  key trace_;
  offset off_;
};

} // namespace <anonymous>

std::vector<std::pair<offset, key>> record_type::find(key const& k) const {
  return finder{k, finder::exact, name()}(*this);
}

std::vector<std::pair<offset, key>>
record_type::find_prefix(key const& k) const {
  return finder{k, finder::prefix, name()}(*this);
}

std::vector<std::pair<offset, key>>
record_type::find_suffix(key const& k) const {
  return finder{k, finder::suffix, name()}(*this);
}

type const* record_type::at(key const& k) const {
  auto r = this;
  for (size_t i = 0; i < k.size(); ++i) {
    auto& id = k[i];
    record_field const* f = nullptr;
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

type const* record_type::at(offset const& o) const {
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

record_type flatten(record_type const& rec) {
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

type flatten(type const& t) {
  auto r = get_if<record_type>(t);
  return r ? flatten(*r) : t;
}

record_type unflatten(record_type const& rec) {
  record_type result;
  for (auto& f : rec.fields) {
    auto names = detail::to_strings(detail::split(f.name, "."));
    VAST_ASSERT(!names.empty());
    record_type* r = &result;
    for (size_t i = 0; i < names.size() - 1; ++i) {
      if (r->fields.empty() || r->fields.back().name != names[i])
        r->fields.emplace_back(std::move(names[i]), record_type{});
      r = get_if<record_type>(r->fields.back().type);
    }
    r->fields.emplace_back(std::move(names.back()), f.type);
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

type unflatten(type const& t) {
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
  template <typename T>
  bool operator()(T const&, T const&) const {
    return true;
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) const {
    return false;
  }

  template <typename T>
  bool operator()(T const& x, alias_type const& a) const {
    using namespace std::placeholders;
    return visit(std::bind(std::cref(*this), std::cref(x), _1), a.value_type);
  }

  template <typename T>
  bool operator()(alias_type const& a, T const& x) const {
    return (*this)(x, a);
  }

  bool operator()(alias_type const& x, alias_type const& y) const {
    return visit(*this, x.value_type, y.value_type);
  }

  bool operator()(enumeration_type const& x, enumeration_type const& y) const {
    return x.fields.size() == y.fields.size();
  }

  bool operator()(vector_type const& x, vector_type const& y) const {
    return visit(*this, x.value_type, y.value_type);
  }

  bool operator()(set_type const& x, set_type const& y) const {
    return visit(*this, x.value_type, y.value_type);
  }

  bool operator()(table_type const& x, table_type const& y) const {
    return visit(*this, x.key_type, y.key_type) &&
        visit(*this, x.value_type, y.value_type);
  }

  bool operator()(record_type const& x, record_type const& y) const {
    if (x.fields.size() != y.fields.size())
      return false;
    for (size_t i = 0; i < x.fields.size(); ++i)
      if (!visit(*this, x.fields[i].type, y.fields[i].type))
        return false;
    return true;
  }
};

struct data_congruence_checker {
  template <typename T, typename U>
  bool operator()(T const&, U const&) const {
    return false;
  }

  bool operator()(none_type const&, none) const {
    return true;
  }

  bool operator()(boolean_type const&, boolean) const {
    return true;
  }

  bool operator()(integer_type const&, integer) const {
    return true;
  }

  bool operator()(count_type const&, count) const {
    return true;
  }

  bool operator()(real_type const&, real) const {
    return true;
  }

  bool operator()(timespan_type const&, timespan) const {
    return true;
  }

  bool operator()(timestamp_type const&, timestamp) const {
    return true;
  }

  bool operator()(string_type const&, std::string const&) const {
    return true;
  }

  bool operator()(pattern_type const&, pattern const&) const {
    return true;
  }

  bool operator()(address_type const&, address const&) const {
    return true;
  }

  bool operator()(subnet_type const&, subnet const&) const {
    return true;
  }

  bool operator()(port_type const&, port const&) const {
    return true;
  }

  bool operator()(enumeration_type const& x, std::string const& y) const {
    return std::find(x.fields.begin(), x.fields.end(), y) != x.fields.end();
  }

  bool operator()(vector_type const&, vector const&) const {
    return true;
  }

  bool operator()(set_type const&, set const&) const {
    return true;
  }

  bool operator()(table_type const&, table const&) const {
    return true;
  }

  bool operator()(record_type const& x, vector const& y) const {
    if (x.fields.size() != y.size())
      return false;
    for (size_t i = 0; i < x.fields.size(); ++i)
      if (!visit(*this, x.fields[i].type, y[i]))
        return false;
    return true;
  }

  template <typename T>
  bool operator()(alias_type const& t, T const& x) const {
    using namespace std::placeholders;
    return visit(std::bind(std::cref(*this), _1, std::cref(x)), t.value_type);
  }
};

} // namespace <anonymous>

bool congruent(type const& x, type const& y) {
  return visit(type_congruence_checker{}, x, y);
}

bool congruent(type const& x, data const& y) {
  return visit(data_congruence_checker{}, x, y);
}

bool congruent(data const& x, type const& y) {
  return visit(data_congruence_checker{}, y, x);
}

bool compatible(type const& lhs, relational_operator op, type const& rhs) {
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

bool compatible(type const& lhs, relational_operator op, data const& rhs) {
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

bool compatible(data const& lhs, relational_operator op, type const& rhs) {
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
  data_checker(type const& t) : type_{t} { }

  template <typename T>
  bool operator()(T const&) const {
    return is<typename data_to_type<T>::type>(type_);
  }

  bool operator()(enumeration const& e) const {
    auto t = get_if<enumeration_type>(type_);
    return t && e < t->fields.size();
  }

  bool operator()(vector const& v) const {
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

  bool operator()(set const& s) const {
    if (s.empty())
      return true;
    auto t = get_if<set_type>(type_);
    return t && type_check(t->value_type, *s.begin());
  }

  bool operator()(table const& x) const {
    if (x.empty())
      return true;
    auto t = get_if<table_type>(type_);
    if (!t)
      return false;
    return type_check(t->key_type, x.begin()->first) &&
      type_check(t->value_type, x.begin()->second);
  }

  type const& type_;
};

} // namespace <anonymous>

bool type_check(type const& t, data const& d) {
  return is<none>(d) || visit(data_checker{t}, d);
}

namespace {

struct default_constructor {
  data operator()(none_type) const {
    return nil;
  }

  template <typename T>
  data operator()(T const&) const {
    return type_to_data<T>{};
  }

  data operator()(record_type const& r) const {
    vector v;
    for (auto& f : r.fields)
      v.push_back(visit(*this, f.type));
    return v;
  }

  data operator()(alias_type const& a) const {
    return construct(a.value_type);
  }
};

} // namespace <anonymous>

data construct(type const& t) {
  return visit(default_constructor{}, t);
}

namespace {

struct kind_printer {
  using result_type = std::string;

  result_type operator()(none_type const&) const {
    return "none";
  }

  result_type operator()(boolean_type const&) const {
    return "bool";
  }

  result_type operator()(integer_type const&) const {
    return "integer";
  }

  result_type operator()(count_type const&) const {
    return "count";
  }

  result_type operator()(real_type const&) const {
    return "real";
  }

  result_type operator()(timespan_type const&) const {
    return "timespan";
  }

  result_type operator()(timestamp_type const&) const {
    return "timestamp";
  }

  result_type operator()(string_type const&) const {
    return "string";
  }

  result_type operator()(pattern_type const&) const {
    return "pattern";
  }

  result_type operator()(address_type const&) const {
    return "address";
  }

  result_type operator()(subnet_type const&) const {
    return "subnet";
  }

  result_type operator()(port_type const&) const {
    return "port";
  }

  result_type operator()(enumeration_type const&) const {
    return "enumeration";
  }

  result_type operator()(vector_type const&) const {
    return "vector";
  }

  result_type operator()(set_type const&) const {
    return "set";
  }

  result_type operator()(table_type const&) const {
    return "table";
  }

  result_type operator()(record_type const&) const {
    return "record";
  }

  result_type operator()(alias_type const&) const {
    return "alias";
  }
};

struct jsonizer {
  jsonizer(json& j) : json_{j} { }

  template <typename T>
  bool operator()(T const&) {
    json_ = {};
    return true;
  }

  bool operator()(enumeration_type const& e) {
    json::array a;
    std::transform(e.fields.begin(),
                   e.fields.end(),
                   std::back_inserter(a),
                   [](auto& x) { return json{x}; });
    json_ = std::move(a);
    return true;
  }

  bool operator()(vector_type const& v) {
    json::object o;
    if (!convert(v.value_type, o["value_type"]))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(set_type const& s) {
    json::object o;
    if (!convert(s.value_type, o["value_type"]))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(table_type const& t) {
    json::object o;
    if (!convert(t.key_type, o["key"]))
      return false;
    if (!convert(t.value_type, o["value"]))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(record_type const& r) {
    json::object o;
    for (auto& field : r.fields)
      if (!convert(field.type, o[to_string(field.name)]))
        return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(alias_type const& a) {
    return convert(a.value_type, json_);
  }

  json& json_;
};

} // namespace <anonymous>

bool convert(type const& t, json& j) {
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
