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
#include <typeindex>
#include <utility>

#include "vast/data.hpp"
#include "vast/json.hpp"
#include "vast/pattern.hpp"
#include "vast/schema.hpp"
#include "vast/type.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"

using caf::get_if;
using caf::holds_alternative;
using caf::visit;
using namespace std::string_view_literals;

namespace vast {

namespace {

none_type none_type_singleton;

} // namespace <anonymous>

// -- type ---------------------------------------------------------------------

bool operator==(const type& x, const type& y) {
  if (x.ptr_ && y.ptr_)
    return *x.ptr_ == *y.ptr_;
  return x.ptr_ == y.ptr_;
}

bool operator<(const type& x, const type& y) {
  if (x.ptr_ && y.ptr_)
    return *x.ptr_ < *y.ptr_;
  return x.ptr_ < y.ptr_;
}

type& type::name(const std::string& x) & {
  if (ptr_)
    ptr_.unshared().name_ = x;
  return *this;
}

type&& type::name(const std::string& x) && {
  if (ptr_)
    ptr_.unshared().name_ = x;
  return std::move(*this);
}

type& type::attributes(std::vector<attribute> xs) & {
  if (ptr_)
    ptr_.unshared().attributes_ = std::move(xs);
  return *this;
}

type&& type::attributes(std::vector<attribute> xs) && {
  if (ptr_)
    ptr_.unshared().attributes_ = std::move(xs);
  return std::move(*this);
}

type::operator bool() const {
  return ptr_ != nullptr;
}

const std::string& type::name() const {
  static const std::string empty_string = "";
  return ptr_ ? ptr_->name_ : empty_string;
}

const std::vector<attribute>& type::attributes() const {
  static const std::vector<attribute> no_attributes = {};
  return ptr_ ? ptr_->attributes_ : no_attributes;
}

abstract_type_ptr type::ptr() const {
  return ptr_;
}

const abstract_type* type::raw_ptr() const noexcept {
  return ptr_ != nullptr ? ptr_.get() : &none_type_singleton;
}

const abstract_type* type::operator->() const noexcept {
  return raw_ptr();
}

const abstract_type& type::operator*() const noexcept {
  return *raw_ptr();
}

type::type(abstract_type_ptr x) : ptr_{std::move(x)} {
  // nop
}

// -- abstract_type -----------------------------------------------------------

abstract_type::~abstract_type() {
  // nop
}

bool abstract_type::equals(const abstract_type& other) const {
  return typeid(*this) == typeid(other)
         && name_ == other.name_ && attributes_ == other.attributes_;
}

bool abstract_type::less_than(const abstract_type& other) const {
  auto tx = std::type_index(typeid(*this));
  auto ty = std::type_index(typeid(other));
  if (tx != ty)
    return tx < ty;
  auto x = std::tie(name_, attributes_);
  auto y = std::tie(other.name_, other.attributes_);
  return x < y;
}

bool operator==(const abstract_type& x, const abstract_type& y) {
  return x.equals(y);
}

bool operator<(const abstract_type& x, const abstract_type& y) {
  return x.less_than(y);
}

// -- record_type --------------------------------------------------------------

record_type::record_type(std::vector<record_field> xs) : fields{std::move(xs)} {
  // nop
}

record_type::record_type(std::initializer_list<record_field> xs)
  : fields{std::move(xs)} {
  // nop
}

std::string record_type::each::range_state::key() const {
  return detail::join(trace.begin(), trace.end(), ".",
                      [](auto field) { return field->name; });
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
  } while ((rec = get_if<record_type>(&state_.trace.back()->type)));
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
  while (auto r = get_if<record_type>(&f->type)) {
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

caf::optional<offset> record_type::resolve(std::string_view key) const {
  offset result;
  if (key.empty())
    return caf::none;
  auto offset = offset::size_type{0};
  for (auto i = fields.begin(); i != fields.end(); ++i, ++offset) {
    auto& field = *i;
    auto& name = field.name;
    VAST_ASSERT(!name.empty());
    // Check whether the field name is a prefix of the key to resolve.
    auto j = std::mismatch(name.begin(), name.end(), key.begin()).first;
    if (j == name.end()) {
      result.push_back(offset);
      if (name.size() == key.size())
        return result;
      // In case we have a partial match, e.g., "x" for "x.y", we need to skip
      // the '.' key separator.
      auto remainder = key.substr(1 + name.size());
      auto rec = get_if<record_type>(&field.type);
      if (!rec)
        return caf::none;
      auto sub_result = rec->resolve(remainder);
      if (!sub_result)
        return caf::none;
      result.insert(result.end(), sub_result->begin(), sub_result->end());
      return result;
    }
  }
  return caf::none;
}

caf::optional<std::string> record_type::resolve(const offset& o) const {
  if (o.empty())
    return caf::none;
  std::string result;
  auto r = this;
  for (size_t i = 0; i < o.size(); ++i) {
    auto x = o[i];
    if (x >= r->fields.size())
      return caf::none;
    if (!result.empty())
      result += '.';
    result += r->fields[x].name;
    if (i != o.size() - 1) {
      r = get_if<record_type>(&r->fields[x].type);
      if (!r)
        return caf::none;
    }
  }
  return result;
}

namespace {

enum class mode {
  prefix,
  suffix,
  exact,
  any,
};

template <mode Mode>
struct finder {
  using result_type = std::vector<std::pair<offset, std::string>>;

  finder(std::string_view key) : rx_{pattern::glob(key)} {
    if constexpr (Mode == mode::prefix)
      rx_ = "^" + rx_ + ".*";
    else if constexpr (Mode == mode::suffix)
      rx_ = ".*" + rx_ + "$";
    else if constexpr (Mode == mode::exact)
      rx_ = "^" + rx_ + "$";
    else if constexpr (Mode == mode::any)
      rx_ = ".*" + rx_ + ".*";
  }

  result_type match() const {
    result_type result;
    if (rx_.match(trace_))
      result.emplace_back(off_, trace_);
    return result;
  }

  template <class T>
  result_type operator()(const T&) const {
    return match();
  }

  result_type operator()(const record_type& r) {
    result_type result;
    if constexpr (Mode != mode::suffix) {
      // Check whether we want this record first. This does not make sense
      // for suffixes, because they always start at a leaf.
      auto sub_result = match();
      result.insert(result.end(),
                    std::make_move_iterator(sub_result.begin()),
                    std::make_move_iterator(sub_result.end()));
    }
    off_.push_back(0);
    for (auto& f : r.fields) {
      auto prev_trace_size = trace_.size();
      trace_ += trace_.empty() ? f.name : '.' + f.name;
      auto sub_result = visit(*this, f.type);
      result.insert(result.end(),
                    std::make_move_iterator(sub_result.begin()),
                    std::make_move_iterator(sub_result.end()));
      trace_.resize(prev_trace_size);
      ++off_.back();
    }
    off_.pop_back();
    return result;
  }

  pattern rx_;
  std::string trace_;
  offset off_;
};

} // namespace <anonymous>

std::vector<std::pair<offset, std::string>>
record_type::find(std::string_view key) const {
  return finder<mode::any>{key}(*this);
}

std::vector<std::pair<offset, std::string>>
record_type::find_prefix(std::string_view key) const {
  return finder<mode::prefix>{key}(*this);
}

std::vector<std::pair<offset, std::string>>
record_type::find_suffix(std::string_view key) const {
  return finder<mode::suffix>{key}(*this);
}

const type* record_type::at(std::string_view key) const {
  auto xs = finder<mode::exact>{key}(*this);
  if (xs.empty())
    return nullptr;
  VAST_ASSERT(xs.size() == 1u);
  return at(xs[0].first);
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
    r = get_if<record_type>(t);
    if (!r)
      return nullptr;
  }
  return nullptr;
}

bool record_type::equals(const abstract_type& other) const {
  return super::equals(other) && fields == downcast(other).fields;
}

bool record_type::less_than(const abstract_type& other) const {
  return super::less_than(other) || fields < downcast(other).fields;
}

caf::optional<size_t> record_type::flat_index_at(offset o) const {
  // Empty offsets are invalid.
  if (o.empty())
    return caf::none;
  // Bounds check.
  if (o[0] >= fields.size())
    return caf::none;
  // Example: o = [1] picks the second element. However, we still need the
  // total amount of nested elements of the first element.
  size_t flat_index = 0;
  for (size_t i = 0; i < o[0]; ++i)
    flat_index += flat_size(fields[i].type);
  // Now, we know how many fields are on the left. We're done if the offset
  // points to a non-record field in this record.
  auto record_field = get_if<record_type>(&fields[o[0]].type);
  if (o.size() == 1) {
    // Sanity check: the offset is invalid if it points to a record type.
    if (record_field != nullptr)
      return caf::none;
    return flat_index;
  }
  // The offset points into the field, therefore it must be a record type.
  if (record_field == nullptr)
    return caf::none;
  // Drop index of the first dimension and dispatch to field recursively.
  o.erase(o.begin());
  auto sub_result = record_field->flat_index_at(o);
  if (!sub_result)
    return caf::none;
  return flat_index + *sub_result;
}

record_type flatten(const record_type& rec) {
  /// Make a copy of the original to keep name and attributes.
  record_type result = rec;
  result.fields.clear();
  for (auto& outer : rec.fields)
    if (auto r = get_if<record_type>(&outer.type)) {
      auto flat = flatten(*r);
      for (auto& inner : flat.fields)
        result.fields.emplace_back(outer.name + "." + inner.name, inner.type);
    } else {
      result.fields.push_back(outer);
    }
  return result;
}

type flatten(const type& t) {
  auto r = get_if<record_type>(&t);
  return r ? flatten(*r) : t;
}

bool is_flat(const record_type& rec) {
  auto& fs = rec.fields;
  return std::all_of(fs.begin(), fs.end(), [](auto& f) {
    return !holds_alternative<record_type>(f.type);
  });
}

bool is_flat(const type& t) {
  auto r = get_if<record_type>(&t);
  return !r || is_flat(*r);
}

size_t flat_size(const record_type& rec) {
  auto op = [](size_t x, const auto& y) { return x + flat_size(y.type); };
  return std::accumulate(rec.fields.begin(), rec.fields.end(), size_t{0}, op);
}

size_t flat_size(const type& t) {
  if (auto r = get_if<record_type>(&t))
    return flat_size(*r);
  return 1;
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
      r = const_cast<record_type*>(get_if<record_type>(&r->fields.back().type));
    }
    r->fields.emplace_back(std::string{names.back()}, f.type);
  }
  std::vector<std::vector<const record_type*>> rs(1);
  rs.back().push_back(&result);
  auto more = true;
  while (more) {
    std::vector<const record_type*> next;
    for (auto& current : rs.back())
      for (auto& f : current->fields)
        if (auto r = get_if<record_type>(&f.type))
          next.push_back(r);
    if (next.empty())
      more = false;
    else
      rs.push_back(std::move(next));
  }
  return result;
}

type unflatten(const type& t) {
  auto r = get_if<record_type>(&t);
  return r ? unflatten(*r) : t;
}

bool is_basic(const type& x) {
  return x && is<type_flags::basic>(x->flags());
}

bool is_complex(const type& x) {
  return x && is<type_flags::complex>(x->flags());
}

bool is_recursive(const type& x) {
  return x && is<type_flags::recursive>(x->flags());
}

bool is_container(const type& x) {
  return x && is<type_flags::container>(x->flags());
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

  bool operator()(const map_type& x, const map_type& y) const {
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

  bool operator()(const none_type&, caf::none_t) const {
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

  bool operator()(const map_type&, const map&) const {
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
      return holds_alternative<string_type>(lhs)
             && holds_alternative<pattern_type>(rhs);
    case equal:
    case not_equal:
      return !lhs || !rhs || congruent(lhs, rhs);
    case less:
    case less_equal:
    case greater:
    case greater_equal:
      return congruent(lhs, rhs);
    case in:
    case not_in:
      if (holds_alternative<string_type>(lhs))
        return holds_alternative<string_type>(rhs) || is_container(rhs);
      else if (holds_alternative<address_type>(lhs)
               || holds_alternative<subnet_type>(lhs))
        return holds_alternative<subnet_type>(rhs) || is_container(rhs);
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
      return holds_alternative<string_type>(lhs)
             && holds_alternative<pattern>(rhs);
    case equal:
    case not_equal:
      return !lhs || holds_alternative<caf::none_t>(rhs) || congruent(lhs, rhs);
    case less:
    case less_equal:
    case greater:
    case greater_equal:
      return congruent(lhs, rhs);
    case in:
    case not_in:
      if (holds_alternative<string_type>(lhs))
        return holds_alternative<std::string>(rhs) || is_container(rhs);
      else if (holds_alternative<address_type>(lhs)
               || holds_alternative<subnet_type>(lhs))
        return holds_alternative<subnet>(rhs) || is_container(rhs);
      else
        return is_container(rhs);
    case ni:
    case not_ni:
      if (holds_alternative<std::string>(rhs))
        return holds_alternative<string_type>(lhs) || is_container(lhs);
      else if (holds_alternative<address>(rhs)
               || holds_alternative<subnet>(rhs))
        return holds_alternative<subnet_type>(lhs) || is_container(lhs);
      else
        return is_container(lhs);
  }
}

bool compatible(const data& lhs, relational_operator op, const type& rhs) {
  return compatible(rhs, flip(op), lhs);
}

// WARNING: making changes to the logic of this function requires adapting the
// companion overload in view.cpp.
bool type_check(const type& t, const data& x) {
  auto f = detail::overload(
    [&](const auto& u) {
      using data_type = type_to_data<std::decay_t<decltype(u)>>;
      return caf::holds_alternative<data_type>(x);
    },
    [&](const none_type&) {
      // Cannot determine data type since data may always be null.
      return true;
    },
    [&](const enumeration_type& u) {
      auto e = caf::get_if<enumeration>(&x);
      printf("got enumeration? %i\n", !!e);
      return e && *e < u.fields.size();
    },
    [&](const vector_type& u) {
      if (auto xs = caf::get_if<vector>(&x))
        return xs->empty() || type_check(u.value_type, *xs->begin());
      return false;
    },
    [&](const set_type& u) {
      if (auto xs = caf::get_if<set>(&x))
        return xs->empty() || type_check(u.value_type, *xs->begin());
      return false;
    },
    [&](const map_type& u) {
      auto xs = caf::get_if<map>(&x);
      if (!xs)
        return false;
      if (xs->empty())
        return true;
      auto& [key, value] = *xs->begin();
      return type_check(u.key_type, key) && type_check(u.value_type, value);
    },
    [&](const record_type& u) {
      // Until we have a separate data type for records we treat them as vector.
      auto xs = caf::get_if<vector>(&x);
      if (!xs)
        return false;
      if (xs->size() != u.fields.size())
        return false;
      for (size_t i = 0; i < xs->size(); ++i)
        if (!type_check(u.fields[i].type, (*xs)[i]))
          return false;
      return true;
    },
    [&](const alias_type& u) {
      return type_check(u.value_type, x);
    }
  );
  return caf::holds_alternative<caf::none_t>(x) || caf::visit(f, t);
}

data construct(const type& x) {
  return visit(detail::overload(
    [](const auto& y) {
      return data{type_to_data<std::decay_t<decltype(y)>>{}};
    },
    [](const record_type& t) {
      vector xs;
      xs.reserve(t.fields.size());
      std::transform(t.fields.begin(), t.fields.end(), std::back_inserter(xs),
                     [&](auto& field) { return construct(field.type); });
      return data{std::move(xs)};
    },
    [](const alias_type& t) {
      return construct(t.value_type);
    }
  ), x);
}

std::string to_digest(const type& x) {
  std::hash<type> h;
  return std::to_string(h(x));
}

namespace {

const char* kind_tbl[] = {
  "none",
  "bool",
  "int",
  "count",
  "real",
  "timespan",
  "timestamp",
  "string",
  "pattern",
  "address",
  "subnet",
  "port",
  "enumeration",
  "vector",
  "set",
  "map",
  "record",
  "alias",
};

using caf::detail::tl_size;

static_assert(std::size(kind_tbl) == tl_size<concrete_types>::value);

std::string kind(const type& x) {
  return kind_tbl[x->index()];
}

json jsonize(const type& x) {
  return visit(detail::overload(
    [](const enumeration_type&t) {
      json::array a;
      std::transform(t.fields.begin(),
                     t.fields.end(),
                     std::back_inserter(a),
                     [](auto& x) { return json{x}; });
      return json{std::move(a)};
    },
    [&](const vector_type& t) {
      json::object o;
      o["value_type"] = to_json(t.value_type);
      return json{std::move(o)};
    },
    [&](const set_type& t) {
      json::object o;
      o["value_type"] = to_json(t.value_type);
      return json{std::move(o)};
    },
    [&](const map_type& t) {
      json::object o;
      o["key_type"] = to_json(t.key_type);
      o["value_type"] = to_json(t.value_type);
      return json{std::move(o)};
    },
    [&](const record_type& t) {
      json::object o;
      for (auto& field : t.fields)
        o[to_string(field.name)] = to_json(field.type);
      return json{std::move(o)};
    },
    [&](const alias_type& t) {
      return to_json(t.value_type);
    },
    [](const abstract_type&) {
      return json{};
    }
  ), x);
}

} // namespace <anonymous>

bool convert(const type& t, json& j) {
  json::object o;
  o["name"] = t.name();
  o["kind"] = kind(t);
  o["structure"] = jsonize(t);
  json::object attrs;
  for (auto& a : t.attributes())
    attrs.emplace(a.key, a.value ? json{*a.value} : json{});
  o["attributes"] = std::move(attrs);
  j = std::move(o);
  return true;
}

} // namespace vast
