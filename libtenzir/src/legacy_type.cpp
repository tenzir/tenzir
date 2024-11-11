//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/legacy_type.hpp"

#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/module.hpp"
#include "tenzir/pattern.hpp"

#include <optional>
#include <tuple>
#include <typeindex>
#include <utility>

using caf::get_if;
using caf::holds_alternative;
using caf::visit;
using namespace std::string_view_literals;

namespace tenzir {

legacy_attribute::legacy_attribute(std::string key) : key{std::move(key)} {
}

legacy_attribute::legacy_attribute(std::string key,
                                   caf::optional<std::string> value)
  : key{std::move(key)}, value{std::move(value)} {
}

bool operator==(const legacy_attribute& x, const legacy_attribute& y) {
  return x.key == y.key && x.value == y.value;
}

bool operator<(const legacy_attribute& x, const legacy_attribute& y) {
  return std::tie(x.key, x.value) < std::tie(y.key, y.value);
}

namespace {

legacy_none_type legacy_none_type_singleton;

} // namespace

// -- type ---------------------------------------------------------------------

bool operator==(const legacy_type& x, const legacy_type& y) {
  if (x.ptr_ && y.ptr_)
    return *x.ptr_ == *y.ptr_;
  return x.ptr_ == y.ptr_;
}

bool operator<(const legacy_type& x, const legacy_type& y) {
  if (x.ptr_ && y.ptr_)
    return *x.ptr_ < *y.ptr_;
  return x.ptr_ < y.ptr_;
}

legacy_type& legacy_type::name(const std::string& x) & {
  if (ptr_)
    ptr_.unshared().name_ = x;
  return *this;
}

legacy_type legacy_type::name(const std::string& x) && {
  if (ptr_)
    ptr_.unshared().name_ = x;
  return std::move(*this);
}

legacy_type&
legacy_type::update_attributes(std::vector<legacy_attribute> xs) & {
  if (ptr_) {
    auto& attrs = ptr_.unshared().attributes_;
    for (auto& x : xs) {
      auto i = std::find_if(attrs.begin(), attrs.end(), [&](auto& attr) {
        return attr.key == x.key;
      });
      if (i == attrs.end())
        attrs.push_back(std::move(x));
      else
        i->value = std::move(x).value;
    }
  }
  return *this;
}

legacy_type
legacy_type::update_attributes(std::vector<legacy_attribute> xs) && {
  if (ptr_) {
    auto& attrs = ptr_.unshared().attributes_;
    for (auto& x : xs) {
      auto i = std::find_if(attrs.begin(), attrs.end(), [&](auto& attr) {
        return attr.key == x.key;
      });
      if (i == attrs.end())
        attrs.push_back(std::move(x));
      else
        i->value = std::move(x).value;
    }
  }
  return std::move(*this);
}

legacy_type::operator bool() const {
  return ptr_ != nullptr;
}

const std::string& legacy_type::name() const {
  static const std::string empty_string = "";
  return ptr_ ? ptr_->name_ : empty_string;
}

const std::vector<legacy_attribute>& legacy_type::attributes() const {
  static const std::vector<legacy_attribute> no_attributes = {};
  return ptr_ ? ptr_->attributes_ : no_attributes;
}

legacy_abstract_type_ptr legacy_type::ptr() const {
  return ptr_;
}

const legacy_abstract_type* legacy_type::raw_ptr() const noexcept {
  return ptr_ != nullptr ? ptr_.get() : &legacy_none_type_singleton;
}

const legacy_abstract_type* legacy_type::operator->() const noexcept {
  return raw_ptr();
}

const legacy_abstract_type& legacy_type::operator*() const noexcept {
  return *raw_ptr();
}

legacy_type::legacy_type(legacy_abstract_type_ptr x) : ptr_{std::move(x)} {
  // nop
}

// -- legacy_abstract_type
// -----------------------------------------------------------

legacy_abstract_type::~legacy_abstract_type() {
  // nop
}

bool legacy_abstract_type::equals(const legacy_abstract_type& other) const {
  return typeid(*this) == typeid(other) && name_ == other.name_
         && attributes_ == other.attributes_;
}

bool legacy_abstract_type::less_than(const legacy_abstract_type& other) const {
  auto tx = std::type_index(typeid(*this));
  auto ty = std::type_index(typeid(other));
  if (tx != ty)
    return tx < ty;
  auto x = std::tie(name_, attributes_);
  auto y = std::tie(other.name_, other.attributes_);
  return x < y;
}

bool operator==(const legacy_abstract_type& x, const legacy_abstract_type& y) {
  return x.equals(y);
}

bool operator<(const legacy_abstract_type& x, const legacy_abstract_type& y) {
  return x.less_than(y);
}

// -- legacy_record_type
// --------------------------------------------------------------

bool operator==(const record_field& x, const record_field& y) {
  return x.name == y.name && x.type == y.type;
}

bool operator<(const record_field& x, const record_field& y) {
  return std::tie(x.name, x.type) < std::tie(y.name, y.type);
}

legacy_record_type::legacy_record_type(std::vector<record_field> xs) noexcept
  : fields{std::move(xs)} {
  // nop
}

legacy_record_type::legacy_record_type(
  std::initializer_list<record_field> xs) noexcept
  : fields{xs} {
  // nop
}

bool legacy_record_type::equals(const legacy_abstract_type& other) const {
  return super::equals(other) && fields == downcast(other).fields;
}

bool legacy_record_type::less_than(const legacy_abstract_type& other) const {
  return super::less_than(other) || fields < downcast(other).fields;
}

caf::expected<legacy_record_type>
merge(const legacy_record_type& lhs, const legacy_record_type& rhs) {
  legacy_record_type result = lhs;
  auto in_lhs = [&](std::string_view name) {
    return std::find_if(result.fields.begin(),
                        result.fields.begin() + lhs.fields.size(),
                        [&](const auto& field) {
                          return field.name == name;
                        });
  };
  for (const auto& rfield : rhs.fields) {
    if (auto it = in_lhs(rfield.name);
        it != result.fields.begin() + lhs.fields.size()) {
      if (it->type == rfield.type)
        continue;
      const auto* lrec = try_as<legacy_record_type>(&it->type);
      const auto* rrec = try_as<legacy_record_type>(&rfield.type);
      if (!(rrec && lrec))
        return caf::make_error(ec::convert_error, //
                               fmt::format("failed to merge {} and {} because "
                                           "of duplicate field {}",
                                           type::from_legacy_type(lhs),
                                           type::from_legacy_type(rhs),
                                           rfield.name));
      auto x = merge(*lrec, *rrec);
      if (!x)
        return x.error();
      it->type = legacy_type{std::move(*x)};
    } else {
      result.fields.push_back(rfield);
    }
  }
  return result.name("");
}

legacy_record_type
priority_merge(const legacy_record_type& lhs, const legacy_record_type& rhs,
               merge_policy p) {
  legacy_record_type result = lhs;
  auto in_lhs = [&](std::string_view name) {
    return std::find_if(result.fields.begin(),
                        result.fields.begin() + lhs.fields.size(),
                        [&](const auto& field) {
                          return field.name == name;
                        });
  };
  for (const auto& rfield : rhs.fields) {
    if (auto it = in_lhs(rfield.name);
        it != result.fields.begin() + lhs.fields.size()) {
      if (it->type == rfield.type)
        continue;
      const auto* lrec = try_as<legacy_record_type>(&it->type);
      const auto* rrec = try_as<legacy_record_type>(&rfield.type);
      if (rrec && lrec)
        it->type = priority_merge(*lrec, *rrec, p);
      else if (p == merge_policy::prefer_right)
        it->type = rfield.type;
      // else policy_left: continue
    } else {
      result.fields.push_back(rfield);
    }
  }
  if (p == merge_policy::prefer_left) {
    result.attributes(rhs.attributes());
    result.update_attributes(lhs.attributes());
  } else {
    result.update_attributes(rhs.attributes());
  }
  return result.name("");
}

std::optional<legacy_record_type>
remove_field(const legacy_record_type& r, std::vector<std::string_view> path) {
  TENZIR_ASSERT(!path.empty());
  auto result = legacy_record_type{}.name(r.name()).attributes(r.attributes());
  for (const auto& f : r.fields) {
    if (f.name == path.front()) {
      if (path.size() > 1) {
        path.erase(path.begin());
        const auto* field_rec = try_as<legacy_record_type>(&f.type);
        if (!field_rec)
          return std::nullopt;
        auto new_rec = remove_field(*field_rec, path);
        if (!new_rec)
          return std::nullopt;
        // TODO: Remove this condition if empty records get allowed.
        if (!new_rec->fields.empty())
          result.fields.emplace_back(f.name, *new_rec);
      }
      // else skips this field. It is the leaf to remove!
    } else {
      result.fields.push_back(f);
    }
  }
  return result;
}

std::optional<legacy_record_type>
remove_field(const legacy_record_type& r, offset o) {
  TENZIR_ASSERT(!o.empty());
  auto result = legacy_record_type{}.name(r.name()).attributes(r.attributes());
  if (o.front() >= r.fields.size())
    return {};
  const auto& field = r.fields[o.front()];
  for (const auto& f : r.fields) {
    if (&f == &field) {
      if (o.size() > 1) {
        o.erase(o.begin());
        const auto* field_rec = try_as<legacy_record_type>(&field.type);
        if (!field_rec)
          return {};
        auto new_rec = remove_field(*field_rec, std::move(o));
        if (!new_rec)
          return {};
        // TODO: Remove this condition if empty records get allowed.
        if (!new_rec->fields.empty())
          result.fields.emplace_back(f.name, *new_rec);
      }
    } else {
      result.fields.push_back(f);
    }
  }
  return result;
}

namespace {

const char* kind_tbl[] = {
  "none", "bool",   "int",     "count",   "real",   "duration",
  "time", "string", "pattern", "address", "subnet", "enumeration",
  "list", "map",    "record",  "alias",
};

using caf::detail::tl_size;

static_assert(std::size(kind_tbl) == tl_size<legacy_concrete_types>::value);

} // namespace

std::string kind(const legacy_type& x) {
  return kind_tbl[x->index()];
}

} // namespace tenzir
