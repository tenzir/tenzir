//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/view.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/type.hpp"

#include <algorithm>
#include <regex>

namespace tenzir {

// -- pattern_view ------------------------------------------------------------

pattern_view::pattern_view(const pattern& x)
  : pattern_{x.string()}, case_insensitive_{x.options().case_insensitive} {
  // nop
}

std::string_view pattern_view::string() const {
  return pattern_;
}

bool pattern_view::case_insensitive() const {
  return case_insensitive_;
}

bool operator==(pattern_view lhs, pattern_view rhs) noexcept {
  return std::tie(lhs.pattern_, lhs.case_insensitive_)
         == std::tie(rhs.pattern_, rhs.case_insensitive_);
}

std::strong_ordering operator<=>(pattern_view lhs, pattern_view rhs) noexcept {
  // This is a polyfill for std::lexicographical_compare_threeway
  while (!lhs.pattern_.empty() && !rhs.pattern_.empty()) {
    if (lhs.pattern_[0] < rhs.pattern_[0])
      return std::strong_ordering::less;
    if (lhs.pattern_[0] > rhs.pattern_[0])
      return std::strong_ordering::greater;
    lhs.pattern_ = lhs.pattern_.substr(1);
    rhs.pattern_ = rhs.pattern_.substr(1);
  }
  return static_cast<uint8_t>(lhs.case_insensitive_)
         <=> static_cast<uint8_t>(rhs.case_insensitive_);
}

bool is_equal(const data& x, const data_view& y) {
  auto pred = [](const auto& lhs, const auto& rhs) {
    return is_equal(lhs, rhs);
  };
  auto f = detail::overload{
    [&](const auto& lhs, const auto& rhs) {
      using lhs_type = std::decay_t<decltype(lhs)>;
      using rhs_type = std::decay_t<decltype(rhs)>;
      if constexpr (std::is_same_v<view<lhs_type>, rhs_type>)
        return lhs == rhs;
      else
        return false;
    },
    [&](const blob& lhs, const view<blob>& rhs) {
      return std::ranges::equal(make_view(lhs), rhs);
    },
    [&](const pattern& lhs, const view<pattern>& rhs) {
      return lhs.string() == rhs.string();
    },
    [&](const list& lhs, const view<list>& rhs) {
      return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), pred);
    },
    [&](const map& lhs, const view<map>& rhs) {
      auto f = [](const auto& xs, const auto& ys) {
        return is_equal(xs.first, ys.first) && is_equal(xs.second, ys.second);
      };
      return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), f);
    },
    [&](const record& lhs, const view<record>& rhs) {
      auto f = [](const auto& xs, const auto& ys) {
        return xs.first == ys.first && is_equal(xs.second, ys.second);
      };
      return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), f);
    },
  };
  return caf::visit(f, x, y);
}

bool is_equal(const data_view& x, const data& y) {
  return is_equal(y, x);
}

// -- default_list_view -----------------------------------------------------

default_list_view::default_list_view(const list& xs) : xs_{xs} {
  // nop
}

default_list_view::value_type default_list_view::at(size_type i) const {
  return make_data_view(xs_[i]);
}

default_list_view::size_type default_list_view::size() const noexcept {
  return xs_.size();
}

// -- default_map_view --------------------------------------------------------

default_map_view::default_map_view(const map& xs) : xs_{xs} {
  // nop
}

default_map_view::value_type default_map_view::at(size_type i) const {
  auto& [key, value] = *std::next(xs_.begin(), i);
  return {make_data_view(key), make_data_view(value)};
}

default_map_view::size_type default_map_view::size() const noexcept {
  return xs_.size();
}

// -- default_record_view --------------------------------------------------------

default_record_view::default_record_view(const record& xs) : xs_{xs} {
  // nop
}

default_record_view::value_type default_record_view::at(size_type i) const {
  TENZIR_ASSERT(i < xs_.size());
  auto& [key, value] = *std::next(xs_.begin(), i);
  return {key, make_data_view(value)};
}

default_record_view::size_type default_record_view::size() const noexcept {
  return xs_.size();
}

// -- make_view ---------------------------------------------------------------

data_view make_view(const data& x) {
  return caf::visit(
    [](const auto& z) {
      return make_data_view(z);
    },
    x);
}

// -- materialization ----------------------------------------------------------

std::string materialize(std::string_view x) {
  return std::string{x};
}

blob materialize(view<blob> x) {
  return {x.begin(), x.end()};
}

pattern materialize(pattern_view x) {
  auto options = pattern_options{x.case_insensitive()};
  auto result = pattern::make(std::string{x.string()}, options);
  TENZIR_ASSERT(result, fmt::to_string(result.error()).c_str());
  return std::move(*result);
}

namespace {

auto materialize(std::pair<data_view, data_view> x) {
  return std::pair(materialize(x.first), materialize(x.second));
}

auto materialize(std::pair<std::string_view, data_view> x) {
  return std::pair(std::string{x.first}, materialize(x.second));
}

template <class Result, class T>
Result materialize_container(const T& xs) {
  Result result;
  if (xs)
    for (auto x : *xs)
      result.insert(result.end(), materialize(x));
  return result;
}

} // namespace

list materialize(list_view_handle xs) {
  return materialize_container<list>(xs);
}

map materialize(map_view_handle xs) {
  return materialize_container<map>(xs);
}

record materialize(record_view_handle xs) {
  return materialize_container<record>(xs);
}

data materialize(data_view x) {
  return caf::visit(
    [](auto y) {
      return data{materialize(y)};
    },
    x);
}

// WARNING: making changes to the logic of this function requires adapting the
// companion overload in type.cpp.
bool type_check(const type& x, const data_view& y) {
  auto f = detail::overload{
    [&](const auto&, const view<caf::none_t>&) {
      // Every type can be assigned null.
      return true;
    },
    [&](const enumeration_type& t, const view<enumeration>& u) {
      return !t.field(u).empty();
    },
    [&](const list_type& t, const view<list>& u) {
      if (u.empty())
        return true;
      const auto vt = t.value_type();
      auto it = u.begin();
      const auto check = [&](const auto& d) noexcept {
        return type_check(vt, d);
      };
      if (check(*it)) {
        // Technically lists can contain heterogeneous data,
        // but for optimization purposes we only check the
        // first element when assertions are disabled.
        TENZIR_ASSERT_EXPENSIVE(std::all_of(it + 1, u.end(), check), //
                                "expected a homogenous list");
        return true;
      }
      return false;
    },
    [&](const map_type& t, const view<map>& u) {
      if (u.empty())
        return true;
      const auto kt = t.key_type();
      const auto vt = t.value_type();
      auto it = u.begin();
      const auto check = [&](const auto& d) noexcept {
        return type_check(kt, d.first) && type_check(vt, d.second);
      };
      if (check(*it)) {
        // Technically maps can contain heterogeneous data,
        // but for optimization purposes we only check the
        // first element when assertions are disabled.
        TENZIR_ASSERT_EXPENSIVE(std::all_of(it + 1, u.end(), check), //
                                "expected a homogenous map");
        return true;
      }
      return false;
    },
    [&](const record_type& t, const view<record>& u) {
      if (u.size() != t.num_fields())
        return false;
      for (size_t i = 0; const auto& [k, v] : u) {
        const auto field = t.field(i++);
        if (field.name != k || type_check(field.type, v))
          return false;
      }
      return true;
    },
    [&]<basic_type T, class U>(const T&, const U&) {
      // For basic types we can solely rely on the result of
      // construct.
      return std::is_same_v<view<type_to_data_t<T>>, U>;
    },
    [&]<complex_type T, class U>(const T&, const U&) {
      // We don't have a matching overload.
      static_assert(!std::is_same_v<view<type_to_data_t<T>>, U>, //
                    "missing type check overload");
      return false;
    },
  };
  return caf::visit(f, x, y);
}

data_view to_canonical(const type& t, const data_view& x) {
  auto v = detail::overload{
    [](const view<enumeration>& x, const enumeration_type& t) -> data_view {
      if (auto result = t.field(materialize(x)); !result.empty())
        return result;
      return caf::none;
    },
    [&](auto&, auto&) {
      return x;
    },
  };
  return caf::visit(v, x, t);
}

data_view to_internal(const type& t, const data_view& x) {
  auto v = detail::overload{
    [](const view<std::string>& s, const enumeration_type& t) -> data_view {
      if (auto key = t.resolve(s))
        return detail::narrow_cast<enumeration>(*key);
      return caf::none;
    },
    [&](auto&, auto&) {
      return x;
    },
  };
  return caf::visit(v, x, t);
}

auto descend(view<record> r, std::string_view path)
  -> caf::expected<data_view> {
  TENZIR_ASSERT(!path.empty());
  auto names = detail::split(path, ".");
  TENZIR_ASSERT(!names.empty());
  auto current = r;
  for (auto& name : names) {
    auto last = &name == &names.back();
    auto it = std::find_if(current.begin(), current.end(),
                           [&name](const auto& field_name_and_value) {
                             return field_name_and_value.first == name;
                           });
    if (it == current.end()) {
      // Field not found.
      return caf::make_error(ec::lookup_error,
                             fmt::format("can't find record at path {}", path));
    }
    auto field = it->second;
    if (last) {
      // Path was completely processed.
      return field;
    }
    auto maybe_rec = try_as<view<record>>(&field);
    if (!maybe_rec) {
      // This is not a record, but path continues.
      return caf::make_error(
        ec::lookup_error,
        fmt::format("expected {} to be a record",
                    fmt::join(std::span{names.data(), &name}, ".")));
    }
    current = *maybe_rec;
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
