//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/parse.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view.hpp"

#include <caf/error.hpp>
#include <caf/message_handler.hpp>

/// As a convention, in this file `From` and `To` refer to the source
/// and target types, and `src` and `dst` refer to their values
///
/// Assigns fields from `src` to `dst`.
///
/// The source must have a structure that matches the destination.
/// For example:
///
/// auto xs = record{               | struct foo {
///   {"a", "foo"},                 |   std::string a;
///   {"b", record{                 |   struct {
///     {"c", -42},                 |     integer c;
///     {"d", list{1, 2, 3}}        |     std::vector<count> d;
///   }},                           |   } b;
///   {"e", record{                 |   struct {
///     {"f", caf::none},           |     integer f;
///     {"g", caf::none},           |     std::optional<count> g;
///   }},                           |   } e;
///   {"h", true}                   |   bool h;
/// };                              | };
///
/// A suitable overload of `caf::inspect()` for `struct foo` that exposes
/// the fields in the same order as the schema of `xs` is required
/// for this machinery to work.
///
/// If a member of `from` is missing in `to`, the value does not get
/// overwritten, Similarly, data in `from` that does not match `to` is ignored.
///
/// A special overload that can turn a list of records into a key-value map
/// requires that one of the fields in the accompanying record_type has
/// the "key" attribute. This field will then be used as the key in the target
/// map.
/// NOTE: The overload for `data` is defined last for reasons explained there.

namespace tenzir {

namespace detail {

// clang-format off
template <concepts::insertable To>
  requires requires {
    typename To::key_type;
    typename To::mapped_type;
  }
caf::error insert_to_map(To& dst, typename To::key_type&& key,
                         typename To::mapped_type&& value) {
  auto entry = dst.find(key);
  if (entry == dst.end()) {
    dst.insert({std::move(key), std::move(value)});
  } else {
    // If the mapped type implements the Semigroup concept the values get
    // combined automatically.
    if constexpr (concepts::semigroup<typename To::mapped_type>)
      entry->second = mappend(std::move(entry->second), std::move(value));
    else
      // TODO: Consider continuing if the old and new values are the same.
      return caf::make_error(ec::convert_error,
                             fmt::format(": redefinition of {} detected: \"{}\" "
                                         "vs \"{}\"",
                                         key, entry->second, value));
  }
  return caf::none;
}
// clang-format on

template <class... Args>
// TODO CAF 0.19. Check if context of the
// caf::error can be mutated instead of creating a new object
[[nodiscard]] caf::error
prepend(caf::error&& in, const char* fstring, Args&&... args) {
  if (!in) {
    return std::move(in);
  }
  TENZIR_ASSERT(in.category() == caf::type_id_v<tenzir::ec>);
  auto hdl = caf::message_handler{
    [&, f = fmt::format("{}{{}}", fstring)](std::string& s) {
      return caf::make_message(fmt::format(
        TENZIR_FMT_RUNTIME(f), std::forward<Args>(args)..., std::move(s)));
    },
  };
  auto ctx = in.context();
  if (auto new_msg = hdl(ctx)) {
    return {static_cast<tenzir::ec>(in.code()), std::move(*new_msg)};
  }

  return caf::make_error(static_cast<tenzir::ec>(in.code()),
                         fmt::format(TENZIR_FMT_RUNTIME(fstring),
                                     std::forward<Args>(args)...));
}

} // namespace detail

/// Checks if `from` can be converted to `to`, i.e. whether a viable overload
/// of `convert(from, to)` exists. For the `TYPED_` version, the additional
/// parameter `type` refers to the intended type of `from`. (this can be
/// different from the actual type of `from`, for example a `count` passed as
/// a positive signed int).
// NOTE: You might wonder why this is defined as a macro instead of a concept.
// The issue here is that we're looking for overloads of a non-member function,
// and the rules for name lookup for free functions mandate that regular
// qualified or unqualified lookup is done when the definition of the template
// is parsed, and only dependent lookup is done in the instantiation phase in
// case the call is unqualified. That means a concept would only be able to
// detect overloads that are declared later iff they happen to be in the same
// namespace as their arguments, but it won't pick up overloads like
// `convert(std::string, std::string)` or `convert(uint64_t, uint64_t)`.
// https://timsong-cpp.github.io/cppwp/n4868/temp.res#temp.dep.candidate
// It would be preferable to forward declare `is_concrete_convertible` but that
// is not allowed.
// The only real way to solve this is to replace function overloading with
// specializations of a converter struct template.
#define IS_TYPED_CONVERTIBLE(from, to, type)                                   \
  requires {                                                                   \
    { tenzir::convert(from, to, type) } -> std::same_as<caf::error>;           \
  }

#define IS_UNTYPED_CONVERTIBLE(from, to)                                       \
  requires {                                                                   \
    { tenzir::convert(from, to) } -> std::same_as<caf::error>;                 \
  }

template <class T>
concept has_schema = requires {
  { T::schema() } noexcept -> concepts::sameish<record_type>;
};

// Overload for records.
caf::error convert(const record& src, concepts::inspectable auto& dst,
                   const record_type& schema);

template <has_schema To>
caf::error convert(const record& src, To& dst);

template <has_schema To>
caf::error convert(const data& src, To& dst);

// Generic overload when `src` and `dst` are of the same type.
// TODO: remove the `!std::integral` constraint once count is a real type.
template <class Type, class T>
  requires(!std::integral<T> || std::same_as<bool, T>)
caf::error convert(const T& src, T& dst, const Type&) {
  dst = src;
  return caf::none;
}

template <class From>
  requires(!std::same_as<From, std::string>)
caf::error convert(const From& src, std::string& dst, const string_type&) {
  // This convertible overload is essentially only used when we have YAML keys
  // that contain a scalar value (= signed integer), but want to parse a string.
  // In those cases, we generally want to prefer unsigned integers over signed
  // integers if possible to avoid the unary plus prefix from being printed, as
  // downstream parsers of the resulting string often cannot handle the unary
  // plus correctly.
  if constexpr (std::is_same_v<From, int64_t>) {
    if (src >= 0) {
      return convert(detail::narrow_cast<uint64_t>(src), dst, string_type{});
    }
  }
  // In order to get consistent formatting as strings we create a data view
  // here. While not as cheap as printing directly, it's at least a bit cheaper
  // than going through a data and creating a copy of the entire data.
  dst = fmt::to_string(data_view{make_view(src)});
  return caf::none;
}

// Dispatch to standard conversion.
// clang-format off
template <class From, class To, class Type>
  requires (!std::same_as<From, To> &&
           std::convertible_to<From, To>)
caf::error convert(const From& src, To& dst, const Type&) {
  dst = src;
  return caf::none;
}
// clang-format on

// Overload for reals.
template <std::floating_point To>
caf::error convert(const int64_t& src, To& dst, const double_type&) {
  dst = src;
  return caf::none;
}

/// Overload for converting any arithmetic type to a floating point type.
//  We need to exclude `From == To` to disambiguate overloads.
template <concepts::arithmetic From, std::floating_point To>
  requires(!std::same_as<From, To>)
caf::error convert(const From& src, To& dst, const double_type&) {
  dst = src;
  return caf::none;
}

// Overload for counts.
template <std::unsigned_integral To>
caf::error convert(const uint64_t& src, To& dst, const uint64_type&) {
  if constexpr (sizeof(To) >= sizeof(uint64_t)) {
    dst = src;
  } else {
    if (src < std::numeric_limits<To>::min()
        || src > std::numeric_limits<To>::max()) {
      return caf::make_error(ec::convert_error,
                             fmt::format(": {} can not be represented by the "
                                         "target variable [{}, {}]",
                                         src, std::numeric_limits<To>::min(),
                                         std::numeric_limits<To>::max()));
    }
    dst = detail::narrow_cast<To>(src);
  }
  return caf::none;
}

template <std::unsigned_integral To>
caf::error convert(const int64_t& src, To& dst, const uint64_type&) {
  if (src < 0) {
    return caf::make_error(ec::convert_error,
                           fmt::format(": {} can not be negative ({})",
                                       detail::pretty_type_name(dst), src));
  }
  if constexpr (sizeof(To) >= sizeof(uint64_t)) {
    dst = src;
  } else {
    if (src > static_cast<int64_t>(std::numeric_limits<To>::max())) {
      return caf::make_error(ec::convert_error,
                             fmt::format(": {} can not be represented by the "
                                         "target variable [{}, {}]",
                                         src, std::numeric_limits<To>::min(),
                                         std::numeric_limits<To>::max()));
    }
    dst = detail::narrow_cast<To>(src);
  }
  return caf::none;
}

// Overload for integers.
template <std::signed_integral To>
caf::error convert(const int64_t& src, To& dst, const int64_type&) {
  if constexpr (sizeof(To) >= sizeof(int64_t)) {
    dst = src;
  } else {
    if (src < std::numeric_limits<To>::min()
        || src > std::numeric_limits<To>::max()) {
      return caf::make_error(ec::convert_error,
                             fmt::format(": {} can not be represented by the "
                                         "target variable [{}, {}]",
                                         src, std::numeric_limits<To>::min(),
                                         std::numeric_limits<To>::max()));
    }
    dst = detail::narrow_cast<To>(src);
  }
  return caf::none;
}

// Overload for enums.
template <class To>
  requires(std::is_enum_v<To>)
caf::error convert(const std::string& src, To& dst, const enumeration_type& t) {
  for (const auto& [canonical, internal] : t.fields()) {
    if (src == canonical) {
      dst = detail::narrow_cast<To>(internal);
      return caf::none;
    }
  }
  return caf::make_error(ec::convert_error,
                         fmt::format(": {} is not a value of {}", src,
                                     detail::pretty_type_name(dst)));
}

template <class From, class To, class Type>
caf::error convert(const From& src, std::optional<To>& dst, const Type& t) {
  if (!dst) {
    dst = To{};
  }
  if constexpr (IS_TYPED_CONVERTIBLE(src, *dst, t)) {
    return convert(src, *dst, t);
  } else {
    return convert(src, *dst, type{t});
  }
}

template <class From, class To, class Type>
caf::error convert(const From& src, caf::optional<To>& dst, const Type& t) {
  if (!dst) {
    dst = To{};
  }
  if constexpr (IS_TYPED_CONVERTIBLE(src, *dst, t)) {
    return convert(src, *dst, t);
  } else {
    return convert(src, *dst, type{t});
  }
}

// Overload for lists.
template <concepts::appendable To>
caf::error convert(const list& src, To& dst, const list_type& t) {
  size_t num = 0;
  for (const auto& element : src) {
    typename To::value_type v{};
    if (auto err = convert(element, v, t.value_type())) {
      return detail::prepend(std::move(err), "[{}]", num);
    }
    dst.push_back(std::move(v));
    num++;
  }
  return caf::none;
}

// Overload for maps.
// clang-format off
template <concepts::insertable To>
  requires requires {
    typename To::key_type;
    typename To::mapped_type;
  }
// clang-format on
caf::error convert(const map& src, To& dst, const map_type& t) {
  // TODO: Use structured bindings outside of the lambda once clang supports
  // that.
  for (const auto& x : src) {
    auto err = [&] {
      const auto& [data_key, data_value] = x;
      typename To::key_type key{};
      if (auto err = convert(data_key, key, t.key_type())) {
        return err;
      }
      typename To::mapped_type value{};
      if (auto err = convert(data_value, value, t.value_type())) {
        return err;
      }
      return detail::insert_to_map(dst, std::move(key), std::move(value));
    }();
    if (err) {
      return detail::prepend(std::move(err), ".{}", x.first);
    }
  }
  return caf::none;
}

// Overload for record to map.
template <concepts::insertable To>
  requires requires {
    typename To::key_type;
    typename To::mapped_type;
  }
caf::error convert(const record& src, To& dst, const map_type& t) {
  // TODO: Use structured bindings outside of the lambda once clang supports
  // that.
  const auto kt = t.key_type();
  const auto vt = t.value_type();
  if (auto key = kt.attribute("key")) {
    return caf::make_error(ec::convert_error,
                           fmt::format("expected a list of records with the "
                                       "key field {}, but received record {}",
                                       *key, src));
  }
  for (const auto& x : src) {
    auto err = [&] {
      const auto& [data_key, data_value] = x;
      typename To::key_type key{};
      if (auto err = convert(data_key, key, kt)) {
        return err;
      }
      typename To::mapped_type value{};
      if (auto err = convert(data_value, value, vt)) {
        return err;
      }
      return detail::insert_to_map(dst, std::move(key), std::move(value));
    }();
    if (err) {
      return detail::prepend(std::move(err), ".{}", x.first);
    }
  }
  return caf::none;
}

// Overload for list<record> to map.
// NOTE: This conversion type needs a field with the "key" attribute in the
// record_type. The field with the "key" attribute is pulled out and used
// as the key for the new entry in the destination map.
template <concepts::insertable To>
  requires requires {
    typename To::key_type;
    typename To::mapped_type;
  }
caf::error convert(const list& src, To& dst, const map_type& t) {
  const auto kt = t.key_type();
  const auto vt = t.value_type();
  const auto* rvt = try_as<record_type>(&vt);
  if (!rvt) {
    return caf::make_error(ec::convert_error,
                           fmt::format(": expected a record_type, but got {}",
                                       vt));
  }
  auto key_field_name = kt.attribute("key");
  if (!key_field_name) {
    return caf::make_error(
      ec::convert_error,
      fmt::format(": record type in list is missing a key field: {}", *rvt));
  }
  // We now iterate over all elements in src, converting both key from the key
  // field and and value from the pruned record type.
  for (const auto& element : src) {
    const auto* element_rec = try_as<record>(&element);
    if (!element_rec) {
      return caf::make_error(ec::convert_error, ": expected record");
    }
    auto record_resolve_key
      = [](const auto& self, const record& d,
           std::string_view name) -> const record::value_type* {
      for (const auto& e : d) {
        const auto& [k, v] = e;
        auto [key_mismatch, name_mismatch]
          = std::mismatch(k.begin(), k.end(), name.begin(), name.end());
        if (key_mismatch != k.end()) {
          continue;
        }
        if (name_mismatch == name.end()) {
          return &e;
        }
        if (*name_mismatch != '.') {
          continue;
        }
        if (const auto* rv = try_as<record>(&v)) {
          auto remainder = name.substr(1 + name_mismatch - name.begin());
          if (auto result = self(self, *rv, remainder)) {
            return result;
          }
        }
      }
      return nullptr;
    };
    auto key
      = record_resolve_key(record_resolve_key, *element_rec, *key_field_name);
    if (!key) {
      continue;
    }
    typename To::key_type key_dst{};
    if (auto err = convert(key->second, key_dst, kt)) {
      return caf::make_error(
        ec::convert_error,
        fmt::format("failed to convert map key {} of type "
                    "{} to {}: {}",
                    key->second, kt, detail::pretty_type_name(key_dst), err));
    }
    typename To::mapped_type value_dst{};
    auto stripped_record_prefix = *key_field_name;
    stripped_record_prefix.remove_suffix(key->first.size() + 1);
    if (stripped_record_prefix.empty()) {
      if (auto err = convert(*element_rec, value_dst, *rvt)) {
        return caf::make_error(ec::convert_error,
                               fmt::format("failed to convert map value {} of "
                                           "type {} to {}: {}",
                                           *element_rec, *rvt,
                                           detail::pretty_type_name(value_dst),
                                           err));
      }
    } else {
      // We need to strip an outer layer before handling the value of the map.
      auto stripped_vt_offset = rvt->resolve_key(stripped_record_prefix);
      if (!stripped_vt_offset) {
        return caf::make_error(
          ec::convert_error,
          fmt::format("failed to strip outer record {} from {} for key {}",
                      stripped_record_prefix, *rvt, *key_field_name));
      }
      auto stripped_vt = rvt->field(*stripped_vt_offset);
      // This cannot fail, we already know we can handle the full name, so we
      // can also handle a prefix to get the record.
      auto value = record_resolve_key(record_resolve_key, *element_rec,
                                      stripped_record_prefix);
      TENZIR_ASSERT(value);
      if (auto err = convert(value->second, value_dst, stripped_vt.type)) {
        return caf::make_error(ec::convert_error,
                               fmt::format("failed to convert stripped map "
                                           "value {} of type {} to {}: {}",
                                           value->second, stripped_vt.type,
                                           detail::pretty_type_name(value_dst),
                                           err));
      }
    }
    if (auto err = detail::insert_to_map(dst, std::move(key_dst),
                                         std::move(value_dst))) {
      return err;
    }
  }
  return caf::none;
}

class record_inspector {
public:
  static constexpr bool is_loading = true;

  template <class To>
  caf::error apply(const record_type::field_view& field, To& dst) {
    // Find the value from the record
    auto it = src.find(field.name);
    const auto& value = it != src.end() ? it->second : data{};
    if (!field.type && caf::holds_alternative<caf::none_t>(value)) {
      return caf::make_error(ec::convert_error, fmt::format("failed to convert "
                                                            "field {} because "
                                                            "it has no type",
                                                            field));
    }
    return match(
      std::tie(value, field.type),
      [&]<concrete_type Type>(const caf::none_t&, const Type&) -> caf::error {
        // If the data is null then we leave the value untouched.
        return caf::none;
      },
      [&]<class Data, concrete_type Type>(const Data& d,
                                          const Type& t) -> caf::error {
        if constexpr (IS_TYPED_CONVERTIBLE(d, dst, t)) {
          return convert(d, dst, t);
        } else if constexpr (IS_UNTYPED_CONVERTIBLE(d, dst)) {
          return convert(d, dst);
        } else {
          return caf::make_error(ec::convert_error,
                                 fmt::format("failed to find conversion "
                                             "operation for value {} of "
                                             "type {} to {}",
                                             data{d}, t,
                                             detail::pretty_type_name(dst)));
        }
      });
  }

  template <class T>
  bool apply(T& x) {
    auto err = apply(*current_iterator_, x);
    ++current_iterator_;
    if (err) {
      TENZIR_WARN("{}", err);
      return false;
    }
    return true;
  }

  template <class T>
  auto field(std::string_view, T& value) {
    return detail::inspection_field{value};
  }

  template <class T>
  auto object(const T&) {
    return detail::inspection_object(*this);
  }

  const record_type& schema;
  const record& src;
  generator<record_type::field_view> field_generator_ = schema.fields();
  generator<record_type::field_view>::iterator current_iterator_
    = field_generator_.begin();
};

// Overload for records.
caf::error convert(const record& src, concepts::inspectable auto& dst,
                   const record_type& schema) {
  auto ri = record_inspector{schema, src};
  if (auto result = inspect(ri, dst); !result) {
    return caf::make_error(ec::convert_error,
                           fmt::format("record inspection failed for record {} "
                                       "with schema {}",
                                       src, schema));
  }
  return {};
}

template <has_schema To>
caf::error convert(const record& src, To& dst) {
  return convert(src, dst, dst.schema());
}

template <has_schema To>
caf::error convert(const data& src, To& dst) {
  if (const auto* r = try_as<record>(&src)) {
    return convert(*r, dst);
  }
  return caf::make_error(ec::convert_error,
                         fmt::format(": expected record, but got {}", src));
}

// TODO: Move to a dedicated header after conversion is refactored to use
// specialization.
template <registered_parser_type To>
caf::error convert(std::string_view src, To& dst) {
  const auto* f = src.begin();
  if (!parse(f, src.end(), dst)) {
    return caf::make_error(ec::convert_error,
                           fmt::format(": unable to parse \"{}\" into a {}",
                                       src, detail::pretty_type_name(dst)));
  }
  return caf::none;
}

// A concept to detect whether any previously declared overloads of
// `convert` can be used for a combination of `Type`, `From`, and `To`.
template <class From, class To, class Type>
concept is_concrete_typed_convertible
  = requires(const From& src, To& dst, const Type& type) {
      { tenzir::convert(src, dst, type) } -> std::same_as<caf::error>;
    };

// The same concept but this time to check for any untyped convert
// overloads.
template <class From, class To>
concept is_concrete_untyped_convertible = requires(const From& src, To& dst) {
  { tenzir::convert(src, dst) } -> std::same_as<caf::error>;
};

// NOTE: This overload has to be last because we need to be able to detect
// all other overloads with `is_concrete_convertible`. At the same time,
// it must not be declared before to prevent recursing into itself because
// of the non-explicit constructor of `data`.
template <class To>
caf::error convert(const data& src, To& dst, const type& t) {
  return match(std::tie(src, t), [&]<class From, class Type>(const From& x,
                                                             const Type& t) {
    if constexpr (is_concrete_typed_convertible<From, To, Type>) {
      return convert(x, dst, t);
    } else if constexpr (is_concrete_untyped_convertible<From, To>) {
      return convert(x, dst);
    } else {
      return caf::make_error(ec::convert_error,
                             fmt::format("can't convert from {} to {} with "
                                         "type {}",
                                         detail::pretty_type_name(x),
                                         detail::pretty_type_name(dst), t));
    }
  });
}

} // namespace tenzir
