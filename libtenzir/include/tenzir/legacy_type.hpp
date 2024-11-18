//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/concept/printable/print.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/operators.hpp"
#include "tenzir/detail/range.hpp"
#include "tenzir/detail/stack_vector.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/time.hpp"
#include "tenzir/variant.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/detail/apply_args.hpp>
#include <caf/detail/int_list.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/error.hpp>
#include <caf/intrusive_cow_ptr.hpp>
#include <caf/make_counted.hpp>
#include <caf/none.hpp>
#include <caf/optional.hpp>
#include <caf/ref_counted.hpp>
#include <caf/sum_type.hpp>
#include <fmt/core.h>

#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace tenzir {

/// A qualifier in the form of a key and optional value.
struct legacy_attribute : detail::totally_ordered<legacy_attribute> {
  legacy_attribute(std::string key = {});
  legacy_attribute(std::string key, caf::optional<std::string> value);

  friend bool operator==(const legacy_attribute& x, const legacy_attribute& y);

  friend bool operator<(const legacy_attribute& x, const legacy_attribute& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_attribute& a) -> bool {
    return detail::apply_all(f, a.key, a.value);
  }

  std::string key;
  caf::optional<std::string> value;
};

// -- type hierarchy ----------------------------------------------------------

// clang-format off
/// @relates type
using legacy_concrete_types = caf::detail::type_list<
  legacy_none_type,
  legacy_bool_type,
  legacy_integer_type,
  legacy_count_type,
  legacy_real_type,
  legacy_duration_type,
  legacy_time_type,
  legacy_string_type,
  legacy_pattern_type,
  legacy_address_type,
  legacy_subnet_type,
  legacy_enumeration_type,
  legacy_list_type,
  legacy_map_type,
  legacy_record_type,
  legacy_alias_type
>;
// clang-format on

using type_id_type = int8_t;

constexpr type_id_type invalid_type_id = -1;

template <class T>
constexpr type_id_type type_id() {
  static_assert(detail::contains_type_v<legacy_concrete_types, T>,
                "type IDs only available for concrete types");
  return caf::detail::tl_index_of<legacy_concrete_types, T>::value;
}

// -- type ------------------------------------------------------------------

/// @relates type
using legacy_abstract_type_ptr = caf::intrusive_cow_ptr<legacy_abstract_type>;

/// The sematic representation of data.
class legacy_type : detail::totally_ordered<legacy_type> {
public:
  // -- construction & assignment ---------------------------------------------

  /// Constructs an invalid type.
  legacy_type() noexcept = default;

  /// Constructs a type from a concrete instance.
  /// @tparam T a type that derives from @ref legacy_abstract_type.
  /// @param x An instance of a type.
  template <class T>
    requires(detail::contains_type_v<legacy_concrete_types, T>)
  legacy_type(T x) : ptr_{caf::make_counted<T>(std::move(x))} {
    // nop
  }

  /// Copy-constructs a type.
  legacy_type(const legacy_type& x) = default;

  /// Copy-assigns a type.
  legacy_type& operator=(const legacy_type& x) = default;

  /// Move-constructs a type.
  legacy_type(legacy_type&&) noexcept = default;

  /// Move-assigns a type.
  legacy_type& operator=(legacy_type&&) noexcept = default;

  ~legacy_type() noexcept = default;

  /// Assigns a type from another instance
  template <class T>
    requires(detail::contains_type_v<legacy_concrete_types, T>)
  legacy_type& operator=(T x) {
    ptr_ = caf::make_counted<T>(std::move(x));
    return *this;
  }

  // -- modifiers -------------------------------------------------------------

  /// Sets the type name.
  /// @param x The new name of the type.
  legacy_type& name(const std::string& x) &;

  /// Sets the type name.
  /// @param x The new name of the type.
  legacy_type name(const std::string& x) &&;

  /// Inserts a list of attributes, updating already existing keys with new
  /// values.
  /// @param xs The list of attributes.
  legacy_type& update_attributes(std::vector<legacy_attribute> xs) &;

  /// Inserts a list of attributes, updating already existing keys with new
  /// values.
  /// @param xs The list of attributes.
  legacy_type update_attributes(std::vector<legacy_attribute> xs) &&;

  // -- inspectors ------------------------------------------------------------

  /// Checks whether a type contains a valid type.
  /// @returns `true` iff the type contains an instantiated type.
  explicit operator bool() const;

  /// @returns the name of the type.
  [[nodiscard]] const std::string& name() const;

  /// @returns The attributes of the type.
  [[nodiscard]] const std::vector<legacy_attribute>& attributes() const;

  /// @cond PRIVATE

  [[nodiscard]] legacy_abstract_type_ptr ptr() const;

  [[nodiscard]] const legacy_abstract_type* raw_ptr() const noexcept;

  const legacy_abstract_type* operator->() const noexcept;

  const legacy_abstract_type& operator*() const noexcept;

  struct inspect_helper {
    type_id_type& type_tag;
    legacy_type& x;

    template <class Inspector>
    friend auto inspect(Inspector& f, inspect_helper& x) -> bool;
  };

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_type& x) -> bool;

  /// @endcond

  friend bool operator==(const legacy_type& x, const legacy_type& y);
  friend bool operator<(const legacy_type& x, const legacy_type& y);

private:
  legacy_type(legacy_abstract_type_ptr x);

  legacy_abstract_type_ptr ptr_;
};

/// Retrieves kind of a type, e.g., `string` for `legacy_string_type`.
/// @param x The type to query.
/// @param the kind of *x*.
/// @relates type
std::string kind(const legacy_type& x);

/// The abstract base class for all types.
/// @relates type
class legacy_abstract_type : public caf::ref_counted,
                             detail::totally_ordered<legacy_abstract_type> {
  friend legacy_type; // to change name/attributes of a copy.

public:
  virtual ~legacy_abstract_type();

  // -- introspection ---------------------------------------------------------

  friend bool
  operator==(const legacy_abstract_type& x, const legacy_abstract_type& y);
  friend bool
  operator<(const legacy_abstract_type& x, const legacy_abstract_type& y);

  /// @cond PRIVATE

  /// @returns the index of this type in `legacy_concrete_types`.
  virtual int index() const noexcept = 0;

  virtual legacy_abstract_type* copy() const = 0;

  /// @endcond
  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_abstract_type& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.abstract_type")
      .fields(f.field("name", x.name_), f.field("attributes", x.attributes_));
  }

  friend auto inspect(caf::binary_deserializer&, legacy_abstract_type&) -> bool
    = delete;

protected:
  virtual bool equals(const legacy_abstract_type& other) const;

  virtual bool less_than(const legacy_abstract_type& other) const;

  std::string name_;
  std::vector<legacy_attribute> attributes_;
};

/// The base class for all concrete types.
/// @relates type
template <class Derived>
class legacy_concrete_type : public legacy_abstract_type,
                             detail::totally_ordered<Derived, Derived> {
public:
  /// @returns the name of the type.
  const std::string& name() const {
    return this->name_;
  }

  /// Sets the type name.
  /// @param x The new name of the type.
  Derived& name(std::string x) & {
    this->name_ = std::move(x);
    return derived();
  }

  /// Sets the type name.
  /// @param x The new name of the type.
  Derived name(const std::string& x) && {
    this->name_ = x;
    return std::move(derived());
  }

  /// @returns The attributes of the type.
  const std::vector<legacy_attribute>& attributes() const {
    return this->attributes_;
  }

  Derived& attributes(std::vector<legacy_attribute> xs) & {
    this->attributes_ = std::move(xs);
    return derived();
  }

  Derived attributes(std::vector<legacy_attribute> xs) && {
    this->attributes_ = std::move(xs);
    return std::move(derived());
  }

  Derived& update_attributes(std::vector<legacy_attribute> xs) & {
    auto& attrs = this->attributes_;
    for (auto& x : xs) {
      auto i = std::find_if(attrs.begin(), attrs.end(), [&](auto& attr) {
        return attr.key == x.key;
      });
      if (i == attrs.end())
        attrs.push_back(std::move(x));
      else
        i->value = std::move(x).value;
    }
    return derived();
  }

  Derived update_attributes(std::vector<legacy_attribute> xs) && {
    auto& attrs = this->attributes_;
    for (auto& x : xs) {
      auto i = std::find_if(attrs.begin(), attrs.end(), [&](auto& attr) {
        return attr.key == x.key;
      });
      if (i == attrs.end())
        attrs.push_back(std::move(x));
      else
        i->value = std::move(x).value;
    }
    return std::move(derived());
  }

  friend bool operator==(const Derived& x, const Derived& y) {
    return x.equals(y);
  }

  friend bool operator<(const Derived& x, const Derived& y) {
    return x.less_than(y);
  }

  int index() const noexcept final {
    return type_id<Derived>();
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_concrete_type& x) -> bool {
    auto name = std::string_view{};
    if constexpr (std::is_same_v<Derived, legacy_none_type>) {
      name = "tenzir.none_type";
    } else if constexpr (std::is_same_v<Derived, legacy_bool_type>) {
      name = "tenzir.bool_type";
    } else if constexpr (std::is_same_v<Derived, legacy_integer_type>) {
      name = "tenzir.integer_type";
    } else if constexpr (std::is_same_v<Derived, legacy_count_type>) {
      name = "tenzir.count_type";
    } else if constexpr (std::is_same_v<Derived, legacy_real_type>) {
      name = "tenzir.real_type";
    } else if constexpr (std::is_same_v<Derived, legacy_duration_type>) {
      name = "tenzir.duration_type";
    } else if constexpr (std::is_same_v<Derived, legacy_time_type>) {
      name = "tenzir.time_type";
    } else if constexpr (std::is_same_v<Derived, legacy_string_type>) {
      name = "tenzir.string_type";
    } else if constexpr (std::is_same_v<Derived, legacy_pattern_type>) {
      name = "tenzir.pattern_type";
    } else if constexpr (std::is_same_v<Derived, legacy_address_type>) {
      name = "tenzir.address_type";
    } else if constexpr (std::is_same_v<Derived, legacy_subnet_type>) {
      name = "tenzir.subnet_type";
    } else if constexpr (std::is_same_v<Derived, legacy_enumeration_type>) {
      name = "tenzir.enumeration_type";
    } else if constexpr (std::is_same_v<Derived, legacy_list_type>) {
      name = "tenzir.list_type";
    } else if constexpr (std::is_same_v<Derived, legacy_map_type>) {
      name = "tenzir.map_type";
    } else if constexpr (std::is_same_v<Derived, legacy_record_type>) {
      name = "tenzir.record_type";
    } else if constexpr (std::is_same_v<Derived, legacy_alias_type>) {
      name = "tenzir.alias_type";
    } else {
      static_assert(detail::always_false_v<Derived>, "cannot inspect non-leaf "
                                                     "type");
    }
    TENZIR_ASSERT(!name.empty());
    auto tid = type_id<Derived>();
    return f.object(x).pretty_name(name).fields(
      f.field("type-id", tid), f.field("name", x.name_),
      f.field("attributes", x.attributes_));
  }

protected:
  // Convenience function to cast an abstract type into an instance of this
  // type. Useful in, e.g., the implementation of comparison operators.
  static const Derived& downcast(const legacy_abstract_type& x) {
    TENZIR_ASSERT(dynamic_cast<const Derived*>(&x) != nullptr);
    return static_cast<const Derived&>(x);
  }

  template <class T>
  static legacy_concrete_type<Derived>& upcast(T& x) {
    return static_cast<legacy_concrete_type&>(x);
  }

  legacy_concrete_type* copy() const final {
    return new Derived(derived());
  }

private:
  Derived& derived() {
    return *static_cast<Derived*>(this);
  }

  const Derived& derived() const {
    return *static_cast<const Derived*>(this);
  }
};

/// A type that does not depend on runtime information.
/// @relates type
template <class Derived>
struct legacy_basic_type : legacy_concrete_type<Derived> {
  using super = legacy_concrete_type<Derived>;

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_basic_type<Derived>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

/// The base type for types that depend on runtime information.
/// @relates legacy_basic_type type
template <class Derived>
struct legacy_complex_type : legacy_concrete_type<Derived> {
  using super = legacy_concrete_type<Derived>;

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_complex_type<Derived>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

/// The base type for types that contain nested types.
/// @relates type
template <class Derived>
struct legacy_recursive_type : legacy_complex_type<Derived> {
  using super = legacy_complex_type<Derived>;

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_recursive_type<Derived>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

/// The base type for types that a single nested type.
template <class Derived>
struct legacy_nested_type : legacy_recursive_type<Derived> {
  friend class legacy_concrete_type<Derived>; // equals/less_than
  using super = legacy_recursive_type<Derived>;

  explicit legacy_nested_type(legacy_type t = {}) : value_type{std::move(t)} {
    // nop
  }

  legacy_type value_type; ///< The type of the enclosed element(s).

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_nested_type<Derived>& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.nested_type")
      .fields(f.field("value", static_cast<super&>(x)),
              f.field("value-type", x.value_type));
  }

  [[nodiscard]] bool equals(const legacy_abstract_type& other) const final {
    return super::equals(other)
           && value_type == super::downcast(other).value_type;
  }

  [[nodiscard]] bool less_than(const legacy_abstract_type& other) const final {
    return super::less_than(other)
           && value_type < super::downcast(other).value_type;
  }
};

// -- leaf types --------------------------------------------------------------

/// Represents a default constructed type.
struct legacy_none_type final : legacy_basic_type<legacy_none_type> {};

/// A type for true/false data.
/// @relates type
struct legacy_bool_type final : legacy_basic_type<legacy_bool_type> {};

/// A type for positive and negative integers.
/// @relates type
struct legacy_integer_type final : legacy_basic_type<legacy_integer_type> {};

/// A type for positive integers.
/// @relates type
struct legacy_count_type final : legacy_basic_type<legacy_count_type> {};

/// A type for floating point numbers.
/// @relates type
struct legacy_real_type final : legacy_basic_type<legacy_real_type> {};

/// A type for time durations.
/// @relates type
struct legacy_duration_type final : legacy_basic_type<legacy_duration_type> {};

/// A type for absolute points in time.
/// @relates type
struct legacy_time_type final : legacy_basic_type<legacy_time_type> {};

/// A string type for sequence of characters.
struct legacy_string_type final : legacy_basic_type<legacy_string_type> {};

/// A type for regular expressions.
/// @relates type
struct legacy_pattern_type final : legacy_basic_type<legacy_pattern_type> {};

/// A type for IP addresses, both v4 and v6.
/// @relates type
struct legacy_address_type final : legacy_basic_type<legacy_address_type> {};

/// A type for IP prefixes.
/// @relates type
struct legacy_subnet_type final : legacy_basic_type<legacy_subnet_type> {};

/// The enumeration type consisting of a fixed number of strings.
/// @relates type
struct legacy_enumeration_type final
  : legacy_complex_type<legacy_enumeration_type> {
  using super = legacy_complex_type<legacy_enumeration_type>;

  explicit legacy_enumeration_type(std::vector<std::string> xs = {})
    : fields{std::move(xs)} {
    // nop
  }

  std::vector<std::string> fields;

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_enumeration_type& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.enumeration_type")
      .fields(f.field("value", super::upcast(x)), f.field("fields", x.fields));
  }

  bool equals(const legacy_abstract_type& other) const final {
    return super::equals(other) && fields == downcast(other).fields;
  }

  bool less_than(const legacy_abstract_type& other) const final {
    return super::less_than(other) && fields < downcast(other).fields;
  }
};

/// A type representing a sequence of elements.
/// @relates type
struct legacy_list_type final : legacy_nested_type<legacy_list_type> {
  using super = legacy_nested_type<legacy_list_type>;
  using super::super;

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_list_type& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

/// A type representinng an associative array.
struct legacy_map_type final : legacy_recursive_type<legacy_map_type> {
  using super = legacy_recursive_type<legacy_map_type>;

  explicit legacy_map_type(legacy_type key = {}, legacy_type value = {})
    : key_type{std::move(key)}, value_type{std::move(value)} {
    // nop
  }

  legacy_type key_type;   ///< The type of the map keys.
  legacy_type value_type; ///< The type of the map values.

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_map_type& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.map_type")
      .fields(f.field("value", super::upcast(x)),
              f.field("key-type", x.key_type),
              f.field("value-type", x.value_type));
  }

  bool equals(const legacy_abstract_type& other) const final {
    return super::equals(other) && key_type == downcast(other).key_type
           && value_type == downcast(other).value_type;
  }

  bool less_than(const legacy_abstract_type& other) const final {
    return super::less_than(other)
           && std::tie(key_type, downcast(other).key_type)
                < std::tie(value_type, downcast(other).value_type);
  }
};

/// A field of a record.
/// @relates legacy_record_type
struct record_field : detail::totally_ordered<record_field> {
  record_field() noexcept = default;
  ~record_field() noexcept = default;
  record_field(const record_field&) = default;
  record_field(record_field&&) noexcept = default;
  record_field& operator=(const record_field&) = default;
  record_field& operator=(record_field&&) noexcept = default;

  explicit record_field(std::string name) noexcept : name{std::move(name)} {
    // nop
  }

  record_field(std::string name, tenzir::legacy_type type) noexcept
    : name{std::move(name)}, type{std::move(type)} {
    // nop
  }

  std::string name;       ///< The name of the field.
  tenzir::legacy_type type; ///< The type of the field.

  friend bool operator==(const record_field& x, const record_field& y);
  friend bool operator<(const record_field& x, const record_field& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, record_field& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.record_field")
      .fields(f.field("name", x.name), f.field("type", x.type));
  }
};

/// A sequence of fields, where each fields has a name and a type.
struct legacy_record_type final : legacy_recursive_type<legacy_record_type> {
  using super = legacy_recursive_type<legacy_record_type>;

  legacy_record_type() = default;
  legacy_record_type(const legacy_record_type&) = default;
  legacy_record_type(legacy_record_type&&) = default;
  legacy_record_type& operator=(const legacy_record_type&) = default;
  legacy_record_type& operator=(legacy_record_type&&) = default;
  ~legacy_record_type() noexcept = default;

  /// Constructs a record type from a list of fields.
  explicit legacy_record_type(std::vector<record_field> xs) noexcept;

  /// Constructs a record type from a list of fields.
  legacy_record_type(std::initializer_list<record_field> xs) noexcept;

  friend bool
  operator==(const legacy_record_type& x, const legacy_record_type& y);
  friend bool
  operator<(const legacy_record_type& x, const legacy_record_type& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_record_type& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.record_type")
      .fields(f.field("value", upcast(x)), f.field("fields", x.fields));
  }

  std::vector<record_field> fields;

  bool equals(const legacy_abstract_type& other) const final;

  bool less_than(const legacy_abstract_type& other) const final;
};

/// An alias of another type.
/// @relates type
struct legacy_alias_type final : legacy_nested_type<legacy_alias_type> {
  using super = legacy_nested_type<legacy_alias_type>;
  using super::super;

  template <class Inspector>
  friend auto inspect(Inspector& f, legacy_alias_type& x) -> bool {
    return inspect(f, static_cast<super&>(x));
  }
};

// -- free functions ----------------------------------------------------------

/// Creates a new unnamed legacy_record_type containing the fields and attribues
/// of lhs and rhs. Errors if a field of the same name but different types is
/// present in both inputs. Errors is the inputs disagree over the value of an
/// attribute with a certain name.
/// @returns The combined legacy_record_type.
/// @relates legacy_record_type
caf::expected<legacy_record_type>
merge(const legacy_record_type& lhs, const legacy_record_type& rhs);

/// @relates priority_merge
enum class merge_policy { prefer_left, prefer_right };

/// Creates a new unnamed legacy_record_type containing the fields and attribues
/// of lhs and rhs. Uses a merge_policy to decide wheter to use a field from lhs
/// or rhs in case of a conflict.
/// @returns The combined legacy_record_type.
/// @relates legacy_record_type merge_policy
legacy_record_type
priority_merge(const legacy_record_type& lhs, const legacy_record_type& rhs,
               merge_policy p);

/// Removes a field from a legacy_record_type by name.
/// @param r The record to mutate.
/// @param path The sequence of keys pointing to the target field.
/// @returns A new type without the target field if it exists in `r`.
/// @pre `!path.empty()`
/// @relates legacy_record_type
std::optional<legacy_record_type>
remove_field(const legacy_record_type& r, std::vector<std::string_view> path);

/// As above, but use an offset instead of a vector of string to specify
/// the field to be removed.
std::optional<legacy_record_type>
remove_field(const legacy_record_type& r, offset o);

template <>
class variant_traits<legacy_type> {
public:
  static constexpr auto count
    = caf::detail::tl_size<legacy_concrete_types>::value;

  static auto index(const legacy_type& x) -> size_t {
    return detail::narrow<size_t>(x->index());
  }

  template <size_t I>
  static auto get(const legacy_type& x) -> decltype(auto) {
    return static_cast<const caf::detail::tl_at_t<legacy_concrete_types, I>&>(
      *x);
  }
};

} // namespace tenzir

// -- helpers ----------------------------------------------------------------

namespace caf {

template <class Result, class Dispatcher, class T>
auto make_dispatch_fun() {
  using fun = Result (*)(Dispatcher*, const tenzir::legacy_abstract_type&);
  auto lambda = [](Dispatcher* d, const tenzir::legacy_abstract_type& ref) {
    return d->invoke(static_cast<const T&>(ref));
  };
  return static_cast<fun>(lambda);
}

template <>
struct sum_type_access<tenzir::legacy_type> {
  using types = tenzir::legacy_concrete_types;

  using type0 = tenzir::legacy_none_type;

  static constexpr bool specialized = true;

  template <class T, int Pos>
  static bool is(const tenzir::legacy_type& x, sum_type_token<T, Pos>) {
    return x->index() == Pos;
  }

  template <class T, int Pos>
  static const T& get(const tenzir::legacy_type& x, sum_type_token<T, Pos>) {
    return static_cast<const T&>(*x);
  }

  template <class T, int Pos>
  static const T* get_if(const tenzir::legacy_type* x, sum_type_token<T, Pos>) {
    auto ptr = x->raw_ptr();
    return ptr->index() == Pos ? static_cast<const T*>(ptr) : nullptr;
  }

  template <class Result, class Visitor, class... Ts>
  struct dispatcher {
    using const_reference = const tenzir::legacy_abstract_type&;
    template <class... Us>
    Result dispatch(const_reference x, caf::detail::type_list<Us...>) {
      using fun = Result (*)(dispatcher*, const_reference);
      static fun tbl[] = {make_dispatch_fun<Result, dispatcher, Us>()...};
      return tbl[x.index()](this, x);
    }

    template <class T>
    Result invoke(const T& x) {
      auto is = caf::detail::get_indices(xs_);
      return caf::detail::apply_args_suffxied(v, is, xs_, x);
    }

    Visitor& v;
    std::tuple<Ts&...> xs_;
  };

  template <class Result, class Visitor, class... Ts>
  static Result apply(const tenzir::legacy_type& x, Visitor&& v, Ts&&... xs) {
    dispatcher<Result, decltype(v), decltype(xs)...> d{
      v, std::forward_as_tuple(xs...)};
    types token;
    return d.dispatch(*x, token);
  }
};

} // namespace caf

// -- inspect (needs caf::visit first) -----------------------------------------

namespace tenzir {

/// @private
template <class Inspector, class T>
auto make_inspect_fun() {
  using fun = bool (*)(Inspector&, legacy_type&);
  auto lambda = [](Inspector& g, legacy_type& ref) {
    auto tmp = T{};
    auto res
      = g.object(tmp).pretty_name("tenzir.type").fields(g.field("value", tmp));
    ref = std::move(tmp);
    return res;
  };
  return static_cast<fun>(lambda);
}

/// @private
template <class Inspector, class... Ts>
auto make_inspect(caf::detail::type_list<Ts...>) {
  return [](Inspector& f, legacy_type::inspect_helper& x) -> bool {
    if constexpr (!Inspector::is_loading) {
      if (x.type_tag != invalid_type_id) {
        return match(x.x, [&f](auto& v) -> bool {
            return f.apply(v);
          });
      }
      return true;
    } else {
      using reference = legacy_type&;
      using fun = bool (*)(Inspector&, reference);
      static fun tbl[] = {make_inspect_fun<Inspector, Ts>()...};
      if (x.type_tag != invalid_type_id)
        return tbl[x.type_tag](f, x.x);
      x.x = legacy_type{};
      return true;
    }
  };
}

/// @relates type
template <class Inspector>
auto inspect(Inspector& f, legacy_type::inspect_helper& x) -> bool {
  auto g = make_inspect<Inspector>(legacy_concrete_types{});
  return g(f, x);
}

/// @relates type
template <class Inspector>
auto inspect(Inspector& f, legacy_type& x) -> bool {
  // We use a single byte for the type index on the wire.
  auto type_tag = x ? static_cast<type_id_type>(x->index()) : invalid_type_id;
  legacy_type::inspect_helper helper{type_tag, x};
  return f.object(x)
    .pretty_name("tenzir.type")
    .fields(f.field("type-tag", type_tag), f.field("value", helper));
}

} // namespace tenzir
