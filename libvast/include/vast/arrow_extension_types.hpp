//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/type.hpp"

#include <arrow/array.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <caf/detail/type_list.hpp>

namespace vast {

/// Enum representation in the Arrow type system, utilizing an extension type.
/// The underlying data is represented as a dictionary, where the `dict` part
/// contains all the possible variants specified in the underlying VAST enum.
class enum_extension_type : public arrow::ExtensionType {
public:
  static constexpr auto vast_id = "vast.enum";

  static const std::shared_ptr<arrow::DataType> arrow_type;

  /// Wrap the provided `enumeration_type` into an `arrow::ExtensionType`.
  /// @param enum_type VAST enum type to wrap.
  explicit enum_extension_type(enumeration_type enum_type);

  /// Unique name to identify the extension type, `vast.enum`.
  std::string extension_name() const override;

  /// Compare two extension types for equality, based on the wrapped enum.
  /// @param other An extension type to test for equality.
  bool ExtensionEquals(const ExtensionType& other) const override;

  /// Create a string representation that contains the wrapped enum.
  std::string ToString() const override;

  /// Wrap built-in Array type in an ExtensionArray instance
  /// @param data the physical storage for the extension type
  std::shared_ptr<arrow::Array>
  MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

  /// Create an instance of enum_extension_type given the actual storage type
  /// and the serialized representation.
  /// @param storage_type the physical storage type of the extension
  /// @param serialized the json representation describing the enum fields.
  arrow::Result<std::shared_ptr<DataType>>
  Deserialize(std::shared_ptr<DataType> storage_type,
              const std::string& serialized) const override;

  /// Create a serialized json representation of the underlying enum fields.
  /// @return the serialized representation.
  std::string Serialize() const override;

  /// Get the wrapped `enumeration_type`.
  /// @return the VAST `enumeration_type` represented by this Arrow type.
  enumeration_type get_enum_type() const;

private:
  enumeration_type enum_type_;
};

/// Address representation as an Arrow extension type.
/// Internal (physical) representation is a 16-byte fixed binary.
class ip_extension_type : public arrow::ExtensionType {
public:
  // NOTE: The identifier for the extension type of VAST's ip type has not
  // changed when the type was renamed from address to ip because that would be
  // a breaking change. This is fixable by registering two separate extension
  // types with the same functionality but different ids, but that's a lot of
  // effort for something users don't usually see.
  static constexpr auto vast_id = "vast.address";

  static const std::shared_ptr<arrow::DataType> arrow_type;

  // Create an arrow type representation of a VAST address type.
  explicit ip_extension_type();

  /// Unique name to identify the extension type, `vast.address`.
  std::string extension_name() const override;

  /// Compare two extension types for equality, based on the extension name.
  /// @param other An extension type to test for equality.
  bool ExtensionEquals(const ExtensionType& other) const override;

  /// Wrap built-in Array type in an ExtensionArray instance
  /// @param data the physical storage for the extension type
  std::shared_ptr<arrow::Array>
  MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

  /// Create an instance of ip_extension_type given the actual storage type
  /// and the serialized representation.
  /// @param storage_type the physical storage type of the extension
  /// @param serialized the serialized form of the extension.
  arrow::Result<std::shared_ptr<DataType>>
  Deserialize(std::shared_ptr<DataType> storage_type,
              const std::string& serialized) const override;

  /// Create serialized representation of this address, based on extension name.
  /// @return the serialized representation, `vast.address`.
  std::string Serialize() const override;
};

/// Subnet representation as an Arrow extension type.
/// Internal (physical) representation is a struct containing
/// a `uint8`, the length of the network prefix, and the address,
/// represented as `ip_extension_type`.
class subnet_extension_type : public arrow::ExtensionType {
public:
  static constexpr auto vast_id = "vast.subnet";
  static const std::shared_ptr<arrow::DataType> arrow_type;

  // Create an arrow type representation of a VAST subnet type.
  subnet_extension_type();

  /// Compare two extension types for equality, based on the extension name.
  /// @param other An extension type to test for equality.
  bool ExtensionEquals(const ExtensionType& other) const override;

  /// Unique name to identify the extension type, `vast.subnet`.
  std::string extension_name() const override;

  /// Wrap built-in Array type in an ExtensionArray instance
  /// @param data the physical storage for the extension type
  std::shared_ptr<arrow::Array>
  MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

  /// Create an instance of subnet given the actual storage type
  /// and the serialized representation.
  /// @param storage_type the physical storage type of the extension
  /// @param serialized the serialized form of the extension.
  arrow::Result<std::shared_ptr<DataType>>
  Deserialize(std::shared_ptr<DataType> storage_type,
              const std::string& serialized) const override;

  /// Create serialized representation of this subnet, based on extension name.
  /// @return the serialized representation, `vast.subnet`.
  std::string Serialize() const override;
};

/// pattern representation as an Arrow extension type.
/// Internal (physical) representation is `arrow::StringType`.
class pattern_extension_type : public arrow::ExtensionType {
public:
  static constexpr auto vast_id = "vast.pattern";
  static const std::shared_ptr<arrow::DataType> arrow_type;

  // Create an arrow type representation of a VAST pattern type.
  pattern_extension_type();

  /// Unique name to identify the extension type, `vast.pattern`.
  std::string extension_name() const override;

  /// Compare two extension types for equality, based on the extension name.
  /// @param other An extension type to test for equality.
  bool ExtensionEquals(const ExtensionType& other) const override;

  /// Wrap built-in Array type in an ExtensionArray instance
  /// @param data the physical storage for the extension type
  std::shared_ptr<arrow::Array>
  MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

  /// Create an instance of pattern given the actual storage type
  /// and the serialized representation.
  /// @param storage_type the physical storage type of the extension
  /// @param serialized the serialized form of the extension.
  arrow::Result<std::shared_ptr<DataType>>
  Deserialize(std::shared_ptr<DataType> storage_type,
              const std::string& serialized) const override;

  /// Create serialized representation of pattern, based on extension name.
  /// @return the serialized representation, `vast.pattern`.
  std::string Serialize() const override;
};

struct enum_array : arrow::ExtensionArray {
  using TypeClass = enum_extension_type;
  using arrow::ExtensionArray::ExtensionArray;
};

struct ip_array : arrow::ExtensionArray {
  using TypeClass = ip_extension_type;
  using arrow::ExtensionArray::ExtensionArray;
};

struct subnet_array : arrow::ExtensionArray {
  using TypeClass = subnet_extension_type;
  using arrow::ExtensionArray::ExtensionArray;
};

struct pattern_array : arrow::ExtensionArray {
  using TypeClass = pattern_extension_type;
  using arrow::ExtensionArray::ExtensionArray;
};

/// Register all VAST-defined Arrow extension types in the global registry.
void register_extension_types();

/// Creates an `ip_extension_type` for VAST `ip_type.
/// @returns An arrow extension type for address.
std::shared_ptr<arrow::ExtensionType> make_arrow_address();

/// Creates an `subnet_extension_type` for VAST `subnet_type.
/// @returns An arrow extension type for subnet.
std::shared_ptr<arrow::ExtensionType> make_arrow_subnet();

/// Creates an `pattern_extension_type` for VAST `pattern_type.
/// @returns An arrow extension type for pattern.
std::shared_ptr<arrow::ExtensionType> make_arrow_pattern();

/// Creates a `enum_extension_type` extension for `enumeration_type`.
/// @param t The enumeration type to represent.
/// @returns An arrow extension type for the specific enumeration.
std::shared_ptr<arrow::ExtensionType> make_arrow_enum(enumeration_type t);

} // namespace vast

namespace caf {

/// Sum type access definitions for `arrow::Array` and `arrow::DataType`
/// alongside their respective `std::shared_ptr` as typically occurring in Apache
/// Arrow are based on the `types_list` for `arrow::Array`, from which the
/// `type_list` for `arrow::DataType` is derived. However, the actual mapping is
/// based on `arrow::DataType`: it's possible to access the underlying datatype
/// from an Array via its `::TypeClass`, but there's no way to go from DataType
/// back to the Array, so there's no way to define `apply` for `arrow::DataType`
/// in terms of indexing for `arrow::Array`.
/// For this machinery to work, we need to create a separate struct extending
/// `arrow::ExtensionArray` for every custom extension type and provide a
/// proper implementation for `::TypeClass`. See `pattern_array` etc.

template <>
struct sum_type_access<arrow::Array> final {
  template <class T>
  struct is_extension_array
    : std::integral_constant<bool, T::TypeClass::type_id
                                     == arrow::ExtensionType::type_id> {};
  template <class Types>
  struct tl_map_array_to_type;
  template <class... Ts>
  struct tl_map_array_to_type<detail::type_list<Ts...>> {
    using type = detail::type_list<typename Ts::TypeClass...>;
  };
  using types = detail::type_list<
    arrow::BooleanArray, arrow::Int64Array, arrow::UInt64Array,
    arrow::DoubleArray, arrow::DurationArray, arrow::StringArray,
    arrow::TimestampArray, arrow::MapArray, arrow::ListArray,
    arrow::StructArray, vast::ip_array, vast::pattern_array, vast::enum_array,
    vast::subnet_array, arrow::FixedSizeBinaryArray>;
  using data_types = typename tl_map_array_to_type<types>::type;
  using extension_types
    = detail::tl_filter_t<data_types, arrow::is_extension_type>;

  using type0 = detail::tl_head_t<types>;
  static constexpr bool specialized = true;

  template <class T, int Index>
  static bool is(const arrow::Array& arr, sum_type_token<T, Index>) {
    if (arr.type_id() != T::TypeClass::type_id)
      return false;
    if constexpr (is_extension_array<T>::value)
      return static_cast<const arrow::ExtensionType&>(*arr.type())
               .extension_name()
             == T::TypeClass::vast_id;
    return true;
  }

  template <class T, int Index>
  static const T& get(const arrow::Array& arr, sum_type_token<T, Index>) {
    return static_cast<const T&>(arr);
  }

  template <class T, int Index>
  static T& get(arrow::Array& arr, sum_type_token<T, Index>) {
    return static_cast<T&>(arr);
  }

  template <class T, int Index>
  static const T*
  get_if(const arrow::Array* arr, sum_type_token<T, Index> token) {
    if (arr && is(*arr, token))
      return &get(*arr, token);
    return nullptr;
  }

  template <class T, int Index>
  static T* get_if(arrow::Array* arr, sum_type_token<T, Index> token) {
    if (arr && is(*arr, token))
      return &get(*arr, token);
    return nullptr;
  }

  static int index_from_type(const arrow::DataType& x) noexcept;

  template <class Result, class Visitor, class... Args>
  static auto apply(const arrow::Array& x, Visitor&& v, Args&&... xs)
    -> Result {
    // A dispatch table that maps variant type index to dispatch function for
    // the concrete type.
    static constexpr auto table = []<class... Ts, int... Indices>(
      detail::type_list<Ts...>,
      std::integer_sequence<int, Indices...>) noexcept {
      return std::array{
        +[](const arrow::Array& x, Visitor&& v, Args&&... xs) -> Result {
          auto xs_as_tuple = std::forward_as_tuple(xs...);
          auto indices = detail::get_indices(xs_as_tuple);
          return detail::apply_args_suffxied(
            std::forward<decltype(v)>(v), std::move(indices), xs_as_tuple,
            get(x, sum_type_token<Ts, Indices>{}));
        }...};
    }
    (types{}, std::make_integer_sequence<int, detail::tl_size<types>::value>());
    const auto dispatch = table[index_from_type(*x.type())];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

template <>
struct sum_type_access<arrow::DataType> final {
  using types = sum_type_access<arrow::Array>::data_types;

  using type0 = detail::tl_head_t<types>;

  static constexpr bool specialized = true;

  template <class T, int Index>
  static bool is(const arrow::DataType& x, sum_type_token<T, Index>) {
    if (x.id() != T::type_id)
      return false;
    if constexpr (arrow::is_extension_type<T>::value)
      return static_cast<const arrow::ExtensionType&>(x).extension_name()
             == T::vast_id;
    return true;
  }

  template <class T, int Index>
  static const T& get(const arrow::DataType& x, sum_type_token<T, Index>) {
    return static_cast<const T&>(x);
  }

  template <class T, int Index>
  static T& get(arrow::DataType& x, sum_type_token<T, Index>) {
    return static_cast<T&>(x);
  }

  template <class T, int Index>
  static const T*
  get_if(const arrow::DataType* x, sum_type_token<T, Index> token) {
    if (x && is(*x, token))
      return &get(*x, token);
    return nullptr;
  }

  template <class T, int Index>
  static T* get_if(arrow::DataType* x, sum_type_token<T, Index> token) {
    if (x && is(*x, token))
      return &get(*x, token);
    return nullptr;
  }

  template <class Result, class Visitor, class... Args>
  static auto apply(const arrow::DataType& x, Visitor&& v, Args&&... xs)
    -> Result {
    // A dispatch table that maps variant type index to dispatch function for
    // the concrete type.
    static constexpr auto table = []<class... Ts, int... Indices>(
      detail::type_list<Ts...>,
      std::integer_sequence<int, Indices...>) noexcept {
      return std::array{
        +[](const arrow::DataType& x, Visitor&& v, Args&&... xs) -> Result {
          auto xs_as_tuple = std::forward_as_tuple(xs...);
          auto indices = detail::get_indices(xs_as_tuple);
          return detail::apply_args_suffxied(
            std::forward<decltype(v)>(v), std::move(indices), xs_as_tuple,
            get(x, sum_type_token<Ts, Indices>{}));
        }...};
    }
    (types{}, std::make_integer_sequence<int, detail::tl_size<types>::value>());
    const auto dispatch
      = table[sum_type_access<arrow::Array>::index_from_type(x)];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

template <>
struct sum_type_access<std::shared_ptr<arrow::Array>> final {
  using types = typename vast::detail::tl_map_shared_ptr<
    typename sum_type_access<arrow::Array>::types>::type;
  using type0 = detail::tl_head_t<types>;
  static constexpr bool specialized = true;

  template <class T, int Index>
  static bool
  is(const std::shared_ptr<arrow::Array>& arr, sum_type_token<T, Index>) {
    VAST_ASSERT(arr);
    if (arr->type_id() != T::element_type::TypeClass::type_id)
      return false;
    if constexpr (sum_type_access<arrow::Array>::is_extension_array<
                    typename T::element_type>::value)
      return static_cast<const arrow::ExtensionType&>(*arr->type())
               .extension_name()
             == T::element_type::TypeClass::vast_id;
    return true;
  }

  template <class T, int Index>
  static T
  get(const std::shared_ptr<arrow::Array>& x, sum_type_token<T, Index>) {
    return std::static_pointer_cast<typename T::element_type>(x);
  }

  template <class T, int Index>
  static std::optional<T> get_if(const std::shared_ptr<arrow::Array>* x,
                                 sum_type_token<T, Index> token) {
    if (x && is(*x, token))
      return get(*x, token);
    return {};
  }

  template <class Result, class Visitor, class... Args>
  static auto
  apply(const std::shared_ptr<arrow::Array>& x, Visitor&& v, Args&&... xs)
    -> Result {
    VAST_ASSERT(x);
    // A dispatch table that maps variant type index to dispatch function for
    // the concrete type.
    static constexpr auto table = []<class... Ts, int... Indices>(
      detail::type_list<Ts...>,
      std::integer_sequence<int, Indices...>) noexcept {
      return std::array{+[](const std::shared_ptr<arrow::Array>& x, Visitor&& v,
                            Args&&... xs) -> Result {
        auto xs_as_tuple = std::forward_as_tuple(xs...);
        auto indices = detail::get_indices(xs_as_tuple);
        return detail::apply_args_suffxied(
          std::forward<decltype(v)>(v), std::move(indices), xs_as_tuple,
          get(x, sum_type_token<Ts, Indices>{}));
      }...};
    }
    (types{}, std::make_integer_sequence<int, detail::tl_size<types>::value>());
    const auto dispatch
      = table[sum_type_access<arrow::Array>::index_from_type(*x->type())];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

template <>
struct sum_type_access<std::shared_ptr<arrow::DataType>> final {
  using types = typename vast::detail::tl_map_shared_ptr<
    typename sum_type_access<arrow::DataType>::types>::type;
  using type0 = detail::tl_head_t<types>;
  static constexpr bool specialized = true;

  template <class T, int Index>
  static bool
  is(const std::shared_ptr<arrow::DataType>& x, sum_type_token<T, Index>) {
    VAST_ASSERT(x);
    if (x->id() != T::element_type::type_id)
      return false;
    if constexpr (arrow::is_extension_type<typename T::element_type>::value)
      return static_cast<const arrow::ExtensionType&>(*x).extension_name()
             == T::element_type::vast_id;
    return true;
    return x->id() == T::element_type::type_id;
  }

  template <class T, int Index>
  static T
  get(const std::shared_ptr<arrow::DataType>& x, sum_type_token<T, Index>) {
    return std::static_pointer_cast<typename T::element_type>(x);
  }

  template <class T, int Index>
  static std::optional<T> get_if(const std::shared_ptr<arrow::DataType>* x,
                                 sum_type_token<T, Index> token) {
    if (x && is(*x, token))
      return get(*x, token);
    return {};
  }

  template <class Result, class Visitor, class... Args>
  static auto
  apply(const std::shared_ptr<arrow::DataType>& x, Visitor&& v, Args&&... xs)
    -> Result {
    // A dispatch table that maps variant type index to dispatch function for
    // the concrete type.
    static constexpr auto table = []<class... Ts, int... Indices>(
      detail::type_list<Ts...>,
      std::integer_sequence<int, Indices...>) noexcept {
      return std::array{+[](const std::shared_ptr<arrow::DataType>& x,
                            Visitor&& v, Args&&... xs) -> Result {
        auto xs_as_tuple = std::forward_as_tuple(xs...);
        auto indices = detail::get_indices(xs_as_tuple);
        return detail::apply_args_suffxied(
          std::forward<decltype(v)>(v), std::move(indices), xs_as_tuple,
          get(x, sum_type_token<Ts, Indices>{}));
      }...};
    }
    (types{}, std::make_integer_sequence<int, detail::tl_size<types>::value>());
    const auto dispatch
      = table[sum_type_access<arrow::Array>::index_from_type(*x)];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

} // namespace caf
