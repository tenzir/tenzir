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

#include <arrow/extension_type.h>
#include <arrow/type_fwd.h>
#include <caf/detail/type_list.hpp>

namespace vast {

/// Enum representation in the Arrow type system, utilizing an extension type.
/// The underlying data is represented as a dictionary, where the `dict` part
/// contains all the possible variants specified in the underlying VAST enum.
class enum_extension_type : public arrow::ExtensionType {
public:
  static constexpr auto vast_id = "vast.enum";
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
class address_extension_type : public arrow::ExtensionType {
public:
  static constexpr auto vast_id = "vast.address";

  static const std::shared_ptr<arrow::DataType> arrow_type;

  // Create an arrow type representation of a VAST address type.
  explicit address_extension_type();

  /// Unique name to identify the extension type, `vast.address`.
  std::string extension_name() const override;

  /// Compare two extension types for equality, based on the extension name.
  /// @param other An extension type to test for equality.
  bool ExtensionEquals(const ExtensionType& other) const override;

  /// Wrap built-in Array type in an ExtensionArray instance
  /// @param data the physical storage for the extension type
  std::shared_ptr<arrow::Array>
  MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

  /// Create an instance of address_extension_type given the actual storage type
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
/// represented as `address_extension_type`.
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

/// Register all VAST-defined Arrow extension types in the global registry.
void register_extension_types();

/// Creates an `address_extension_type` for VAST `address_type.
/// @returns An arrow extension type for address.
std::shared_ptr<address_extension_type> make_arrow_address();

/// Creates an `subnet_extension_type` for VAST `subnet_type.
/// @returns An arrow extension type for subnet.
std::shared_ptr<subnet_extension_type> make_arrow_subnet();

/// Creates an `pattern_extension_type` for VAST `pattern_type.
/// @returns An arrow extension type for pattern.
std::shared_ptr<pattern_extension_type> make_arrow_pattern();

/// Creates a `enum_extension_type` extension for `enumeration_type`.
/// @param t The enumeration type to represent.
/// @returns An arrow extension type for the specific enumeration.
std::shared_ptr<enum_extension_type> make_arrow_enum(enumeration_type t);

} // namespace vast

namespace caf {

template <>
struct sum_type_access<arrow::DataType> final {
  template <class T>
  struct is_extension_type
    : std::integral_constant<bool, T::type_id == arrow::ExtensionType::type_id> {
  };

  using types = caf::detail::type_list<
    arrow::NullType, arrow::BooleanType, arrow::Int64Type, arrow::UInt64Type,
    arrow::DoubleType, arrow::DurationType, arrow::StringType,
    arrow::TimestampType, arrow::MapType, arrow::ListType, arrow::StructType,
    vast::address_extension_type, vast::enum_extension_type,
    vast::subnet_extension_type, vast::pattern_extension_type>;

  using extension_types = detail::tl_filter_t<types, is_extension_type>;
  using type0 = detail::tl_head_t<types>;

  static constexpr bool specialized = true;

  template <class T, int Index>
  static bool is(const arrow::DataType& x, sum_type_token<T, Index>) {
    if (x.id() != T::type_id)
      return false;
    if constexpr (is_extension_type<T>::value)
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

  static int index_from_type(const arrow::DataType& x) noexcept;

  template <class Result, class Visitor, class... Args>
  static auto apply(const arrow::DataType& x, Visitor&& v, Args&&... xs)
    -> Result {
    // A dispatch table that maps variant type index to dispatch function for
    // the concrete type.
    static constexpr auto table = []<class... Ts, int... Indices>(
      caf::detail::type_list<Ts...>,
      std::integer_sequence<int, Indices...>) noexcept {
      return std::array{
        // decay to function pointer
        +[](const arrow::DataType& x, Visitor&& v, Args&&... xs) -> Result {
          auto xs_as_tuple = std::forward_as_tuple(xs...);
          auto indices = caf::detail::get_indices(xs_as_tuple);
          return caf::detail::apply_args_suffxied(
            std::forward<decltype(v)>(v), std::move(indices), xs_as_tuple,
            get(x, sum_type_token<Ts, Indices>{}));
        }...};
    }
    (types{}, std::make_integer_sequence<int, detail::tl_size<types>::value>());
    const auto dispatch = table[index_from_type(x)];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

template <>
struct sum_type_access<std::shared_ptr<arrow::DataType>> final {
  template <class Types>
  struct tl_map_shared_ptr;
  template <class... Ts>
  struct tl_map_shared_ptr<caf::detail::type_list<Ts...>> {
    using type = caf::detail::type_list<std::shared_ptr<Ts>...>;
  };

  using types = typename tl_map_shared_ptr<
    typename sum_type_access<arrow::DataType>::types>::type;
  using type0 = detail::tl_head_t<types>;
  static constexpr bool specialized = true;

  template <class T, int Index>
  static bool
  is(const std::shared_ptr<arrow::DataType>& x, sum_type_token<T, Index>) {
    VAST_ASSERT(x);
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
      caf::detail::type_list<Ts...>,
      std::integer_sequence<int, Indices...>) noexcept {
      return std::array{// decay to function pointer
                        +[](const std::shared_ptr<arrow::DataType>& x,
                            Visitor&& v, Args&&... xs) -> Result {
                          auto xs_as_tuple = std::forward_as_tuple(xs...);
                          auto indices = caf::detail::get_indices(xs_as_tuple);
                          return caf::detail::apply_args_suffxied(
                            std::forward<decltype(v)>(v), std::move(indices),
                            xs_as_tuple, get(x, sum_type_token<Ts, Indices>{}));
                        }...};
    }
    (types{}, std::make_integer_sequence<int, detail::tl_size<types>::value>());
    const auto dispatch
      = table[sum_type_access<arrow::DataType>::index_from_type(*x)];
    VAST_ASSERT(dispatch);
    return dispatch(x, std::forward<Visitor>(v), std::forward<Args>(xs)...);
  }
};

} // namespace caf
