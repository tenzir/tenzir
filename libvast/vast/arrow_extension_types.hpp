#include "vast/fwd.hpp"

#include "vast/type.hpp"

#include <arrow/extension_type.h>
#include <arrow/type_fwd.h>

namespace vast {

/// Enum representation in the Arrow type system, utilizing an extension type.
/// The underlying data is represented as a dictionary, where the `dict` part
/// contains all the possible variants specified in the underlying VAST enum.
class enum_extension_type : public arrow::ExtensionType {
public:
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

/// Subnet representation as an Arrow extension type.
/// Internal (physical) representation is a 16-byte fixed binary.
class address_extension_type : public arrow::ExtensionType {
public:
  static std::string id;

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

/// Register all VAST-defined Arrow extension types in the global registry.
void register_extension_types();

/// Creates a `enum_extension_type` extension for `enumeration_type`.
/// @param t The enumeration type to represent.
/// @returns An arrow extension type for the specific enumeration.
std::shared_ptr<enum_extension_type> make_arrow_enum(enumeration_type t);

} // namespace vast
