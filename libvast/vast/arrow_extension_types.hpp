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
  std::string extension_name() const override;

  bool ExtensionEquals(const ExtensionType& other) const override;

  std::string ToString() const override;

  std::shared_ptr<arrow::Array>
  MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

  arrow::Result<std::shared_ptr<DataType>>
  Deserialize(std::shared_ptr<DataType> storage_type,
              const std::string& serialized) const override;

  std::string Serialize() const override;

  enumeration_type get_enum_type() const;

private:
  enumeration_type enum_type_;
};

/// Register all VAST-defined Arrow extension types in the global registry.
void register_extension_types();

/// Creates a `enum_extension_type` extension for `enumeration_type`.
/// @param t The enumeration type to represent.
/// @returns An arrow extension type for the specific enumeration.
std::shared_ptr<enum_extension_type> make_arrow_enum(enumeration_type t);

} // namespace vast
