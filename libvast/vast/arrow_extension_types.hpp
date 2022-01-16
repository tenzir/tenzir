#include "vast/fwd.hpp"

#include "vast/type.hpp"

#include <arrow/api.h>

namespace vast {

class EnumType : public arrow::ExtensionType {
public:
  explicit EnumType(enumeration_type enum_type)
    : arrow::ExtensionType(arrow::dictionary(arrow::int8(), arrow::utf8())),
      enum_type_(std::move(enum_type)) {
  }

  std::string extension_name() const override;

  // TODO: this is probably insufficient, we want equality to consider
  // the possible values of the enum as well.
  bool ExtensionEquals(const ExtensionType& other) const override {
    if (other.extension_name() == this->extension_name()) {
      return this->enum_type_ == static_cast<const EnumType&>(other).enum_type_;
    }
    return false;
  }

  std::string ToString() const override {
    return fmt::format("{} <{}>", this->extension_name(), this->enum_type_);
  }

  std::shared_ptr<arrow::Array>
  MakeArray(std::shared_ptr<arrow::ArrayData> data) const override;

  arrow::Result<std::shared_ptr<DataType>>
  Deserialize(std::shared_ptr<DataType> storage_type,
              const std::string& serialized) const override;

  std::string Serialize() const override;

  enumeration_type enum_type_;
};

/// Register all VAST-defined Arrow extension types in the global registry.
void register_extension_types();

/// Creates a `EnumType` extension for the provided `enumeration_type`.
/// @param t The enumeration type to represent.
/// @returns An arrow extension type for the specific enumeration.
std::shared_ptr<EnumType> make_arrow_enum(enumeration_type t);
} // namespace vast
