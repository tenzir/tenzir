#include "vast/arrow_extension_types.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/die.hpp"

#include <simdjson.h>

namespace vast {

enum_extension_type::enum_extension_type(enumeration_type enum_type)
  : arrow::ExtensionType(arrow::dictionary(arrow::int16(), arrow::utf8())),
    enum_type_(std::move(enum_type)) {
}

bool enum_extension_type::ExtensionEquals(const ExtensionType& other) const {
  if (other.extension_name() == this->extension_name()) {
    return this->enum_type_
           == static_cast<const enum_extension_type&>(other).enum_type_;
  }
  return false;
}

std::string enum_extension_type::ToString() const {
  return fmt::format("{} <{}>", this->extension_name(), this->enum_type_);
}

std::string enum_extension_type::extension_name() const {
  return "vast.enum";
}

std::shared_ptr<arrow::Array>
enum_extension_type::MakeArray(std::shared_ptr<arrow::ArrayData> data) const {
  VAST_ASSERT(data->type->id() == arrow::Type::EXTENSION);
  VAST_ASSERT(ExtensionEquals(static_cast<const ExtensionType&>(*data->type)));
  return std::make_shared<arrow::ExtensionArray>(data);
}

arrow::Result<std::shared_ptr<arrow::DataType>>
enum_extension_type::Deserialize(std::shared_ptr<arrow::DataType> storage_type,
                                 const std::string& serialized) const {
  if (!storage_type->Equals(*storage_type_)) {
    return arrow::Status::Invalid("Invalid storage type for VAST enum "
                                  "dictionary: ",
                                  storage_type->ToString());
  }
  // simdjson requires additional padding on the string
  simdjson::padded_string json{serialized};
  simdjson::dom::parser parser;
  auto doc = parser.parse(json);
  std::vector<struct vast::enumeration_type::field> enum_fields{};
  for (auto f : doc.get_object()) {
    std::string_view key = f.key;
    if (!f.value.is<uint64_t>())
      return arrow::Status::SerializationError(f.value, " is not an uint64_t");
    enum_fields.emplace_back(
      std::string{key}, detail::narrow_cast<uint32_t>(f.value.get_uint64()));
  }
  return std::make_shared<enum_extension_type>(enumeration_type{enum_fields});
}

std::string enum_extension_type::Serialize() const {
  auto out = std::string{};
  auto inserter = std::back_inserter(out);
  fmt::format_to(inserter, "{{ ");
  bool first = true;
  for (const auto& f : enum_type_.fields()) {
    if (first)
      first = false;
    else
      fmt::format_to(inserter, ", ");
    fmt::format_to(inserter, "\"{}\": {}", f.name, f.key);
  }
  fmt::format_to(inserter, "}}");
  return out;
}

enumeration_type enum_extension_type::get_enum_type() const {
  return this->enum_type_;
}

void register_extension_type(const std::shared_ptr<arrow::ExtensionType>& t) {
  if (auto et = arrow::GetExtensionType(t->extension_name()); !et)
    if (!arrow::RegisterExtensionType(t).ok())
      die(fmt::format("unable to register extension type; {}",
                      t->extension_name()));
}

void register_extension_types() {
  register_extension_type(make_arrow_enum(enumeration_type{{}}));
}

std::shared_ptr<enum_extension_type> make_arrow_enum(enumeration_type t) {
  return std::make_shared<enum_extension_type>(t);
}

} // namespace vast
