#include "vast/arrow_extension_types.hpp"

#include <simdjson.h>

namespace vast {

namespace {

auto deserialize(const std::string& json_) {
  // simdjson requires additional padding on the string
  simdjson::padded_string json{json_};

  simdjson::ondemand::parser parser;
  auto doc = parser.iterate(json);
  std::vector<struct vast::enumeration_type::field> enum_fields{};

  for (auto f : doc.get_object()) {
    std::string_view key = f.unescaped_key();
    enum_fields.emplace_back(std::string{key}, f.value().get_uint64());
  }

  return enumeration_type{enum_fields};
}

std::string serialize(const enumeration_type& t) {
  auto out = fmt::memory_buffer();
  auto inserter = std::back_inserter(out);
  fmt::format_to(inserter, "{{ ");
  bool first = true;
  for (const auto& f : t.fields()) {
    if (first)
      first = false;
    else
      fmt::format_to(inserter, ", ");
    fmt::format_to(inserter, "\"{}\": {}", f.name, f.key);
  }
  fmt::format_to(inserter, "}} ");

  return fmt::to_string(out);
}

} // namespace

std::string EnumType::extension_name() const {
  return "vast.enum";
}

std::shared_ptr<arrow::Array>
EnumType::MakeArray(std::shared_ptr<arrow::ArrayData> data) const {
  VAST_ASSERT(data->type->id() == arrow::Type::EXTENSION);
  VAST_ASSERT(ExtensionEquals(static_cast<const ExtensionType&>(*data->type)));
  return std::make_shared<arrow::ExtensionArray>(data);
}

arrow::Result<std::shared_ptr<arrow::DataType>>
EnumType::Deserialize(std::shared_ptr<arrow::DataType> storage_type,
                      const std::string& serialized) const {
  if (!storage_type->Equals(*storage_type_)) {
    return arrow::Status::Invalid("Invalid storage type for VAST enum "
                                  "dictionary: ",
                                  storage_type->ToString());
  }
  return std::make_shared<EnumType>(deserialize(serialized));
}

std::string EnumType::Serialize() const {
  return serialize(enum_type_);
}

void register_extension_types() {
  if (const auto& t = make_arrow_enum(enumeration_type{{}});
      !arrow::RegisterExtensionType(t).ok())
    VAST_WARN("unable to regiser extension type; {}", t);
}

std::shared_ptr<EnumType> make_arrow_enum(enumeration_type t) {
  return std::make_shared<EnumType>(t);
}

} // namespace vast
