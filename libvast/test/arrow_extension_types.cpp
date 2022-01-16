#include "vast/arrow_extension_types.hpp"

#define SUITE arrow_extension_types

#include "vast/test/test.hpp"

auto arrow_enum_roundtrip(const vast::enumeration_type& et) {
  const auto& dict_type = arrow::dictionary(arrow::int8(), arrow::utf8());

  const auto& arrow_type = std::make_shared<vast::EnumType>(et);
  const auto serialized = arrow_type->Serialize();
  const auto& standin = vast::EnumType(vast::enumeration_type{{"stub"}});
  const auto& deserialized
    = standin.Deserialize(dict_type, serialized).ValueOrDie();
  CHECK(arrow_type->Equals(*deserialized, true));
}

TEST(arrow enum extension type roundtrip) {
  using vast::enumeration_type;
  arrow_enum_roundtrip(enumeration_type{{"true"}, {"false"}});
  arrow_enum_roundtrip(enumeration_type{{"1"}, {"2"}, {"3"}, {"4"}});
}

TEST(enum extension type equality) {
  using vast::enumeration_type;
  using vast::EnumType;

  EnumType t1{enumeration_type{{"one"}, {"two"}, {"three"}}};
  EnumType t2{enumeration_type{{"one"}, {"two"}, {"three"}}};
  EnumType t3{enumeration_type{{"one"}, {"three"}, {"two"}}};
  EnumType t4{enumeration_type{{"one"}, {"two", 3}, {"three"}}};
  EnumType t5{enumeration_type{{"some"}, {"other"}, {"vals"}}};

  CHECK(t1.ExtensionEquals(t2));
  CHECK(!t1.ExtensionEquals(t3));
  CHECK(!t1.ExtensionEquals(t4));
  CHECK(!t1.ExtensionEquals(t5));
}
