#define SUITE arrow_extension_types

#include "vast/arrow_extension_types.hpp"

#include "vast/detail/overload.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"

#include <arrow/api.h>

TEST(enum extension type equality) {
  using vast::enum_extension_type;
  using vast::enumeration_type;
  enum_extension_type t1{enumeration_type{{"one"}, {"two"}, {"three"}}};
  enum_extension_type t2{enumeration_type{{"one"}, {"two"}, {"three"}}};
  enum_extension_type t3{enumeration_type{{"one"}, {"three"}, {"two"}}};
  enum_extension_type t4{enumeration_type{{"one"}, {"two", 3}, {"three"}}};
  enum_extension_type t5{enumeration_type{{"some"}, {"other"}, {"vals"}}};
  CHECK(t1.ExtensionEquals(t2));
  CHECK(!t1.ExtensionEquals(t3));
  CHECK(!t1.ExtensionEquals(t4));
  CHECK(!t1.ExtensionEquals(t5));
}

namespace {

auto arrow_enum_roundtrip(const vast::enumeration_type& et) {
  const auto& dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
  const auto& arrow_type = std::make_shared<vast::enum_extension_type>(et);
  const auto serialized = arrow_type->Serialize();
  const auto& standin
    = vast::enum_extension_type(vast::enumeration_type{{"stub"}});
  const auto& deserialized
    = standin.Deserialize(dict_type, serialized).ValueOrDie();
  CHECK(arrow_type->Equals(*deserialized, true));
}

template <class ExtType>
void serde_roundtrip() {
  const auto& arrow_type = std::make_shared<ExtType>();
  const auto serialized = arrow_type->Serialize();
  const auto& standin = std::make_shared<ExtType>();
  const auto& deserialized
    = standin->Deserialize(ExtType::arrow_type, serialized).ValueOrDie();
  CHECK(arrow_type->Equals(*deserialized, true));
  CHECK(!standin->Deserialize(arrow::fixed_size_binary(23), serialized).ok());
}

template <class Builder, class T = typename Builder::value_type>
std::shared_ptr<arrow::Array> makeArrowArray(std::vector<T> xs) {
  Builder b{};
  CHECK(b.AppendValues(xs).ok());
  return b.Finish().ValueOrDie();
}

std::shared_ptr<arrow::Array> makeAddressArray() {
  arrow::FixedSizeBinaryBuilder b{arrow::fixed_size_binary(16)};
  return std::make_shared<vast::address_array>(vast::make_arrow_address(),
                                               b.Finish().ValueOrDie());
}

// Returns a visitor that checks whether the expected concrete types are the
// types resulting in the visitation.
template <class... T>
auto is_type() {
  return []<class... U>(const U&...) {
    return (std::is_same_v<T, U> && ...);
  };
}

} // namespace

TEST(arrow enum extension type roundtrip) {
  using vast::enumeration_type;
  arrow_enum_roundtrip(enumeration_type{{"true"}, {"false"}});
  arrow_enum_roundtrip(enumeration_type{{"1"}, {"2"}, {"3"}, {"4"}});
}

TEST(arrow enum parse error) {
  const auto& standin
    = vast::enum_extension_type(vast::enumeration_type{{"stub"}});

  auto r = standin.Deserialize(arrow::dictionary(arrow::int16(), arrow::utf8()),
                               R"({ "a": "no_int" })");
  CHECK(r.status().IsSerializationError());
}

TEST(address type serde roundtrip) {
  serde_roundtrip<vast::address_extension_type>();
}

TEST(subnet type serde roundtrip) {
  serde_roundtrip<vast::subnet_extension_type>();
}

TEST(pattern type serde roundtrip) {
  serde_roundtrip<vast::pattern_extension_type>();
}

TEST(arrow::DataType sum type) {
  CHECK(caf::visit(is_type<arrow::NullType>(), *arrow::null()));
  CHECK(caf::visit(is_type<arrow::Int64Type>(), *arrow::int64()));
  CHECK(caf::visit(
    is_type<vast::address_extension_type>(),
    static_cast<const arrow::DataType&>(vast::address_extension_type())));
  CHECK(caf::visit(
    is_type<vast::pattern_extension_type>(),
    static_cast<const arrow::DataType&>(vast::pattern_extension_type())));
  CHECK(caf::visit(is_type<arrow::Int64Type, arrow::NullType>(),
                   *arrow::int64(), *arrow::null()));

  CHECK_EQUAL(caf::get_if<arrow::StringType>(&*arrow::utf8()), &*arrow::utf8());
  CHECK(
    caf::visit(is_type<std::shared_ptr<arrow::Int64Type>>(), arrow::int64()));
  CHECK(caf::visit(is_type<std::shared_ptr<arrow::Int64Type>,
                           std::shared_ptr<arrow::NullType>>(),
                   arrow::int64(), arrow::null()));
  auto n = arrow::null();
  auto et = static_pointer_cast<arrow::DataType>(
    vast::make_arrow_enum(vast::enumeration_type{{"A"}, {"B"}, {"C"}}));
  auto pt = static_pointer_cast<arrow::DataType>(vast::make_arrow_pattern());
  CHECK(caf::get_if<std::shared_ptr<arrow::NullType>>(&n));
  CHECK(!caf::get_if<std::shared_ptr<arrow::Int64Type>>(&n));
  CHECK(caf::get_if<std::shared_ptr<vast::enum_extension_type>>(&et));
  CHECK(!caf::get_if<std::shared_ptr<vast::enum_extension_type>>(&pt));
  CHECK(!caf::get_if<std::shared_ptr<vast::pattern_extension_type>>(&et));
  CHECK(caf::get_if<std::shared_ptr<vast::pattern_extension_type>>(&pt));
}

TEST(arrow::Array sum type) {
  auto str_arr = makeArrowArray<arrow::StringBuilder, std::string>({"a", "b"});
  auto uint_arr = makeArrowArray<arrow::UInt64Builder>({7, 8});
  auto int_arr = makeArrowArray<arrow::Int64Builder>({3, 2, 1});
  auto addr_arr = makeAddressArray();
  const auto& pattern_arr = std::static_pointer_cast<arrow::Array>(
    std::make_shared<vast::pattern_array>(vast::make_arrow_pattern(), str_arr));
  CHECK(caf::get_if<arrow::StringArray>(&*str_arr));
  CHECK(!caf::get_if<arrow::UInt64Array>(&*str_arr));
  CHECK(!caf::get_if<arrow::StringArray>(&*uint_arr));
  CHECK(caf::get_if<arrow::UInt64Array>(&*uint_arr));
  CHECK(!caf::get_if<vast::address_array>(&*uint_arr));
  CHECK(!caf::get_if<vast::pattern_array>(&*addr_arr));
  CHECK(caf::get_if<vast::address_array>(&*addr_arr));
  CHECK(!caf::get_if<vast::address_array>(&*pattern_arr));
  CHECK(caf::get_if<vast::pattern_array>(&*pattern_arr));
  CHECK(caf::get_if<vast::pattern_array>(&*pattern_arr));
  caf::visit(is_type<arrow::StringArray>(), *str_arr);
  caf::visit(is_type<vast::pattern_array>(), *pattern_arr);
  caf::visit(is_type<vast::pattern_array>(), *str_arr);
  auto f = vast::detail::overload{
    [](const vast::address_array&) {
      return 99;
    },
    [](const vast::pattern_array&) {
      return 100;
    },
    [](const arrow::StringArray&) {
      return 101;
    },
    [](const auto&) {
      return -1;
    },
  };
  CHECK_EQUAL(caf::visit(f, *str_arr), 101);
  CHECK_EQUAL(caf::visit(f, *pattern_arr), 100);
  CHECK_EQUAL(caf::visit(f, *addr_arr), 99);
  CHECK_EQUAL(caf::visit(f, *int_arr), -1);
  CHECK(caf::visit(is_type<std::shared_ptr<arrow::StringArray>>(), str_arr));
  CHECK(!caf::visit(is_type<std::shared_ptr<arrow::StringArray>>(), addr_arr));
  CHECK(!caf::visit(is_type<std::shared_ptr<vast::pattern_array>>(), addr_arr));
  CHECK(caf::visit(is_type<std::shared_ptr<vast::address_array>>(), addr_arr));
  CHECK(!caf::get_if<std::shared_ptr<vast::address_array>>(&str_arr));
  CHECK(!caf::get_if<std::shared_ptr<vast::pattern_array>>(&str_arr));
  CHECK(caf::get_if<std::shared_ptr<vast::address_array>>(&addr_arr));
  CHECK(caf::get_if<std::shared_ptr<vast::pattern_array>>(&pattern_arr));
  CHECK(!caf::get_if<std::shared_ptr<vast::address_array>>(&pattern_arr));
}
