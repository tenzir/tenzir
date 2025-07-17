//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <arrow/api.h>

namespace tenzir {

TEST(enum extension type equality) {
  enumeration_type::arrow_type t1{
    enumeration_type{{"one"}, {"two"}, {"three"}}};
  enumeration_type::arrow_type t2{
    enumeration_type{{"one"}, {"two"}, {"three"}}};
  enumeration_type::arrow_type t3{
    enumeration_type{{"one"}, {"three"}, {"two"}}};
  enumeration_type::arrow_type t4{
    enumeration_type{{"one"}, {"two", 3}, {"three"}}};
  enumeration_type::arrow_type t5{
    enumeration_type{{"some"}, {"other"}, {"vals"}}};
  CHECK(t1.ExtensionEquals(t2));
  CHECK(!t1.ExtensionEquals(t3));
  CHECK(!t1.ExtensionEquals(t4));
  CHECK(!t1.ExtensionEquals(t5));
}

namespace {

template <concrete_type Type>
void serde_roundtrip(const Type& type,
                     std::shared_ptr<typename Type::arrow_type> stub
                     = nullptr) {
  const auto& arrow_type = type.to_arrow_type();
  const auto serialized = arrow_type->Serialize();
  if (!stub)
    stub = type.to_arrow_type();
  const auto& deserialized
    = stub->Deserialize(arrow_type->storage_type(), serialized);
  if (!deserialized.status().ok())
    FAIL(deserialized.status().ToString());
  CHECK(arrow_type->Equals(*deserialized.ValueUnsafe(), true));
  CHECK(!stub->Deserialize(arrow::fixed_size_binary(23), serialized).ok());
}

template <class Builder, class T = typename Builder::value_type>
std::shared_ptr<arrow::Array> make_arrow_array(std::vector<T> xs) {
  Builder b{};
  CHECK(b.AppendValues(xs).ok());
  return check(b.Finish());
}

std::shared_ptr<arrow::Array> make_ip_array() {
  arrow::FixedSizeBinaryBuilder b{arrow::fixed_size_binary(16)};
  return std::make_shared<ip_type::array_type>(
    std::make_shared<ip_type::arrow_type>(), check(b.Finish()));
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

TEST(arrow enum parse error) {
  const auto& standin
    = enumeration_type::arrow_type(enumeration_type{{"stub"}});
  auto r = standin.Deserialize(arrow::dictionary(arrow::uint8(), arrow::utf8()),
                               R"({ "a": "no_int" })");
  CHECK(r.status().IsSerializationError());
}

TEST(enumeration type serde roundtrip) {
  auto stub = enumeration_type{{"stub"}}.to_arrow_type();
  serde_roundtrip(enumeration_type{{"true"}, {"false"}}, stub);
  serde_roundtrip(enumeration_type{{"1"}, {"2"}, {"3"}, {"4"}}, stub);
}

TEST(address type serde roundtrip) {
  serde_roundtrip(ip_type{});
}

TEST(subnet type serde roundtrip) {
  serde_roundtrip(subnet_type{});
}

TEST(arrow::DataType sum type) {
  CHECK(match(*arrow::int64(), is_type<arrow::Int64Type>()));
  CHECK(match(static_cast<const arrow::DataType&>(ip_type::arrow_type()), is_type<ip_type::arrow_type>()));
  CHECK(match(std::tie(*arrow::int64(), *arrow::uint64()), is_type<arrow::Int64Type, arrow::UInt64Type>()));
  CHECK_EQUAL(try_as<arrow::StringType>(arrow::utf8().get()),
              arrow::utf8().get());
  auto et = static_pointer_cast<arrow::DataType>(
    std::make_shared<enumeration_type::arrow_type>(
      enumeration_type{{"A"}, {"B"}, {"C"}}));
  CHECK(try_as<enumeration_type::arrow_type>(et.get()));
  CHECK(!try_as<subnet_type::arrow_type>(et.get()));
}

TEST(arrow::Array sum type) {
  auto str_arr
    = make_arrow_array<arrow::StringBuilder, std::string>({"a", "b"});
  auto uint_arr = make_arrow_array<arrow::UInt64Builder>({7, 8});
  auto int_arr = make_arrow_array<arrow::Int64Builder>({3, 2, 1});
  auto addr_arr = make_ip_array();
  CHECK(try_as<arrow::StringArray>(&*str_arr));
  CHECK(!try_as<arrow::UInt64Array>(&*str_arr));
  CHECK(!try_as<arrow::StringArray>(&*uint_arr));
  CHECK(try_as<arrow::UInt64Array>(&*uint_arr));
  CHECK(!try_as<ip_type::array_type>(&*uint_arr));
  CHECK(try_as<ip_type::array_type>(&*addr_arr));
  match(*str_arr, is_type<arrow::StringArray>());
  auto f = detail::overload{
    [](const ip_type::array_type&) {
      return 99;
    },
    [](const arrow::StringArray&) {
      return 101;
    },
    [](const auto&) {
      return -1;
    },
  };
  CHECK_EQUAL(match(*str_arr, f), 101);
  CHECK_EQUAL(match(*addr_arr, f), 99);
  CHECK_EQUAL(match(*int_arr, f), -1);
}

} // namespace tenzir
