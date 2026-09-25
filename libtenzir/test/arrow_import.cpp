//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/arrow_import.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>

#include <cstring>
#include <limits>
#include <string>
#include <vector>

using namespace tenzir;

namespace {

auto import(arrow::Array const& array) -> nova::Array<nova::Data> {
  auto result = nova::import_arrow_array(array);
  REQUIRE(result.is_ok());
  return std::move(result).unwrap();
}

auto import(std::shared_ptr<arrow::Array> array) -> nova::Array<nova::Data> {
  auto result = nova::import_arrow_array(std::move(array));
  REQUIRE(result.is_ok());
  return std::move(result).unwrap();
}

template <class Tag, class Value>
auto take_sparse(nova::Array<nova::Data> array)
  -> nova::storage::SparseStorage<Value> {
  auto values = array.get_alternative<Tag>();
  REQUIRE(values);
  return as<nova::storage::SparseStorage<Value>>(
    std::move(values->data).storage());
}

} // namespace

TEST("Arrow scalar slices preserve values and nulls") {
  auto builder = arrow::Int32Builder{};
  REQUIRE(builder.Append(99).ok());
  REQUIRE(builder.Append(-7).ok());
  REQUIRE(builder.AppendNull().ok());
  REQUIRE(builder.Append(42).ok());
  auto input = builder.Finish().ValueOrDie()->Slice(1);
  auto result = import(*input);
  CHECK_EQUAL(result.length(), 3);
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{int64_t{-7}});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(2)), data{int64_t{42}});
}

TEST("Arrow list slices rebase their child offsets and preserve nested nulls") {
  auto values = std::make_shared<arrow::Int32Builder>();
  auto lists = arrow::ListBuilder{arrow::default_memory_pool(), values};
  REQUIRE(lists.Append().ok());
  REQUIRE(values->Append(99).ok());
  REQUIRE(lists.Append().ok());
  REQUIRE(values->Append(1).ok());
  REQUIRE(values->AppendNull().ok());
  REQUIRE(values->Append(3).ok());
  REQUIRE(lists.AppendNull().ok());
  REQUIRE(lists.Append().ok());
  auto input = lists.Finish().ValueOrDie()->Slice(1);
  auto result = import(*input);
  CHECK_EQUAL(result.length(), 3);
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)),
              (data{list{int64_t{1}, data{}, int64_t{3}}}));
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(2)), data{list{}});
  CHECK_EQUAL(import(*input->Slice(input->length(), 0)).length(), 0);
}

TEST("Arrow struct slices honor the parent validity offset") {
  auto values = arrow::Int32Builder{};
  REQUIRE(values.AppendValues(std::vector<int32_t>{1, 2, 3}).ok());
  auto records
    = arrow::StructArray::Make(
        {values.Finish().ValueOrDie()}, std::vector<std::string>{"x"},
        arrow::Buffer::FromString(std::string(1, '\x05')))
        .ValueOrDie();
  auto result = import(*records->Slice(1));
  CHECK_EQUAL(result.length(), 2);
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)),
              (data{record{{"x", int64_t{3}}}}));
}

TEST("Arrow timestamps normalize units and reject overflow") {
  auto builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool()};
  REQUIRE(builder.Append(1234).ok());
  REQUIRE(builder.AppendNull().ok());
  auto result = import(*builder.Finish().ValueOrDie());
  auto times = result.get_alternative<nova::Time>();
  REQUIRE(times);
  CHECK(times->present.get(0));
  CHECK(not times->present.get(1));
  CHECK_EQUAL(*times->data.get(0), tenzir::time{duration{1234000000}});
  REQUIRE(builder.Append(std::numeric_limits<int64_t>::max()).ok());
  auto overflow = nova::import_arrow_array(*builder.Finish().ValueOrDie());
  REQUIRE(overflow.is_err());
  CHECK(not overflow.unwrap_err().empty());
}

TEST("Arrow dictionaries import their values rather than their indices") {
  auto builder = arrow::StringBuilder{};
  REQUIRE(builder.Append("first").ok());
  REQUIRE(builder.Append("second").ok());
  auto indices = arrow::Int8Builder{};
  REQUIRE(indices.Append(1).ok());
  REQUIRE(indices.AppendNull().ok());
  REQUIRE(indices.Append(0).ok());
  auto dictionary
    = arrow::DictionaryArray::FromArrays(indices.Finish().ValueOrDie(),
                                         builder.Finish().ValueOrDie())
        .ValueOrDie();
  auto result = import(*dictionary);
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{"second"});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(2)), data{"first"});
}

namespace {

auto make_dictionary(std::vector<Option<int32_t>> const& indices,
                     std::shared_ptr<arrow::Array> values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::Int32Builder{};
  for (auto index : indices) {
    REQUIRE((index ? builder.Append(*index) : builder.AppendNull()).ok());
  }
  return arrow::DictionaryArray::FromArrays(builder.Finish().ValueOrDie(),
                                            std::move(values))
    .ValueOrDie();
}

auto make_strings(std::vector<Option<std::string>> const& values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::StringBuilder{};
  for (auto const& value : values) {
    REQUIRE((value ? builder.Append(*value) : builder.AppendNull()).ok());
  }
  return builder.Finish().ValueOrDie();
}

template <class Tag>
auto is_constant(nova::Array<nova::Data> const& array) -> bool {
  auto values = array.get_alternative<Tag>();
  REQUIRE(values);
  using Constant
    = nova::storage::ConstantStorage<Tag, typename nova::Type<Tag>::ViewType>;
  return is<Constant>(values->data.storage());
}

} // namespace

TEST("Arrow dictionaries whose rows name one value import as constants") {
  auto result = import(*make_dictionary({0, 0, 0}, make_strings({"only"})));
  CHECK(is_constant<nova::String>(result));
  CHECK_EQUAL(result.length(), 3);
  CHECK_EQUAL(nova::materialize_legacy(result.get(2)), data{"only"});
  // Rows may all name one of several values, also within a slice.
  auto input
    = make_dictionary({0, 1, 1, 1}, make_strings({"a", "b"}))->Slice(1);
  result = import(*input);
  CHECK(is_constant<nova::String>(result));
  CHECK_EQUAL(result.length(), 3);
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{"b"});
  auto bytes = arrow::BinaryBuilder{};
  REQUIRE(bytes.Append(std::string_view{"\x01\x02"}).ok());
  result = import(*make_dictionary({0, 0}, bytes.Finish().ValueOrDie()));
  CHECK(is_constant<nova::Blob>(result));
  auto expected = blob{};
  expected.push_back(std::byte{1});
  expected.push_back(std::byte{2});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{expected});
}

TEST("Arrow dictionaries with nulls or several values import per row") {
  auto result = import(*make_dictionary({0, 1}, make_strings({"a", "b"})));
  CHECK(not is_constant<nova::String>(result));
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{"a"});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{"b"});
  result = import(*make_dictionary({0, None{}, 0}, make_strings({"a"})));
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{"a"});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(2)), data{"a"});
  // A null value is not a constant of its type.
  result = import(*make_dictionary({0, 0}, make_strings({None{}})));
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
  result = import(*make_dictionary({}, make_strings({"a"})));
  CHECK_EQUAL(result.length(), 0);
}

TEST("Arrow numeric imports preserve physical widths and own their buffers") {
  auto check = []<class ArrowType, class Tag>() {
    using Value = typename ArrowType::c_type;
    auto builder = arrow::NumericBuilder<ArrowType>{};
    REQUIRE(builder.Append(Value{99}).ok());
    REQUIRE(builder.Append(Value{7}).ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(builder.Append(Value{13}).ok());
    REQUIRE(builder.Append(std::numeric_limits<Value>::lowest()).ok());
    REQUIRE(builder.Append(std::numeric_limits<Value>::max()).ok());
    auto input = builder.Finish().ValueOrDie();
    auto result = import(*input->Slice(1));
    input.reset();
    auto values = result.template get_alternative<Tag>();
    REQUIRE(values);
    CHECK(is<nova::storage::SparseStorage<Value>>(values->data.storage()));
    CHECK_EQUAL(*values->data.get(0), static_cast<Tag>(7));
    CHECK_EQUAL(*values->data.get(2), static_cast<Tag>(13));
    CHECK_EQUAL(*values->data.get(3),
                static_cast<Tag>(std::numeric_limits<Value>::lowest()));
    CHECK_EQUAL(*values->data.get(4),
                static_cast<Tag>(std::numeric_limits<Value>::max()));
    CHECK(not values->present.get(1));
    CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
    auto empty = import(*builder.Finish().ValueOrDie());
    CHECK_EQUAL(empty.length(), 0);
  };
  check.template operator()<arrow::Int8Type, nova::Int>();
  check.template operator()<arrow::Int16Type, nova::Int>();
  check.template operator()<arrow::Int32Type, nova::Int>();
  check.template operator()<arrow::Int64Type, nova::Int>();
  check.template operator()<arrow::UInt8Type, nova::UInt>();
  check.template operator()<arrow::UInt16Type, nova::UInt>();
  check.template operator()<arrow::UInt32Type, nova::UInt>();
  check.template operator()<arrow::UInt64Type, nova::UInt>();
  check.template operator()<arrow::FloatType, nova::Float>();
  check.template operator()<arrow::DoubleType, nova::Float>();
}

TEST("Arrow boolean and validity bitmaps honor slices and clear padding") {
  auto builder = arrow::BooleanBuilder{};
  for (auto i = 0; i < 400; ++i) {
    REQUIRE(
      (i % 7 == 1 ? builder.AppendNull() : builder.Append(i % 5 != 2)).ok());
  }
  auto input = builder.Finish().ValueOrDie();
  for (auto offset : {0, 1, 7, 8, 63, 64, 65, 127, 128}) {
    for (auto length : {0, 1, 63, 64, 127, 128, 129, 257}) {
      auto result = import(*input->Slice(offset, length));
      CHECK_EQUAL(result.length(), length);
      for (auto i = 0; i < length; ++i) {
        auto row = offset + i;
        auto expected = row % 7 == 1 ? data{} : data{row % 5 != 2};
        CHECK_EQUAL(nova::materialize_legacy(result.get(i)), expected);
      }
      if (auto values = result.get_alternative<nova::Bool>()) {
        auto const& bits = as<nova::storage::BitMap>(values->data.storage());
        auto true_count = 0;
        for (auto i = 0; i < length; ++i) {
          true_count += bits.get(i);
        }
        CHECK_EQUAL(bits.true_count(), true_count);
        if (not bits.data().empty() and length % bits.word_bits != 0) {
          CHECK((bits.data().back() >> (length % bits.word_bits)) == 0);
        }
        auto const& present = values->present;
        if (not present.data().empty() and length % present.word_bits != 0) {
          CHECK((present.data().back() >> (length % present.word_bits)) == 0);
        }
      }
    }
  }
}

TEST("Arrow byte columns copy sliced payloads once and rebase spans") {
  auto check = []<class ArrowType, class Tag, class Char>() {
    auto builder = typename arrow::TypeTraits<ArrowType>::BuilderType{};
    auto text = std::string{"a\0b", 3};
    REQUIRE(builder.Append("excluded prefix").ok());
    REQUIRE(builder.Append(text).ok());
    REQUIRE(builder.Append("").ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(builder.Append("tail").ok());
    REQUIRE(builder.Append("excluded suffix").ok());
    auto input = builder.Finish().ValueOrDie();
    auto result = import(*input->Slice(1, 4));
    auto empty = import(*input->Slice(input->length(), 0));
    CHECK_EQUAL(empty.length(), 0);
    input.reset();
    auto values = result.template get_alternative<Tag>();
    REQUIRE(values);
    auto const& physical = as<typename nova::Type<Tag>::PrimaryPhysicalStorage>(
      values->data.storage());
    CHECK_EQUAL(physical.data().length(), 7);
    CHECK_EQUAL(physical.span(0).begin, 0);
    CHECK_EQUAL(physical.span(0).end, 3);
    CHECK_EQUAL(physical.span(3).begin, 3);
    CHECK_EQUAL(physical.span(3).end, 7);
    for (auto i : {0, 1, 3}) {
      auto expected = i == 0   ? text
                      : i == 1 ? std::string{}
                               : std::string{"tail"};
      auto bytes = *values->data.get(i);
      CHECK_EQUAL(bytes.size(), expected.size());
      CHECK(std::equal(bytes.begin(), bytes.end(),
                       reinterpret_cast<Char const*>(expected.data())));
    }
    CHECK_EQUAL(nova::materialize_legacy(result.get(2)), data{});
    REQUIRE(builder.Append("").ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(builder.Append("").ok());
    auto only_empty = import(*builder.Finish().ValueOrDie());
    auto empty_values = only_empty.template get_alternative<Tag>();
    REQUIRE(empty_values);
    CHECK((*empty_values->data.get(0)).empty());
    CHECK((*empty_values->data.get(2)).empty());
    CHECK_EQUAL(nova::materialize_legacy(only_empty.get(1)), data{});
  };
  check.template operator()<arrow::StringType, nova::String, char>();
  check.template operator()<arrow::BinaryType, nova::Blob, std::byte>();
}

TEST("Arrow structs build a shared shape and retain duplicate-field "
     "semantics") {
  auto builder = arrow::Int32Builder{};
  REQUIRE(builder.AppendValues(std::vector<int32_t>{1, 2, 3}).ok());
  auto first = builder.Finish().ValueOrDie();
  REQUIRE(builder.AppendValues(std::vector<int32_t>{4, 5, 6}).ok());
  auto last = builder.Finish().ValueOrDie();
  auto input = arrow::StructArray::Make({first, first, last},
                                        std::vector<std::string>{"x", "y", "x"})
                 .ValueOrDie();
  auto result = import(*input->Slice(1));
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)),
              (data{record{{"x", int64_t{5}}, {"y", int64_t{2}}}}));
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)),
              (data{record{{"x", int64_t{6}}, {"y", int64_t{3}}}}));
  CHECK_EQUAL(import(*input->Slice(3, 0)).length(), 0);
}

TEST("Arrow dictionary values and indices both contribute nulls") {
  auto builder = arrow::StringBuilder{};
  REQUIRE(builder.Append("first").ok());
  REQUIRE(builder.AppendNull().ok());
  auto indices = arrow::Int8Builder{};
  REQUIRE(indices.Append(0).ok());
  REQUIRE(indices.Append(1).ok());
  REQUIRE(indices.AppendNull().ok());
  auto input = arrow::DictionaryArray::FromArrays(indices.Finish().ValueOrDie(),
                                                  builder.Finish().ValueOrDie())
                 .ValueOrDie();
  auto result = import(*input);
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{"first"});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(2)), data{});
}

TEST("Arrow duration slices normalize units without losing nulls") {
  auto builder = arrow::DurationBuilder{arrow::duration(arrow::TimeUnit::MICRO),
                                        arrow::default_memory_pool()};
  REQUIRE(builder.Append(123).ok());
  REQUIRE(builder.AppendNull().ok());
  REQUIRE(builder.Append(-3).ok());
  auto result = import(*builder.Finish().ValueOrDie()->Slice(1));
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{duration{-3000}});
}

TEST("Arrow extension slices retain nulls and own their converted values") {
  auto check = []<class Type, class Value>(Type type, Value value) {
    auto builder = typename Type::builder_type{};
    REQUIRE(append_builder(type, builder, value).ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(append_builder(type, builder, value).ok());
    auto input = builder.Finish().ValueOrDie();
    auto result = import(*input->Slice(1));
    input.reset();
    CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{});
    CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{value});
  };
  check(ip_type{}, ip::v4(0x01020304));
  check(subnet_type{}, subnet{ip::v4(0xc0000200), 120});
}

TEST("Arrow subnet imports reject null children only in valid rows") {
  auto value = subnet{ip::v4(0xc0000200), 120};
  for (auto null_address : {false, true}) {
    for (auto null_length : {false, true}) {
      for (auto null_parent : {false, true}) {
        for (auto offset : {0, 1}) {
          for (auto owned : {false, true}) {
            auto builder = subnet_type::builder_type{};
            REQUIRE(append_builder(subnet_type{}, builder, value).ok());
            REQUIRE(builder.Append(not null_parent).ok());
            REQUIRE((null_address
                       ? builder.ip_builder().AppendNull()
                       : append_builder(ip_type{}, builder.ip_builder(),
                                        value.network()))
                      .ok());
            REQUIRE((null_length
                       ? builder.length_builder().AppendNull()
                       : builder.length_builder().Append(value.length()))
                      .ok());
            REQUIRE(append_builder(subnet_type{}, builder, value).ok());
            auto input = builder.Finish().ValueOrDie()->Slice(offset);
            // Arrow permits null struct children even with a valid parent.
            REQUIRE(input->ValidateFull().ok());
            auto valid_tail = import(*input->Slice(2 - offset));
            CHECK_EQUAL(nova::materialize_legacy(valid_tail.get(0)),
                        data{value});
            auto result = owned ? nova::import_arrow_array(std::move(input))
                                : nova::import_arrow_array(*input);
            if (not null_parent and (null_address or null_length)) {
              REQUIRE(result.is_err());
              CHECK_EQUAL(result.unwrap_err(),
                          "non-null subnet contains a null address or prefix "
                          "length");
              continue;
            }
            REQUIRE(result.is_ok());
            auto imported = std::move(result).unwrap();
            CHECK_EQUAL(imported.length(), 3 - offset);
            for (auto i = 0; i < imported.length(); ++i) {
              auto expected
                = null_parent and i + offset == 1 ? data{} : data{value};
              CHECK_EQUAL(nova::materialize_legacy(imported.get(i)), expected);
            }
          }
        }
      }
    }
  }
}

TEST("Arrow empty structs retain their row count") {
  auto input = arrow::StructArray{arrow::struct_({}), 3, arrow::ArrayVector{}};
  auto result = import(input);
  CHECK_EQUAL(result.length(), 3);
  for (auto i = 0; i < 3; ++i) {
    CHECK_EQUAL(nova::materialize_legacy(result.get(i)), data{record{}});
  }
}

TEST("owned Arrow numeric slices transfer buffers and retain their lifetime") {
  auto check = []<class ArrowType, class Tag>() {
    using Value = typename ArrowType::c_type;
    auto builder = arrow::NumericBuilder<ArrowType>{};
    REQUIRE(builder.Append(Value{99}).ok());
    REQUIRE(builder.Append(Value{7}).ok());
    REQUIRE(builder.AppendNull().ok());
    REQUIRE(builder.Append(Value{13}).ok());
    auto input = builder.Finish().ValueOrDie()->Slice(1);
    auto const* original = input->data()->buffers[1]->data() + sizeof(Value);
    auto lifetime = std::weak_ptr<arrow::Buffer>{input->data()->buffers[1]};
    {
      auto result = import(std::move(input));
      CHECK(not lifetime.expired());
      CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
      auto physical = take_sparse<Tag, Value>(std::move(result));
      auto mutable_values =
        typename nova::storage::SparseStorage<Value>::Mutable{
          std::move(physical)};
      CHECK_EQUAL(static_cast<void const*>(mutable_values.data()), original);
      CHECK_EQUAL(mutable_values.get(0), Value{7});
      CHECK_EQUAL(mutable_values.get(2), Value{13});
      auto owned = std::move(mutable_values).finish();
      auto alias = owned;
      auto changed =
        typename nova::storage::SparseStorage<Value>::Mutable{std::move(owned)};
      CHECK_NOT_EQUAL(static_cast<void const*>(changed.data()), original);
      changed.set(0, Value{42});
      CHECK_EQUAL(alias.get(0), Value{7});
    }
    CHECK(lifetime.expired());
  };
  check.template operator()<arrow::Int8Type, nova::Int>();
  check.template operator()<arrow::Int16Type, nova::Int>();
  check.template operator()<arrow::Int32Type, nova::Int>();
  check.template operator()<arrow::Int64Type, nova::Int>();
  check.template operator()<arrow::UInt8Type, nova::UInt>();
  check.template operator()<arrow::UInt16Type, nova::UInt>();
  check.template operator()<arrow::UInt32Type, nova::UInt>();
  check.template operator()<arrow::UInt64Type, nova::UInt>();
  check.template operator()<arrow::FloatType, nova::Float>();
  check.template operator()<arrow::DoubleType, nova::Float>();
}

TEST("owned Arrow byte slices transfer their payload buffers") {
  auto check = []<class ArrowType, class Tag>() {
    auto builder = typename arrow::TypeTraits<ArrowType>::BuilderType{};
    REQUIRE(builder.Append("prefix").ok());
    REQUIRE(builder.Append("value").ok());
    REQUIRE(builder.AppendNull().ok());
    auto input = builder.Finish().ValueOrDie()->Slice(1);
    auto const* original = input->data()->buffers[2]->data() + 6;
    auto result = import(std::move(input));
    auto values = result.template get_alternative<Tag>();
    REQUIRE(values);
    auto const& physical = as<typename nova::Type<Tag>::PrimaryPhysicalStorage>(
      values->data.storage());
    CHECK_EQUAL(static_cast<void const*>(physical.data().begin()), original);
    CHECK_EQUAL(physical.data().length(), 5);
    CHECK_EQUAL(physical.span(0).begin, 0);
    CHECK_EQUAL(physical.span(0).end, 5);
    CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{});
  };
  check.template operator()<arrow::StringType, nova::String>();
  check.template operator()<arrow::BinaryType, nova::Blob>();
}

TEST("owned Arrow bitmaps reuse aligned storage and copy unaligned slices") {
  for (auto offset : {0, 1, 7, 8, 127, 128}) {
    auto builder = arrow::BooleanBuilder{};
    for (auto i = 0; i < 400; ++i) {
      REQUIRE(
        (i % 7 == 1 ? builder.AppendNull() : builder.Append(i % 5 != 2)).ok());
    }
    auto input = builder.Finish().ValueOrDie()->Slice(offset, 129);
    auto const* original = input->data()->buffers[1]->data() + offset / 8;
    auto const* validity = input->data()->buffers[0]->data() + offset / 8;
    auto result = import(std::move(input));
    for (auto i = 0; i < 129; ++i) {
      auto row = offset + i;
      auto expected = row % 7 == 1 ? data{} : data{row % 5 != 2};
      CHECK_EQUAL(nova::materialize_legacy(result.get(i)), expected);
    }
    auto values = result.get_alternative<nova::Bool>();
    REQUIRE(values);
    auto const& bits = as<nova::storage::BitMap>(values->data.storage());
    if (offset % nova::storage::BitMap::word_bits == 0) {
      CHECK_EQUAL(static_cast<void const*>(bits.data().data()), original);
      auto nulls = result.get_alternative<nova::Null>();
      REQUIRE(nulls);
      CHECK_EQUAL(static_cast<void const*>(nulls->present.data().data()),
                  validity);
    } else {
      CHECK_NOT_EQUAL(static_cast<void const*>(bits.data().data()), original);
    }
    CHECK((bits.data().back() >> 1) == 0);
    auto alias = bits;
    auto inverted = std::move(alias).make_inverted();
    CHECK_EQUAL(inverted.true_count(), bits.length() - bits.true_count());
    CHECK_NOT_EQUAL(inverted.data().data(), bits.data().data());
  }
}

TEST("owned Arrow imports do not mutate aliased arrays data or buffers") {
  for (auto alias_level : {0, 1, 2}) {
    auto builder = arrow::Int32Builder{};
    REQUIRE(builder.Append(7).ok());
    auto input = builder.Finish().ValueOrDie();
    auto const* original = input->data()->buffers[1]->data();
    auto array_alias = alias_level == 0 ? input : nullptr;
    auto data_alias = alias_level == 1 ? input->data() : nullptr;
    auto buffer_alias = alias_level == 2 ? input->data()->buffers[1] : nullptr;
    auto physical = take_sparse<nova::Int, int32_t>(import(std::move(input)));
    auto values
      = nova::storage::SparseStorage<int32_t>::Mutable{std::move(physical)};
    CHECK_NOT_EQUAL(static_cast<void const*>(values.data()), original);
    values.set(0, 42);
    CHECK_EQUAL(*reinterpret_cast<int32_t const*>(original), 7);
  }
}

TEST("owned Arrow imports copy non-owning and immutable buffers") {
  for (auto mutable_buffer : {false, true}) {
    auto raw = std::vector<int32_t>{7, 13};
    auto* bytes = reinterpret_cast<uint8_t*>(raw.data());
    auto size = raw.size() * sizeof(int32_t);
    auto buffer = std::shared_ptr<arrow::Buffer>{};
    if (mutable_buffer) {
      buffer = std::make_shared<arrow::MutableBuffer>(bytes, size);
    } else {
      buffer = std::make_shared<arrow::Buffer>(bytes, size);
    }
    auto input = std::make_shared<arrow::Int32Array>(2, buffer);
    buffer.reset();
    auto physical = take_sparse<nova::Int, int32_t>(import(std::move(input)));
    auto values
      = nova::storage::SparseStorage<int32_t>::Mutable{std::move(physical)};
    CHECK_NOT_EQUAL(values.data(), raw.data());
    values.set(0, 42);
    CHECK_EQUAL(raw[0], 7);
  }
}

TEST("owned Arrow imports copy unaligned numeric buffers") {
  auto raw = std::vector<int32_t>{7, 13};
  auto size = raw.size() * sizeof(int32_t);
  auto allocation = arrow::AllocateResizableBuffer(size + 1).ValueOrDie();
  std::memcpy(allocation->mutable_data() + 1, raw.data(), size);
  auto input = std::make_shared<arrow::Int32Array>(
    2, arrow::SliceMutableBuffer(std::move(allocation), 1, size));
  auto result = import(std::move(input));
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), data{int64_t{7}});
  CHECK_EQUAL(nova::materialize_legacy(result.get(1)), data{int64_t{13}});
}

TEST("Arrow record batches adopt only unaliased nested payloads") {
  for (auto retain_child : {false, true}) {
    auto builder = arrow::StringBuilder{};
    REQUIRE(builder.Append("value").ok());
    auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("x", arrow::utf8())}), 1,
      {builder.Finish().ValueOrDie()});
    auto const* original = batch->column(0)->data()->buffers[2]->data();
    auto child_alias = retain_child ? batch->column(0) : nullptr;
    auto columns = batch->ToStructArray().ValueOrDie();
    batch.reset();
    auto result = import(std::move(columns));
    auto records = result.try_as<nova::Record>();
    REQUIRE(records);
    auto field = records->field("x");
    REQUIRE(field);
    auto strings = field->data.try_as<nova::String>();
    REQUIRE(strings);
    auto const& physical = as<nova::Type<nova::String>::PrimaryPhysicalStorage>(
      strings->storage());
    CHECK_EQUAL(static_cast<void const*>(physical.data().begin()) == original,
                not retain_child);
    CHECK_EQUAL(nova::materialize_legacy(result.get(0)),
                (data{record{{"x", "value"}}}));
  }
}

TEST("compressed IPC files and streams transfer decoded buffers into Nova") {
  struct BatchListener final : arrow::ipc::Listener {
    auto OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> input)
      -> arrow::Status override {
      batch = std::move(input);
      return arrow::Status::OK();
    }
    std::shared_ptr<arrow::RecordBatch> batch;
  };
  for (auto file_format : {false, true}) {
    auto integers = arrow::Int32Builder{};
    auto strings = arrow::StringBuilder{};
    REQUIRE(integers.AppendValues(std::vector<int32_t>(128, 42)).ok());
    REQUIRE(strings.AppendValues(std::vector<std::string>(128, "value")).ok());
    auto input = arrow::RecordBatch::Make(
      arrow::schema(
        {arrow::field("n", arrow::int32()), arrow::field("s", arrow::utf8())}),
      128, {integers.Finish().ValueOrDie(), strings.Finish().ValueOrDie()});
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    options.codec
      = arrow::util::Codec::Create(arrow::Compression::ZSTD).ValueOrDie();
    auto writer
      = (file_format
           ? arrow::ipc::MakeFileWriter(sink, input->schema(), options)
           : arrow::ipc::MakeStreamWriter(sink, input->schema(), options))
          .ValueOrDie();
    REQUIRE(writer->WriteRecordBatch(*input).ok());
    REQUIRE(writer->Close().ok());
    auto bytes = sink->Finish().ValueOrDie();
    auto listener = std::make_shared<BatchListener>();
    auto decoder
      = arrow::ipc::StreamDecoder{listener, arrow_ipc_read_options()};
    auto reader = std::shared_ptr<arrow::ipc::RecordBatchFileReader>{};
    auto batch = std::shared_ptr<arrow::RecordBatch>{};
    if (file_format) {
      reader = arrow::ipc::RecordBatchFileReader::Open(
                 std::make_shared<arrow::io::BufferReader>(bytes),
                 arrow_ipc_read_options())
                 .ValueOrDie();
      batch = reader->ReadRecordBatch(0).ValueOrDie();
    } else {
      REQUIRE(decoder.Consume(bytes).ok());
      batch = std::move(listener->batch);
    }
    REQUIRE(batch);
    auto const* original_numbers = batch->column(0)->data()->buffers[1]->data();
    auto const* original_strings = batch->column(1)->data()->buffers[2]->data();
    auto physical = [&] {
      auto columns = batch->ToStructArray().ValueOrDie();
      batch.reset();
      // Keep both decoder objects alive, just as the Feather reader does.
      auto result = import(std::move(columns));
      auto records = result.try_as<nova::Record>();
      REQUIRE(records);
      auto text = records->field("s");
      REQUIRE(text);
      auto strings = text->data.try_as<nova::String>();
      REQUIRE(strings);
      auto const& payload
        = as<nova::Type<nova::String>::PrimaryPhysicalStorage>(
          strings->storage());
      CHECK_EQUAL(static_cast<void const*>(payload.data().begin()),
                  original_strings);
      CHECK_EQUAL(nova::materialize_legacy(result.get(127)),
                  (data{record{{"n", int64_t{42}}, {"s", "value"}}}));
      auto numbers = records->field("n");
      REQUIRE(numbers);
      return take_sparse<nova::Int, int32_t>(std::move(numbers->data));
    }();
    auto values
      = nova::storage::SparseStorage<int32_t>::Mutable{std::move(physical)};
    CHECK_EQUAL(static_cast<void const*>(values.data()), original_numbers);
    CHECK_EQUAL(values.get(127), 42);
  }
}

TEST("owned Arrow list slices transfer flattened child payloads") {
  auto values = std::make_shared<arrow::StringBuilder>();
  auto lists = arrow::ListBuilder{arrow::default_memory_pool(), values};
  REQUIRE(lists.Append().ok());
  REQUIRE(values->Append("prefix").ok());
  REQUIRE(lists.Append().ok());
  REQUIRE(values->Append("value").ok());
  auto input = lists.Finish().ValueOrDie()->Slice(1);
  auto const* original = input->data()->child_data[0]->buffers[2]->data() + 6;
  auto result = import(std::move(input));
  auto imported = result.try_as<nova::List>();
  REQUIRE(imported);
  auto const& physical = as<nova::storage::ListStorage>(imported->storage());
  auto strings = physical.values().try_as<nova::String>();
  REQUIRE(strings);
  auto const& bytes
    = as<nova::Type<nova::String>::PrimaryPhysicalStorage>(strings->storage());
  CHECK_EQUAL(static_cast<void const*>(bytes.data().begin()), original);
  CHECK_EQUAL(nova::materialize_legacy(result.get(0)), (data{list{"value"}}));
}

TEST("Arrow import rejects unsupported columns without materializing them") {
  auto builder = arrow::Decimal256Builder{arrow::decimal256(50, 2)};
  auto array = builder.Finish().ValueOrDie();
  auto unsupported = nova::import_arrow_array(*array);
  REQUIRE(unsupported.is_err());
  CHECK_EQUAL(unsupported.unwrap_err(),
              "unsupported arrow type `decimal256(50, 2)`");
  auto nested = arrow::StructArray::Make({array}, std::vector<std::string>{"x"})
                  .ValueOrDie();
  auto nested_error = nova::import_arrow_array(*nested);
  REQUIRE(nested_error.is_err());
  CHECK_EQUAL(nested_error.unwrap_err(), unsupported.unwrap_err());
  auto oversized
    = arrow::NullArray{int64_t{std::numeric_limits<int32_t>::max()} + 1};
  auto capacity = nova::import_arrow_array(oversized);
  REQUIRE(capacity.is_err());
  CHECK_EQUAL(capacity.unwrap_err(),
              "Arrow column exceeds the supported row limit");
}
