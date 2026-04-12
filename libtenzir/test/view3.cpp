//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/view3.hpp"

#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"

#include <utility>

namespace tenzir {

namespace {

template <concrete_type Type, class Value>
auto make_singleton_array(const Type& type, Value&& value)
  -> std::shared_ptr<type_to_arrow_array_t<Type>> {
  auto builder = series_builder{tenzir::type{type}};
  builder.data(std::forward<Value>(value));
  auto result = builder.finish_assert_one_array();
  auto typed_result = result.as<Type>();
  REQUIRE(typed_result);
  return typed_result->array;
}

template <concrete_type Type>
auto make_null_array(const Type& type)
  -> std::shared_ptr<type_to_arrow_array_t<Type>> {
  auto builder = series_builder{tenzir::type{type}};
  builder.null();
  auto result = builder.finish_assert_one_array();
  auto typed_result = result.as<Type>();
  REQUIRE(typed_result);
  return typed_result->array;
}

TEST("view3 view_at non-null branches") {
  auto bool_array = make_singleton_array(bool_type{}, true);
  auto int_array = make_singleton_array(int64_type{}, int64_t{42});
  auto uint_array = make_singleton_array(uint64_type{}, uint64_t{42});
  auto double_array = make_singleton_array(double_type{}, 4.2);
  auto duration_array = make_singleton_array(duration_type{}, duration{123});
  auto time_value = time{} + duration{456};
  auto time_array = make_singleton_array(time_type{}, time_value);
  auto string_array = make_singleton_array(string_type{}, std::string{"hello"});
  auto ip_value = ip::v4(0xC0A80101);
  auto ip_array = make_singleton_array(ip_type{}, ip_value);
  auto subnet_value = subnet{ip::v4(0x0A000001), 24};
  auto subnet_array = make_singleton_array(subnet_type{}, subnet_value);
  auto enum_type = enumeration_type{{"zero", 0}, {"one", 1}};
  auto enum_array = make_singleton_array(enum_type, enumeration{1});
  auto list_array = make_singleton_array(list_type{int64_type{}},
                                         list{int64_t{7}, int64_t{8}});
  auto record_array = make_singleton_array(record_type{{"x", int64_type{}}},
                                           record{{"x", int64_t{9}}});
  auto blob_value = blob{};
  blob_value.push_back(std::byte{'a'});
  blob_value.push_back(std::byte{'b'});
  blob_value.push_back(std::byte{'c'});
  auto blob_array = make_singleton_array(blob_type{}, blob_value);
  auto secret_value = secret::make_literal("shh");
  auto secret_array = make_singleton_array(secret_type{}, secret_value);

  CHECK_EQUAL(*view_at(*bool_array, 0), true);
  CHECK_EQUAL(*view_at(*int_array, 0), int64_t{42});
  CHECK_EQUAL(*view_at(*uint_array, 0), uint64_t{42});
  CHECK_EQUAL(*view_at(*double_array, 0), 4.2);
  CHECK_EQUAL(*view_at(*duration_array, 0), duration{123});
  CHECK_EQUAL(*view_at(*time_array, 0), time_value);
  CHECK_EQUAL(*view_at(*string_array, 0), std::string_view{"hello"});
  CHECK_EQUAL(*view_at(*ip_array, 0), ip_value);
  CHECK_EQUAL(*view_at(*subnet_array, 0), subnet_value);
  CHECK_EQUAL(*view_at(*enum_array, 0), enumeration{1});
  auto list_view = view_at(*list_array, 0);
  REQUIRE(list_view);
  REQUIRE_EQUAL(list_view->size(), 2u);
  auto list_it = list_view->begin();
  CHECK_EQUAL(as<int64_t>(*list_it), int64_t{7});
  ++list_it;
  CHECK_EQUAL(as<int64_t>(*list_it), int64_t{8});
  auto record_view = view_at(*record_array, 0);
  REQUIRE(record_view);
  auto record_it = record_view->begin();
  REQUIRE(record_it != record_view->end());
  auto [field, value] = *record_it;
  CHECK_EQUAL(field, "x");
  CHECK_EQUAL(as<int64_t>(value), int64_t{9});
  ++record_it;
  CHECK(record_it == record_view->end());
  auto blob_view = view_at(*blob_array, 0);
  REQUIRE(blob_view);
  REQUIRE_EQUAL(blob_view->size(), blob_value.size());
  CHECK_EQUAL((*blob_view)[0], blob_value[0]);
  CHECK_EQUAL((*blob_view)[1], blob_value[1]);
  CHECK_EQUAL((*blob_view)[2], blob_value[2]);
  auto secret_view = view_at(*secret_array, 0);
  REQUIRE(secret_view);
  CHECK_EQUAL(materialize(*secret_view), secret_value);
}

TEST("view3 view_at null semantics") {
  auto null_array = make_null_array(null_type{});
  CHECK(not view_at(*null_array, 0));

  auto bool_array = make_null_array(bool_type{});
  CHECK(not view_at(*bool_array, 0));
  auto erased = view_at(static_cast<const arrow::Array&>(*bool_array), 0);
  CHECK(is<caf::none_t>(erased));
}

} // namespace

} // namespace tenzir
