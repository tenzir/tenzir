//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/inspection_common.hpp"

#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace {

template <bool IsLoading>
struct dummy_inspector {
  static constexpr bool is_loading = IsLoading;
};

using dummy_loading_inspector = dummy_inspector<true>;
using dummy_saving_inspector = dummy_inspector<false>;

} // namespace

TEST("callback is invoked and the fields invocation "
     "returns true when all fields and callback return true") {
  dummy_saving_inspector inspector;
  std::size_t callback_calls_count{0u};
  bool field1_invoked = false;
  bool field2_invoked = false;
  // create sut
  auto callback = [&] {
    ++callback_calls_count;
    REQUIRE(field2_invoked);
    return true;
  };
  auto sut = tenzir::detail::inspection_object{inspector}.on_save(callback);
  // create fields
  auto field1 = [&](auto&) {
    REQUIRE_EQUAL(callback_calls_count, 0u);
    REQUIRE(! field2_invoked);
    field1_invoked = true;
    return true;
  };
  auto field2 = [&](auto&) {
    REQUIRE_EQUAL(callback_calls_count, 0u);
    REQUIRE(field1_invoked);
    REQUIRE(! field2_invoked);
    field2_invoked = true;
    return true;
  };
  // verify
  CHECK(sut.fields(field1, field2));
  CHECK_EQUAL(callback_calls_count, 1u);
}

TEST("callback and second field arent invoked and the fields invocation "
     "returns false when first field returned false") {
  dummy_loading_inspector inspector;
  std::size_t callback_calls_count{0u};
  // create sut
  auto callback = [&] {
    ++callback_calls_count;
    return true;
  };
  auto sut = tenzir::detail::inspection_object{inspector}.on_load(callback);
  // create fields
  bool field1_invoked = false;
  bool field2_invoked = false;
  auto field1 = [&](auto&) {
    REQUIRE_EQUAL(callback_calls_count, 0u);
    REQUIRE(! field2_invoked);
    field1_invoked = true;
    return false;
  };
  auto field2 = [&](auto&) {
    return true;
  };
  // verify
  CHECK(! sut.fields(field1, field2));
  CHECK_EQUAL(callback_calls_count, 0u);
  CHECK(field1_invoked);
  CHECK(! field2_invoked);
}

TEST("fields invocation returns false when callback returns false") {
  dummy_saving_inspector inspector;
  std::size_t callback_calls_count{0u};
  auto callback = [&] {
    ++callback_calls_count;
    return false;
  };
  auto sut = tenzir::detail::inspection_object{inspector}.on_save(callback);
  CHECK(! sut.fields([&](auto&) {
    return true;
  }));
  CHECK_EQUAL(callback_calls_count, 1u);
}

TEST("on_save doesnt call callback when inspector has is_loading set to true") {
  dummy_loading_inspector inspector;
  std::size_t callback_calls_count{0u};
  auto callback = [&] {
    ++callback_calls_count;
    return true;
  };
  auto sut = tenzir::detail::inspection_object{inspector};
  auto& out = sut.on_save(callback);
  // on_save should return reference of the sut if it can't use callback
  static_assert(std::is_same_v<decltype(sut), std::decay_t<decltype(out)>>);
  CHECK_EQUAL(std::addressof(sut), std::addressof(out));
  CHECK(sut.fields([&](auto&) {
    return true;
  }));
  CHECK_EQUAL(callback_calls_count, 0u);
}

TEST("on_load doesnt call callback when inspector has is_loading set to "
     "false") {
  dummy_saving_inspector inspector;
  std::size_t callback_calls_count{0u};
  auto callback = [&] {
    ++callback_calls_count;
    return true;
  };
  auto sut = tenzir::detail::inspection_object{inspector};
  auto& out = sut.on_load(callback);
  // on_load should return reference of the sut if it can't use callback
  static_assert(std::is_same_v<decltype(sut), std::decay_t<decltype(out)>>);
  CHECK_EQUAL(std::addressof(sut), std::addressof(out));
  CHECK(sut.fields([&](auto&) {
    return true;
  }));
  CHECK_EQUAL(callback_calls_count, 0u);
}

TEST("inspect_enum with caf binary inspectors") {
  enum class enum_example { value_1 = 5201, value_2 = 8 };
  // serialize
  auto in = enum_example::value_1;
  caf::byte_buffer buf;
  caf::binary_serializer serializer{buf};
  tenzir::detail::inspect_enum(serializer, in);
  // deserialize
  auto out = enum_example::value_2;
  caf::binary_deserializer deserializer{buf};
  tenzir::detail::inspect_enum(deserializer, out);
  CHECK_EQUAL(in, out);
}
