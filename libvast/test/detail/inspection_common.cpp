//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE inspection_common

#include "vast/detail/inspection_common.hpp"

#include "vast/test/test.hpp"

namespace {

template <bool IsLoading>
struct dummy_inspector {
  static constexpr bool is_loading = IsLoading;
};

using dummy_loading_inspector = dummy_inspector<true>;
using dummy_saving_inspector = dummy_inspector<false>;

} // namespace

TEST(callback is invoked and the fields invocation returns true when all
       fields and callback return true) {
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
  auto sut = vast::detail::inspection_object{inspector}.on_save(callback);
  // create fields
  auto field1 = [&](auto&) {
    REQUIRE_EQUAL(callback_calls_count, 0u);
    REQUIRE(!field2_invoked);
    field1_invoked = true;
    return true;
  };
  auto field2 = [&](auto&) {
    REQUIRE_EQUAL(callback_calls_count, 0u);
    REQUIRE(field1_invoked);
    REQUIRE(!field2_invoked);
    field2_invoked = true;
    return true;
  };
  // verify
  CHECK(sut.fields(field1, field2));
  CHECK_EQUAL(callback_calls_count, 1u);
}

TEST(callback and second field arent invoked and the fields invocation
       returns false when first field returned false) {
  dummy_loading_inspector inspector;
  std::size_t callback_calls_count{0u};
  // create sut
  auto callback = [&] {
    ++callback_calls_count;
    return true;
  };
  auto sut = vast::detail::inspection_object{inspector}.on_load(callback);
  // create fields
  bool field1_invoked = false;
  bool field2_invoked = false;
  auto field1 = [&](auto&) {
    REQUIRE_EQUAL(callback_calls_count, 0u);
    REQUIRE(!field2_invoked);
    field1_invoked = true;
    return false;
  };
  auto field2 = [&](auto&) {
    return true;
  };
  // verify
  CHECK(!sut.fields(field1, field2));
  CHECK_EQUAL(callback_calls_count, 0u);
  CHECK(field1_invoked);
  CHECK(!field2_invoked);
}

TEST(fields invocation returns false when callback returns false) {
  dummy_saving_inspector inspector;
  std::size_t callback_calls_count{0u};
  auto callback = [&] {
    ++callback_calls_count;
    return false;
  };
  auto sut = vast::detail::inspection_object{inspector}.on_save(callback);
  CHECK(!sut.fields([&](auto&) {
    return true;
  }));
  CHECK_EQUAL(callback_calls_count, 1u);
}
