//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concepts.hpp"

#include "tenzir/concept/support/unused_type.hpp"
#include "tenzir/test/test.hpp"

#include <array>
#include <type_traits>

TEST("transparent") {
  struct with {
    using is_transparent = std::true_type;
  };
  struct without {};
  static_assert(tenzir::concepts::transparent<with>);
  static_assert(!tenzir::concepts::transparent<without>);
}

TEST("container") {
  static_assert(tenzir::concepts::container<std::array<int, 1>>);
  struct empty {};
  static_assert(!tenzir::concepts::container<empty>);
  struct user_defined_type {
    auto data() const {
      return nullptr;
    }
    auto size() const {
      return 0;
    }
  };
  static_assert(tenzir::concepts::container<user_defined_type>);
}

TEST("byte_container") {
  using byte_array = std::array<std::uint8_t, 2>;
  static_assert(tenzir::concepts::byte_container<byte_array>);
  using u32_array = std::array<std::uint32_t, 2>;
  static_assert(!tenzir::concepts::byte_container<u32_array>);
  struct not_byte_container {};
  static_assert(!tenzir::concepts::byte_container<not_byte_container>);
}

// -- inspectable --------------------------------------------------------------

struct inspect_friend {
  bool value;
  template <class Inspector>
  friend auto inspect(Inspector& f, inspect_friend& x) {
    return f.apply(x);
  }
};

struct inspect_free {
  bool value;
};

template <class I>
auto inspect(I& i, inspect_free& x) {
  return i.apply(x);
}

TEST("inspectable") {
  static_assert(tenzir::concepts::inspectable<inspect_friend>);
  static_assert(tenzir::concepts::inspectable<inspect_free>);
  static_assert(!tenzir::concepts::inspectable<std::array<bool, 2>>);
}

// -- monoid -------------------------------------------------------------------

struct monoid_friend {
  bool value;
  friend monoid_friend
  mappend(const monoid_friend& lhs, const monoid_friend& rhs) {
    return {lhs.value || rhs.value};
  }
};

struct monoid_free {
  bool value;
};

monoid_free mappend(const monoid_free& lhs, const monoid_free& rhs) {
  return {lhs.value || rhs.value};
}

struct monoid_bad {
  bool value;
  friend tenzir::unused_type mappend(const monoid_bad&, const monoid_bad&) {
    return tenzir::unused;
  }
};

TEST("monoid") {
  static_assert(tenzir::concepts::monoid<monoid_friend>);
  static_assert(tenzir::concepts::monoid<monoid_free>);
  static_assert(!tenzir::concepts::monoid<monoid_bad>);
}

TEST("sameish") {
  static_assert(tenzir::concepts::sameish<int, int&>);
  static_assert(tenzir::concepts::sameish<int&, int>);
  static_assert(tenzir::concepts::sameish<const int, int>);
  static_assert(tenzir::concepts::sameish<int, int&>);
  static_assert(tenzir::concepts::sameish<int, const int&>);
  static_assert(tenzir::concepts::sameish<const int&, int>);
  static_assert(tenzir::concepts::sameish<const int&, int&>);
  static_assert(!tenzir::concepts::sameish<int, bool>);
}
