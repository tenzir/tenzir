//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/uuid.hpp"

#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/uuid.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/test/test.hpp"

#include <span>
#include <string>
#include <string_view>

using namespace tenzir;

TEST("pod size") {
  CHECK(sizeof(uuid) == size_t{16});
}

TEST("parseable and printable") {
  auto x = to<uuid>("01234567-89ab-cdef-0123-456789abcdef");
  REQUIRE(x);
  CHECK_EQUAL(to_string(*x), "01234567-89ab-cdef-0123-456789abcdef");
}

TEST("UUID spelling variants") {
  for (auto text : {"01234567-89ab-cdef-0123-456789abcdef",
                    "01234567-89AB-CDEF-0123-456789ABCDEF",
                    "0123456789abcdef0123456789abcdef",
                    "{01234567-89ab-cdef-0123-456789abcdef}",
                    "{0123456789abcdef0123456789abcdef}"}) {
    auto parsed = to<uuid>(std::string_view{text});
    REQUIRE(parsed);
    CHECK_EQUAL(to_string(*parsed), "01234567-89ab-cdef-0123-456789abcdef");
    CHECK(parsers::uuid(std::string_view{text}));
  }
}

TEST("UUID rejects non-hexadecimal characters in either nibble") {
  auto text = std::string{"01234567-89ab-cdef-0123-456789abcdef"};
  for (auto& c : text) {
    if (c == '-') {
      continue;
    }
    auto original = c;
    for (auto invalid : std::string_view{"gG/:\0\xff", 6}) {
      c = invalid;
      CHECK(not to<uuid>(text));
      CHECK(not parsers::uuid(text));
    }
    c = original;
  }
}

TEST("UUID rejects malformed delimiters and trailing input") {
  for (auto text : {"", "{", "01234567-89ab-cdef-0123-456789abcde",
                    "01234567-89ab-cdef-0123-456789abcdef0",
                    "01234567-89ab-cdef-0123456789abcdef",
                    "0123456789ab-cdef-0123-456789abcdef",
                    "{01234567-89ab-cdef-0123-456789abcdef",
                    "{01234567-89ab-cdef-0123-456789abcdefx",
                    "{01234567-89ab-cdef-0123-456789abcdef}}",
                    "01234567-89ab-cdef-0123-456789abcdef}"}) {
    CHECK(not to<uuid>(std::string_view{text}));
    CHECK(not parsers::uuid(std::string_view{text}));
  }
}

TEST("construction from span") {
  std::array<char, 16> bytes{0, 1, 2,  3,  4,  5,  6,  7,
                             8, 9, 10, 12, 12, 13, 14, 15};
  auto bytes_view = as_bytes(std::span<char, 16>{bytes});
  CHECK(bytes_view == as_bytes(uuid{bytes_view}));
}
