/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE msgpack
#include "vast/msgpack.hpp"

#include "vast/test/test.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/msgpack_builder.hpp"
#include "vast/time.hpp"

#include <caf/test/dsl.hpp>

#include <array>
#include <string>
#include <vector>

using namespace vast;
using namespace vast::msgpack;
using namespace std::string_literals;
using namespace std::string_view_literals;
using vast::byte;

namespace {

struct fixture {
  fixture() : builder{buf} {
    // nop
  }

  auto data(size_t at = 0) const {
    REQUIRE(at < buf.size());
    return as_bytes(span{buf.data() + at, buf.size() - at});
  }

  std::vector<byte> buf;
  msgpack::builder<> builder;
};

template <class T>
void check_value(object o, T x) {
  visit(detail::overload{
          [](auto) { FAIL("invalid type dispatch"); },
          [=](T y) { CHECK_EQUAL(y, x); },
        },
        o);
}

} // namespace

TEST(format) {
  CHECK(is_fixstr(vast::msgpack::format{0b1010'0000}));
  CHECK(is_fixstr(vast::msgpack::format{0b1010'0001}));
  CHECK(is_fixstr(vast::msgpack::format{0b1011'1111}));
  CHECK(is_fixarray(vast::msgpack::format{0b1001'0000}));
  CHECK(is_fixarray(vast::msgpack::format{0b1001'1011}));
  CHECK(is_fixarray(vast::msgpack::format{0b1001'1111}));
  CHECK(is_fixmap(vast::msgpack::format{0b1000'0000}));
  CHECK(is_fixmap(vast::msgpack::format{0b1000'1011}));
  CHECK(is_fixmap(vast::msgpack::format{0b1000'1111}));
}

FIXTURE_SCOPE(msgpack_tests, fixture)

TEST(nil) {
  CHECK_EQUAL(builder.add<nil>(), 1u);
  CHECK_EQUAL(buf[0], static_cast<byte>(nil));
  check_value(object{data()}, caf::none);
}

TEST(invalid format) {
  auto never_used = static_cast<vast::msgpack::format>(0xc1);
  buf.push_back(byte{never_used});
  check_value(object{buf}, never_used);
}

TEST(boolean) {
  CHECK_EQUAL(builder.add<true_>(), 1u);
  CHECK_EQUAL(builder.add<false_>(), 1u);
  CHECK_EQUAL(buf[0], static_cast<byte>(true_));
  CHECK_EQUAL(buf[1], static_cast<byte>(false_));
}

TEST(positive fixint) {
  CHECK_EQUAL(builder.add<positive_fixint>(-1), 0u);
  CHECK_EQUAL(builder.add<positive_fixint>(0), 1u);
  CHECK_EQUAL(builder.add<positive_fixint>(42), 1u);
  CHECK_EQUAL(buf[1], static_cast<byte>(42 & positive_fixint));
  CHECK_EQUAL(builder.add<positive_fixint>(128), 0u);
  auto x0 = object{data()};
  CHECK_EQUAL(x0.format(), 0u);
  check_value(x0, uint8_t{0});
  auto x1 = object{data(1)};
  check_value(x1, uint8_t{42});
}

TEST(negative fixint) {
  CHECK_EQUAL(builder.add<negative_fixint>(-33), 0u);
  CHECK_EQUAL(builder.add<negative_fixint>(-30), 1u);
  CHECK_EQUAL(buf[0], static_cast<byte>(-30 & negative_fixint));
  CHECK_EQUAL(builder.add<negative_fixint>(0), 0u);
  CHECK_EQUAL(builder.add<negative_fixint>(42), 0u);
  check_value(object{data()}, int8_t{-30});
}

TEST(uint) {
  CHECK_EQUAL(builder.add<uint8>(uint8_t{0x11}), 2u);
  CHECK_EQUAL(builder.add<uint16>(uint16_t{0x1122}), 3u);
  CHECK_EQUAL(builder.add<uint32>(uint32_t{0x11223344}), 5u);
  CHECK_EQUAL(builder.add<uint64>(uint64_t{0x1122334455667788}), 9u);
  check_value(object{data()}, uint8_t{0x11});
  check_value(object{data(2)}, uint16_t{0x1122});
  check_value(object{data(5)}, uint32_t{0x11223344});
  check_value(object{data(10)}, uint64_t{0x1122334455667788});
}

TEST(int) {
  CHECK_EQUAL(builder.add<int8>(int8_t{0x11}), 2u);
  CHECK_EQUAL(builder.add<int16>(int16_t{0x1122}), 3u);
  CHECK_EQUAL(builder.add<int32>(int32_t{0x11223344}), 5u);
  CHECK_EQUAL(builder.add<int64>(int64_t{0x1122334455667788}), 9u);
  check_value(object{data()}, int8_t{0x11});
  check_value(object{data(2)}, int16_t{0x1122});
  check_value(object{data(5)}, int32_t{0x11223344});
  check_value(object{data(10)}, int64_t{0x1122334455667788});
}

TEST(float) {
  CHECK_EQUAL(builder.add<float32>(4.2f), 5u);
  CHECK_EQUAL(builder.add<float64>(4.2), 9u);
  check_value(object{data()}, 4.2f);
  check_value(object{data(5)}, 4.2);
}

TEST(fixstr) {
  CHECK_EQUAL(builder.add<fixstr>(""sv), 1u);
  CHECK_EQUAL(static_cast<uint8_t>(buf[0]), 0b1010'0000);
  CHECK_EQUAL(builder.add<fixstr>("foo"sv), 1u + 3);
  auto str = std::string(32, 'x');
  CHECK_EQUAL(builder.add<fixstr>(str), 0u);
  check_value(object{data()}, std::string_view{});
  check_value(object{data(1)}, "foo"sv);
}

TEST(str8) {
  CHECK_EQUAL(builder.add<str8>(""), 1u + 1);
  CHECK_EQUAL(builder.add<str8>("foo"), 1u + 1 + 3);
  auto str = std::string(255, 'x');
  CHECK_EQUAL(builder.add<str8>(str), 1u + 1 + str.size());
  str += 'x';
  CHECK_EQUAL(builder.add<str8>(str), 0u);
  check_value(object{data()}, std::string_view{});
  check_value(object{data(2)}, "foo"sv);
  str.pop_back();
  check_value(object{data(7)}, std::string_view{str});
}

TEST(str16) {
  CHECK_EQUAL(builder.add<str16>(""), 1u + 2);
  CHECK_EQUAL(builder.add<str16>("foo"), 1u + 2 + 3);
  auto str = std::string(1000, 'x');
  CHECK_EQUAL(builder.add<str16>(str), 1u + 2 + str.size());
  check_value(object{data()}, std::string_view{});
  check_value(object{data(3)}, "foo"sv);
}

TEST(str32) {
  CHECK_EQUAL(builder.add<str32>(""), 1u + 4);
  CHECK_EQUAL(builder.add<str32>("foo"), 1u + 4 + 3);
  check_value(object{data()}, std::string_view{});
  check_value(object{data(5)}, "foo"sv);
}

TEST(fixarray) {
  auto proxy = builder.build<fixarray>();
  CHECK_EQUAL(proxy.add<true_>(), 1u);
  CHECK_EQUAL(proxy.add<float32>(4.2f), 5u);
  CHECK_EQUAL(proxy.add<fixstr>("foo"), 4u);
  CHECK_EQUAL(builder.add(std::move(proxy)), 11u);
  auto o = object{buf};
  REQUIRE(is_fixarray(o.format()));
  auto view = unbox(get<array_view>(o));
  CHECK_EQUAL(view.size(), 3u);
  auto xs = view.data();
  auto x0 = xs.get();
  CHECK_EQUAL(xs.next(), 1u);
  auto x1 = xs.get();
  CHECK_EQUAL(xs.next(), 5u);
  auto x2 = xs.get();
  check_value(x0, true);
  check_value(x1, 4.2f);
  check_value(x2, "foo"sv);
}

TEST(array16) {
  auto proxy = builder.build<array16>();
  for (auto x : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    CHECK_EQUAL(proxy.add<int32>(x), 5u);
  CHECK_EQUAL(builder.add(std::move(proxy)), 53u);
  auto o = object{buf};
  REQUIRE_EQUAL(o.format(), array16);
  auto view = unbox(get<array_view>(o));
  auto xs = view.data();
  REQUIRE_EQUAL(view.size(), 10u);
  auto first = xs.get();
  check_value(first, 1);
  REQUIRE(xs.next(9) > 0);
  auto last = xs.get();
  check_value(last, 10);
}

TEST(fixmap) {
  auto proxy = builder.build<map16>();
  CHECK_EQUAL(proxy.add<int16>(42), 3u);          // key
  CHECK_EQUAL(proxy.add<true_>(), 1u);            // value
  CHECK_EQUAL(proxy.add<int16>(43), 3u);          // key
  CHECK_EQUAL(proxy.add<positive_fixint>(7), 1u); // value
  CHECK_EQUAL(proxy.add<int16>(44), 3u);          // key
  CHECK_EQUAL(proxy.add<fixstr>("foo"), 4u);      // value
  CHECK_EQUAL(builder.add(std::move(proxy)), 18u);
  array_view view{map16, 3, data(3)};
  CHECK_EQUAL(view.size(), 3u);
  auto xs = view.data();
  auto x0 = xs.get();
  CHECK_EQUAL(xs.next(), 3u);
  auto y0 = xs.get();
  CHECK_EQUAL(xs.next(), 1u);
  auto x1 = xs.get();
  CHECK_EQUAL(xs.next(), 3u);
  auto y1 = xs.get();
  CHECK_EQUAL(xs.next(), 1u);
  auto x2 = xs.get();
  CHECK_EQUAL(xs.next(), 3u);
  auto y2 = xs.get();
  CHECK_EQUAL(xs.next(), 4u); // advance to end
  check_value(x0, int16_t{42});
  check_value(y0, true);
  check_value(x1, int16_t{43});
  check_value(y1, uint8_t{7});
  check_value(x2, int16_t{44});
  check_value(y2, "foo"sv);
}

TEST(fixext) {
  auto a1 = std::array<int8_t, 1>{{1}};
  auto a2 = std::array<int8_t, 2>{{1, 2}};
  auto a4 = std::array<int8_t, 4>{{1, 2, 3, 4}};
  auto a8 = std::array<int8_t, 8>{{1, 2, 3, 4, 5, 6, 7, 8}};
  auto s1 = as_bytes(span<int8_t>{a1});
  auto s2 = as_bytes(span<int8_t>{a2});
  auto s4 = as_bytes(span<int8_t>{a4});
  auto s8 = as_bytes(span<int8_t>{a8});
  CHECK_EQUAL(builder.add<fixext1>(42, s1), 1u + 1 + 1);
  CHECK_EQUAL(builder.add<fixext2>(42, s2), 1u + 1 + 2);
  CHECK_EQUAL(builder.add<fixext4>(42, s4), 1u + 1 + 4);
  CHECK_EQUAL(builder.add<fixext8>(42, s8), 1u + 1 + 8);
  auto ev1 = ext_view{fixext1, 42, s1};
  auto ev2 = ext_view{fixext2, 42, s2};
  auto ev4 = ext_view{fixext4, 42, s4};
  auto ev8 = ext_view{fixext8, 42, s8};
  check_value(object{data()}, ev1);
  check_value(object{data(3)}, ev2);
  check_value(object{data(7)}, ev4);
  check_value(object{data(13)}, ev8);
}

TEST(ext8 via proxy) {
  auto foobar = "foobar"s;
  auto proxy = builder.build<ext8>();
  CHECK_EQUAL(proxy.add<fixstr>(std::string_view{foobar}), foobar.size() + 1);
  CHECK_EQUAL(proxy.add<uint8>(uint8_t{7}), 2u);
  auto size = header_size<ext8>() + header_size<fixstr>() + foobar.size() + 2;
  auto result = builder.add(std::move(proxy), 42);
  CHECK_EQUAL(result, size);
  auto inner = as_bytes(
    span{buf.data() + header_size<ext8>(), buf.size() - header_size<ext8>()});
  auto view = unbox(get<ext_view>(object{span<const byte>{buf}}));
  auto expected = ext_view{ext8, 42, inner};
  CHECK_EQUAL(view, expected);
  MESSAGE("verify inner data");
  auto o = overlay{view.data()};
  auto str = unbox(get<std::string_view>(o.get()));
  CHECK_EQUAL(str, foobar);
  CHECK_EQUAL(o.next(), foobar.size() + 1);
  auto seven = unbox(get<uint8_t>(o.get()));
  CHECK_EQUAL(seven, 7u);
}

TEST(ext16) {
  auto foobar = "foobar"s;
  auto xs = as_bytes(span{foobar.data(), foobar.size()});
  CHECK_EQUAL(builder.add<ext16>(42, xs), 1u + 2 + 1 + foobar.size());
}

TEST(time) {
  using namespace std::chrono;
  auto x0 = vast::time{}; // UNIX epoch
  MESSAGE("adding timestamp32");
  CHECK_EQUAL(builder.add(x0), 6u); // => fixext4 => 6 bytes
  auto x1 = vast::time{x0 + seconds{42}};
  CHECK_EQUAL(builder.add(x1), 6u); // timestamp32 => fixext4 => 6 bytes
  MESSAGE("adding timestamp64");
  auto x2 = vast::time{x1 + nanoseconds{1337}};
  CHECK_EQUAL(builder.add(x2), 10u); // timestamp64 => fixext8 => 10 bytes
  MESSAGE("adding timestamp96");
  auto secs = seconds{1ll << 34};
  auto ns = nanoseconds{42};
  auto x3 = vast::time{duration_cast<nanoseconds>(secs) + ns};
  CHECK_EQUAL(builder.add(secs, ns), 15u); // timestamp96 => ext8 => 15 bytes
  MESSAGE("verifying");
  check_value(object{data()}, x0);
  check_value(object{data(6)}, x1);
  check_value(object{data(6 + 6)}, x2);
  check_value(object{data(6 + 6 + 10)}, x3);
}

TEST(overlay) {
  CHECK_EQUAL(builder.add<str32>("foo"), 8u);
  CHECK_EQUAL(builder.add<nil>(), 1u);
  CHECK_EQUAL(builder.add<int32>(42), 5u);
  CHECK_EQUAL(builder.add<false_>(), 1u);
  auto xs = overlay{buf};
  check_value(xs.get(), std::string_view{"foo"});
  CHECK_EQUAL(xs.next(), 8u);
  CHECK_EQUAL(xs.get().format(), nil);
  CHECK_EQUAL(xs.next(), 1u);
  check_value(xs.get(), int32_t{42});
  CHECK_EQUAL(xs.next(), 5u);
  check_value(xs.get(), false);
}

TEST(put int8) {
  CHECK_EQUAL(put(builder, int8_t{-31}), 1u);
  CHECK(is_negative_fixint(object{buf}.format()));
  builder.reset();
  CHECK_EQUAL(put(builder, int8_t{0}), 1u);
  CHECK(is_positive_fixint(object{buf}.format()));
  builder.reset();
  CHECK_EQUAL(put(builder, int8_t{31}), 1u);
  CHECK(is_positive_fixint(object{buf}.format()));
  builder.reset();
  CHECK_EQUAL(put(builder, int8_t{42}), 2u);
  CHECK_EQUAL(object{buf}.format(), int8);
  builder.reset();
  CHECK_EQUAL(put(builder, int8_t{127}), 2u);
  CHECK_EQUAL(object{buf}.format(), int8);
}

TEST(put uint8) {
  CHECK_EQUAL(put(builder, uint8_t{0}), 1u);
  CHECK(is_positive_fixint(object{buf}.format()));
  builder.reset();
  CHECK_EQUAL(put(builder, uint8_t{31}), 1u);
  CHECK(is_positive_fixint(object{buf}.format()));
  builder.reset();
  CHECK_EQUAL(put(builder, uint8_t{42}), 2u);
  CHECK_EQUAL(object{buf}.format(), uint8);
  builder.reset();
  CHECK_EQUAL(put(builder, uint8_t{255}), 2u);
  CHECK_EQUAL(object{buf}.format(), uint8);
}

TEST(put vector) {
  auto xs = std::vector<int>{1, 2, 3, 4};
  CHECK_EQUAL(put(builder, xs), 1u + 4);
  auto o = object{buf};
  REQUIRE(is_fixarray(o.format()));
  auto v = unbox(get<array_view>(o));
  CHECK_EQUAL(v.size(), 4u);
  auto ys = v.data();
  auto first = unbox(get<uint8_t>(ys.get())); // positive_fixint
  CHECK_EQUAL(first, 1);
  ys.next(3);
  auto last = unbox(get<uint8_t>(ys.get())); // positive_fixint
  CHECK_EQUAL(last, 4);
}

TEST(put map) {
  auto xs = std::map<int, bool>{{1, true}, {2, false}, {3, false}};
  CHECK_EQUAL(put(builder, xs), 1u + 3 * 2);
  auto o = object{buf};
  REQUIRE(is_fixmap(o.format()));
  auto v = unbox(get<array_view>(o));
  CHECK_EQUAL(v.size(), 3u * 2);
  auto ys = v.data();
  auto first_key = unbox(get<uint8_t>(ys.get()));
  ys.next();
  auto first_value = unbox(get<bool>(ys.get()));
  CHECK_EQUAL(first_key, 1u);
  CHECK(first_value);
  ys.next(1 + 2);
  auto last_key = unbox(get<uint8_t>(ys.get()));
  ys.next();
  auto last_value = unbox(get<bool>(ys.get()));
  CHECK_EQUAL(last_key, 3u);
  CHECK(!last_value);
}

TEST(put variadic) {
  CHECK_EQUAL(put(builder, true, false, true), 3u);
  auto xs = overlay{buf};
  CHECK_EQUAL(xs.get().format(), true_);
  xs.next();
  CHECK_EQUAL(xs.get().format(), false_);
  xs.next();
  CHECK_EQUAL(xs.get().format(), true_);
}

FIXTURE_SCOPE_END()
