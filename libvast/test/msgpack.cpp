
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

#include "vast/detail/msgpack.hpp"

#define SUITE msgpack
#include "test.hpp"

using namespace vast::detail;

namespace {

struct fixture {
  fixture() : buf(1024), builder{msgpack::builder{buf}} {
    // nop
  }

  std::vector<byte> buf;
  msgpack::builder<msgpack::bounds_check> builder;
};

} // namespace <anonymous>

FIXTURE_SCOPE(msgpack_tests, fixture)

TEST(nil) {
  CHECK_EQUAL(builder.add<msgpack::nil>(), 1u);
  CHECK_EQUAL(buf[0], static_cast<byte>(msgpack::nil));
}

TEST(boolean) {
  CHECK_EQUAL(builder.add<msgpack::true_>(), 1u);
  CHECK_EQUAL(builder.add<msgpack::false_>(), 1u);
  CHECK_EQUAL(buf[0], static_cast<byte>(msgpack::true_));
  CHECK_EQUAL(buf[1], static_cast<byte>(msgpack::false_));
  CHECK_EQUAL(builder.size(), 2u);
}

TEST(positive fixint) {
  CHECK_EQUAL(builder.add<msgpack::positive_fixint>(-1), 0u);
  CHECK_EQUAL(builder.add<msgpack::positive_fixint>(0), 1u);
  CHECK_EQUAL(builder.add<msgpack::positive_fixint>(42), 1u);
  CHECK_EQUAL(buf[1], static_cast<byte>(42 & msgpack::positive_fixint));
  CHECK_EQUAL(builder.add<msgpack::positive_fixint>(128), 0u);
  CHECK_EQUAL(builder.size(), 2u);
}

TEST(negative fixint) {
  CHECK_EQUAL(builder.add<msgpack::negative_fixint>(-33), 0u);
  CHECK_EQUAL(builder.add<msgpack::negative_fixint>(-30), 1u);
  CHECK_EQUAL(buf[0], static_cast<byte>(-30 & msgpack::negative_fixint));
  CHECK_EQUAL(builder.add<msgpack::negative_fixint>(0), 0u);
  CHECK_EQUAL(builder.add<msgpack::negative_fixint>(42), 0u);
}

TEST(uint) {
  CHECK_EQUAL(builder.add<msgpack::uint8>(uint8_t{0x11}), 2u);
  CHECK_EQUAL(builder.add<msgpack::uint16>(uint16_t{0x1122}), 3u);
  CHECK_EQUAL(builder.add<msgpack::uint32>(uint32_t{0x11223344}), 5u);
  CHECK_EQUAL(builder.add<msgpack::uint64>(uint64_t{0x1122334455667788}), 9u);
}

TEST(int) {
  CHECK_EQUAL(builder.add<msgpack::int8>(int8_t{0x11}), 2u);
  CHECK_EQUAL(builder.add<msgpack::int16>(int16_t{0x1122}), 3u);
  CHECK_EQUAL(builder.add<msgpack::int32>(int32_t{0x11223344}), 5u);
  CHECK_EQUAL(builder.add<msgpack::int64>(int64_t{0x1122334455667788}), 9u);
}

TEST(float) {
  CHECK_EQUAL(builder.add<msgpack::float32>(4.2f), 5u);
  CHECK_EQUAL(builder.add<msgpack::float64>(4.2), 9u);
}

TEST(fixstr) {
  CHECK_EQUAL(builder.add<msgpack::fixstr>(""), 1u);
  CHECK_EQUAL(builder.add<msgpack::fixstr>("foo"), 1u + 3);
  auto str = std::string(32, 'x');
  CHECK_EQUAL(builder.add<msgpack::fixstr>(str), 0u);
}

TEST(str8) {
  CHECK_EQUAL(builder.add<msgpack::str8>(""), 1u + 1);
  CHECK_EQUAL(builder.add<msgpack::str8>("foo"), 1u + 1 + 3);
  auto str = std::string(255, 'x');
  CHECK_EQUAL(builder.add<msgpack::str8>(str), 1u + 1 + str.size());
  str += 'x';
  CHECK_EQUAL(builder.add<msgpack::str8>(str), 0u);
}

TEST(str16) {
  CHECK_EQUAL(builder.add<msgpack::str16>(""), 1u + 2);
  CHECK_EQUAL(builder.add<msgpack::str16>("foo"), 1u + 2 + 3);
  auto str = std::string(1000, 'x');
  CHECK_EQUAL(builder.add<msgpack::str16>(str), 1u + 2 + str.size());
  str.resize(buf.size());
  CHECK_EQUAL(builder.add<msgpack::str16>(str), 0u);
}

TEST(str32) {
  CHECK_EQUAL(builder.add<msgpack::str32>(""), 1u + 4);
  CHECK_EQUAL(builder.add<msgpack::str32>("foo"), 1u + 4 + 3);
}

TEST(run-time builder) {
  builder.add(msgpack::positive_fixint, 42);
  builder.add(msgpack::negative_fixint, -7);
  builder.add(msgpack::nil);
  builder.add(msgpack::false_);
  builder.add(msgpack::true_);
  CHECK_EQUAL(builder.size(), 5u);
}

FIXTURE_SCOPE_END()
