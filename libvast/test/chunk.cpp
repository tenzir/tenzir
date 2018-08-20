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

#define SUITE chunk

#include "vast/chunk.hpp"

#include "test.hpp"

#include "vast/load.hpp"
#include "vast/save.hpp"

using namespace vast;

TEST(construction) {
  auto x = chunk::make(100);
  CHECK_EQUAL(x->size(), 100u);
}

TEST(slicing) {
  char buf[100];
  auto i = 42;
  auto deleter = [&](char*, size_t) { i = 0; };
  auto x = chunk::make(sizeof(buf), buf, deleter);
  auto y = x->slice(50);
  auto z = y->slice(40, 5);
  CHECK_EQUAL(y->size(), 50u);
  CHECK_EQUAL(z->size(), 5u);
  x = y = nullptr;
  CHECK_EQUAL(z->size(), 5u);
  CHECK_EQUAL(i, 42);
  z = nullptr;
  CHECK_EQUAL(i, 0);
}

TEST(serialization) {
  char str[] = "foobarbaz";
  auto x = chunk::make(sizeof(str));
  std::memcpy(x->data(), str, sizeof(str));
  std::vector<char> buf;
  CHECK(save(buf, x));
  chunk_ptr y;
  CHECK(load(buf, y));
  REQUIRE(y);
  CHECK(std::equal(x->begin(), x->end(), y->begin(), y->end()));
}
