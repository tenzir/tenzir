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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#define SUITE parse_data 
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(data) {
  auto p = make_parser<data>();
  data d;

  MESSAGE("bool");
  auto str = "T"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == true);

  MESSAGE("numbers");
  str = "+1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001);
  str = "1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001u);
  str = "10.01"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 10.01);

  MESSAGE("string");
  str = "\"bar\""s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == "bar");

  MESSAGE("pattern");
  str = "/foo/"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == pattern{"foo"});

  MESSAGE("address");
  str = "10.0.0.1"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == *to<address>("10.0.0.1"));

  MESSAGE("port");
  str = "22/tcp"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == port{22, port::tcp});

  MESSAGE("vector");
  str = "[42,4.2,nil]"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == vector{42u, 4.2, nil});

  MESSAGE("set");
  str = "{-42,+42,-1}"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == set{-42, 42, -1});

  MESSAGE("table");
  str = "{T->1,F->0}"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == table{{true, 1u}, {false, 0u}});
}
