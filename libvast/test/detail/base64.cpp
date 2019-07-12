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

#define SUITE base64

#include "vast/test/test.hpp"

#include "vast/detail/base64.hpp"

using namespace std::string_view_literals;
using namespace vast::detail;

// Ground truth:
//
//   printf "The quick brown fox jumps over the lazy dog" | base64
//   VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==

TEST(encode) {
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  auto enc = "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="sv;
  CHECK_EQUAL(base64::encode(dec), enc);
}

TEST(decode) {
  auto enc = "VGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw=="sv;
  auto dec = "The quick brown fox jumps over the lazy dog"sv;
  CHECK_EQUAL(base64::decode(enc), dec);
}
