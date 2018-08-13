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

#define SUITE counting_stream_buffer

#include "test.hpp"

#include "vast/detail/counting_stream_buffer.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(reading and writing) {
  std::stringbuf ss{"foobarbaz"};
  detail::counting_stream_buffer sb{ss};
  char buf[128];
  MESSAGE("get area");
  sb.sgetn(buf, 2);
  sb.sgetn(buf, 4);
  CHECK_EQUAL(sb.got(), 2u + 4u);
  MESSAGE("put area");
  sb.sputn(buf, 3);
  sb.sputn(buf, 2);
  CHECK_EQUAL(sb.put(), 3u + 2u);
}
