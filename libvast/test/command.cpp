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

#include "vast/command.hpp"

#include "test.hpp"

using namespace vast;

TEST(command) {
  command cmd;
  cmd
    .opt("example,e", "a full option with value", "x")
    .opt("flag,f", "print version and exit")
    .opt("long", "a boolean long flag")
    .callback([](const command& cmd, std::vector<std::string> args) {
      // TODO
    });
  // TODO
  //cmd.dispatch();
}
