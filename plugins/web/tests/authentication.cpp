//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/authenticator.hpp"

#include <tenzir/test/test.hpp>

TEST("token validation") {
  tenzir::plugins::web::authenticator_state state;
  auto token = state.generate();
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(state.authenticate(*token), true);
  CHECK_EQUAL(state.authenticate("Shub-Niggurath"), false);
  auto serialized_state = state.save();
  REQUIRE_NOERROR(serialized_state);
  tenzir::plugins::web::authenticator_state recovered_state;
  CHECK_EQUAL(recovered_state.initialize_from(*serialized_state), caf::error{});
  CHECK_EQUAL(state.authenticate(*token), true);
  CHECK_EQUAL(state.authenticate("Yog-Sothoth"), false);
}
