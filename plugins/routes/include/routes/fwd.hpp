//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include <tenzir/fwd.hpp>

namespace tenzir::plugins::routes {

struct config;
struct named_input_actor;
struct named_output_actor;

} // namespace tenzir::plugins::routes

CAF_BEGIN_TYPE_ID_BLOCK(routes_types, 21009)
  CAF_ADD_TYPE_ID(routes_types, (tenzir::plugins::routes::config))
  CAF_ADD_TYPE_ID(routes_types, (tenzir::plugins::routes::named_input_actor))
  CAF_ADD_TYPE_ID(routes_types, (tenzir::plugins::routes::named_output_actor))
CAF_END_TYPE_ID_BLOCK(routes_types)
