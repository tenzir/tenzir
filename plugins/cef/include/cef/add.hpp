//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/fwd.hpp>
#include <vast/view.hpp>

#include <caf/error.hpp>

#include <string>

namespace vast::plugins::cef {

struct message;

/// Parses a line of ASCII as CEF message.
/// @param msg The CEF message.
/// @param builder The table slice builder to add the message to.
caf::error add(const message& msg, table_slice_builder& builder);

} // namespace vast::plugins::cef
