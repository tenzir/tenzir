//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/span.hpp>

#include <broker/zeek.hh>
#include <caf/error.hpp>

#include <cstddef>

namespace vast::plugins::broker {

/// Decodes binary data created by Zeek's internal serialization framework.
// TODO: This function will get another "consumer" sink argument in the future
// after the printf decoding implementation is complete.
caf::error process(const ::broker::zeek::LogWrite& msg);

} // namespace vast::plugins::broker
