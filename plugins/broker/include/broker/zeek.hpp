//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/span.hpp>
#include <vast/type.hpp>

#include <broker/zeek.hh>
#include <caf/error.hpp>

#include <cstddef>

namespace vast::plugins::broker {

/// Handles a Zeek *log create* message. This message opens a log stream and
/// conveys the type information needed to correctly interpret subsequent log
/// writes.
/// @param msg The log create message to process.
/// @returns The VAST type corresponding to the meta data in the message.
caf::expected<record_type> process(const ::broker::zeek::LogCreate& msg);

/// Handle a Zeek *log write* message. This message contains the data portion
/// corresponding to a previous log create message. The message data is
/// serialized using Zeek's custom binary wire format. This is a rather
/// complex format, but fortunately log writes include only *threading vals*,
/// which represent a manageable subset of all possible Zeek types.
/// @param msg The log write message to process.
/// @returns An error on failure.
/// @note This function implements the binary deserialization as result of
/// reverse engineering the Zeek source code (`SerializationFormat::Read` and
/// `threading::Value::Read`)
// TODO: This function will get another "consumer" sink argument in the future
// after the printf decoding implementation is complete.
caf::error process(const ::broker::zeek::LogWrite& msg);

} // namespace vast::plugins::broker
