//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/type.hpp>
#include <vast/view.hpp>

#include <broker/zeek.hh>
#include <caf/error.hpp>

#include <cstddef>
#include <span>

namespace vast::plugins::broker {

/// Constructs a Broker endpoint from command line options.
/// @param options The command line options.
/// @param category The parent category for *options*.
/// @returns A Broker endpoint.
std::unique_ptr<::broker::endpoint>
make_endpoint(const caf::settings& options, std::string_view category);

/// Attaches a Broker subscriber to an endpoint.
/// @param endpoint The endpoint to attach a subscriber to.
/// @param topics The list of topics to subscribe to.
/// @returns A subscriber for *topics*.
std::unique_ptr<::broker::subscriber>
make_subscriber(::broker::endpoint& endpoint, std::vector<std::string> topics);

/// Handles a Zeek *log create* message. This message opens a log stream and
/// conveys the type information needed to correctly interpret subsequent log
/// writes.
/// @param msg The log create message to process.
/// @returns The VAST type corresponding to the meta data in the message.
caf::expected<type> process(const ::broker::zeek::LogCreate& msg);

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
caf::expected<std::vector<data>> process(const ::broker::zeek::LogWrite& msg);

} // namespace vast::plugins::broker
