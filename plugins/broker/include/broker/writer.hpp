//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/format/writer.hpp>

#include <broker/endpoint.hh>
#include <broker/status_subscriber.hh>
#include <broker/subscriber.hh>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

namespace vast::plugins::broker {

/// A writer for Zeek Broker.
class writer : public format::writer {
public:
  /// Constructs a Broker writer.
  /// @param options The configuration options for the writer.
  /// @param sys The actor system of the containing actor.
  explicit writer(const caf::settings& options, caf::actor_system& sys);

  ~writer() override = default;

  using format::writer::write;

  caf::error write(const table_slice& slice) override;

  caf::expected<void> flush() override;

  [[nodiscard]] const char* name() const override;

private:
  std::unique_ptr<::broker::endpoint> endpoint_;
  std::unique_ptr<::broker::status_subscriber> status_subscriber_;
  std::unique_ptr<::broker::subscriber> subscriber_;
  std::string topic_;
  std::string event_name_;
};

} // namespace vast::plugins::broker
