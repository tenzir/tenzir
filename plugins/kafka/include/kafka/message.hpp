//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstddef>
#include <memory>
#include <rdkafkacpp.h>
#include <span>

namespace vast::plugins::kafka {

/// A message from a consumer.
class message {
  friend class consumer;

public:
  auto payload() const -> std::span<const std::byte>;

private:
  std::shared_ptr<RdKafka::Message> message_{};
};

} // namespace vast::plugins::kafka
