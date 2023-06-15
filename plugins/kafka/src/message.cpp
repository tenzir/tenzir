//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/message.hpp"

namespace vast::plugins::kafka {

auto message::payload() const -> std::span<const std::byte> {
  const auto* ptr = reinterpret_cast<const std::byte*>(message_->payload());
  return {ptr, message_->len()};
}

} // namespace vast::plugins::kafka
