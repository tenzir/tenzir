//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

#include <compare>
#include <string>
#include <string_view>

namespace tenzir {

struct PipeId;
struct ChannelId;

struct OpId {
  std::string value;

  auto sub(size_t index) const -> PipeId;

  auto to(OpId other, size_t from_port = 0) const -> ChannelId;

  friend auto format_as(OpId const& self) -> std::string_view {
    return self.value;
  }

  auto operator<=>(OpId const& other) const = default;
};

struct PipeId {
  std::string value;

  auto op(size_t index) const -> OpId {
    return OpId{fmt::format("{}/{}", value, index)};
  }

  /// Qualify a plan-relative operator ID with this runtime pipeline ID.
  auto op(OpId relative) const -> OpId {
    return OpId{fmt::format("{}/{}", value, relative.value)};
  }

  friend auto format_as(PipeId const& self) -> std::string_view {
    return self.value;
  }

  auto operator<=>(PipeId const& other) const = default;
};

struct ChannelId {
  // Keep the endpoint label independent of the port: profilers use it to
  // attribute every output port's counters to the same operator.
  std::string value;
  size_t from_port = 0;

  static auto first(OpId id) -> ChannelId {
    return ChannelId{fmt::format("_ -> {}", id.value)};
  }

  static auto last(OpId id, size_t from_port = 0) -> ChannelId {
    return ChannelId{fmt::format("{} -> _", id.value), from_port};
  }

  friend auto format_as(ChannelId const& self) -> std::string_view {
    return self.value;
  }

  auto operator<=>(ChannelId const& other) const = default;
};

inline auto OpId::sub(size_t index) const -> PipeId {
  return PipeId{fmt::format("{}-{}", value, index)};
}

inline auto OpId::to(OpId other, size_t from_port) const -> ChannelId {
  return ChannelId{fmt::format("{} -> {}", value, other.value), from_port};
}

} // namespace tenzir

template <>
struct std::hash<tenzir::OpId> {
  auto operator()(tenzir::OpId const& id) const -> size_t {
    return std::hash<std::string>{}(id.value);
  }
};
