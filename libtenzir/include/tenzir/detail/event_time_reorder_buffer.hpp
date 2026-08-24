//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/saturating_arithmetic.hpp"
#include "tenzir/option.hpp"
#include "tenzir/time.hpp"

#include <algorithm>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <utility>
#include <vector>

namespace tenzir::detail {

/// Incrementally orders payloads by event time within a bounded tolerance.
///
/// Equal timestamps retain insertion order. An event is late only when its
/// timestamp precedes the timestamp of the last emitted event.
template <class Payload>
class EventTimeReorderBuffer {
public:
  enum class InsertResult {
    accepted,
    late,
  };

  struct Event {
    time timestamp;
    uint64_t sequence = 0;
    Payload payload;

    friend auto inspect(auto& f, Event& x) -> bool {
      return f.object(x).fields(f.field("timestamp", x.timestamp),
                                f.field("sequence", x.sequence),
                                f.field("payload", x.payload));
    }
  };

  explicit EventTimeReorderBuffer(duration tolerance) : tolerance_{tolerance} {
    TENZIR_ASSERT(tolerance_ >= duration::zero());
  }

  auto insert(time timestamp, Payload payload) -> InsertResult {
    if (is_late(timestamp)) {
      return InsertResult::late;
    }
    if (next_sequence_ == std::numeric_limits<uint64_t>::max()) {
      resequence();
    }
    largest_observed_time_ = largest_observed_time_
                               ? std::max(*largest_observed_time_, timestamp)
                               : timestamp;
    auto event = Event{timestamp, next_sequence_++, std::move(payload)};
    auto inserted
      = events_.emplace(Key{event.timestamp, event.sequence}, std::move(event))
          .second;
    TENZIR_ASSERT(inserted);
    return InsertResult::accepted;
  }

  auto drain() -> std::vector<Event> {
    auto result = std::vector<Event>{};
    auto current_watermark = watermark();
    if (not current_watermark) {
      return result;
    }
    auto end = events_.upper_bound(
      Key{*current_watermark, std::numeric_limits<uint64_t>::max()});
    result.reserve(std::distance(events_.begin(), end));
    while (events_.begin() != end) {
      auto entry = events_.extract(events_.begin());
      last_emitted_timestamp_ = entry.mapped().timestamp;
      result.push_back(std::move(entry.mapped()));
    }
    return result;
  }

  auto flush() -> std::vector<Event> {
    auto result = std::vector<Event>{};
    result.reserve(events_.size());
    while (not events_.empty()) {
      auto entry = events_.extract(events_.begin());
      last_emitted_timestamp_ = entry.mapped().timestamp;
      result.push_back(std::move(entry.mapped()));
    }
    return result;
  }

  auto is_late(time timestamp) const -> bool {
    return last_emitted_timestamp_ and timestamp < *last_emitted_timestamp_;
  }

  auto watermark() const -> Option<time> {
    return largest_observed_time_.map([&](time timestamp) {
      return detail::saturating_sub(timestamp, tolerance_);
    });
  }

  auto largest_observed_time() const -> Option<time> {
    return largest_observed_time_;
  }

  auto last_emitted_timestamp() const -> Option<time> {
    return last_emitted_timestamp_;
  }

  auto size() const -> size_t {
    return events_.size();
  }

  template <class Serde>
  auto snapshot(Serde& serde) -> void {
    serde("events", events_);
    serde("largest_observed_time", largest_observed_time_);
    serde("last_emitted_timestamp", last_emitted_timestamp_);
    serde("next_sequence", next_sequence_);
  }

private:
  struct Key {
    time timestamp;
    uint64_t sequence = 0;

    friend auto operator<=>(Key const&, Key const&) = default;

    friend auto inspect(auto& f, Key& x) -> bool {
      return f.object(x).fields(f.field("timestamp", x.timestamp),
                                f.field("sequence", x.sequence));
    }
  };

  auto resequence() -> void {
    auto replacement = std::map<Key, Event>{};
    auto sequence = uint64_t{0};
    while (not events_.empty()) {
      auto entry = events_.extract(events_.begin());
      auto event = std::move(entry.mapped());
      event.sequence = sequence++;
      auto inserted
        = replacement
            .emplace(Key{event.timestamp, event.sequence}, std::move(event))
            .second;
      TENZIR_ASSERT(inserted);
    }
    events_ = std::move(replacement);
    next_sequence_ = sequence;
  }

  duration tolerance_;
  std::map<Key, Event> events_;
  Option<time> largest_observed_time_;
  Option<time> last_emitted_timestamp_;
  uint64_t next_sequence_ = 0;
};

} // namespace tenzir::detail
