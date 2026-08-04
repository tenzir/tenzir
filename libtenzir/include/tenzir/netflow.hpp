//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/box.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/option.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace tenzir {

class Serde;

namespace netflow {

enum class Version : uint16_t {
  v5 = 5,
  v9 = 9,
  ipfix = 10,
};

template <class Inspector>
auto inspect(Inspector& f, Version& x) -> bool {
  return tenzir::detail::inspect_enum(f, x);
}

enum class RecordKind : uint8_t {
  flow,
  options,
};

template <class Inspector>
auto inspect(Inspector& f, RecordKind& x) -> bool {
  return tenzir::detail::inspect_enum(f, x);
}

struct Peer {
  ip address;
  uint16_t port = 0;

  friend auto inspect(auto& f, Peer& x) -> bool {
    return f.object(x).fields(f.field("address", x.address),
                              f.field("port", x.port));
  }
};

struct V5Metadata {
  uint8_t engine_type = 0;
  uint8_t engine_id = 0;
  uint8_t sampling_mode = 0;
  uint16_t sampling_interval = 0;
};

struct Metadata {
  Version version = Version::v5;
  RecordKind record_kind = RecordKind::flow;
  time export_time = {};
  uint32_t sequence_number = 0;
  Option<uint32_t> observation_domain_id;
  Option<uint16_t> template_id;
  Option<duration> sys_uptime;
  Option<Peer> exporter;
  Option<V5Metadata> v5;
};

struct DecodedField {
  std::string name;
  data value;
};

struct DecodedRecord {
  Metadata metadata;
  std::vector<DecodedField> fields;
};

enum class DecodeErrorKind : uint8_t {
  unsupported_version,
  malformed,
};

struct DecodeError {
  DecodeErrorKind kind = DecodeErrorKind::malformed;
  uint16_t version = 0;
  std::string message;
};

struct DecodeResult {
  std::vector<DecodedRecord> records;
  Option<DecodeError> error;

  explicit operator bool() const {
    return error.is_none();
  }
};

enum class FrameStatus : uint8_t {
  ready,
  incomplete,
  ambiguous,
  error,
};

struct FrameResult {
  FrameStatus status = FrameStatus::incomplete;
  size_t size = 0;
  uint16_t version = 0;
  std::string message;
};

/// Tracks the idle grace period for a length-ambiguous stream frame.
class IdleFrameTimer {
public:
  using clock = std::chrono::steady_clock;
  using duration = clock::duration;
  using time_point = clock::time_point;

  explicit IdleFrameTimer(duration timeout) : timeout_{timeout} {
  }

  /// Clears the current grace period after non-empty input.
  auto on_input(size_t byte_count) -> void {
    if (byte_count > 0) {
      reset();
    }
  }

  /// Records an ambiguous prefix without extending an existing grace period.
  auto observe(size_t frame_size, time_point now = clock::now()) -> void {
    frame_size_ = frame_size;
    if (not deadline_) {
      deadline_ = now + timeout_;
    }
  }

  auto wait_for(time_point now = clock::now()) const -> Option<duration> {
    if (not deadline_) {
      return None{};
    }
    auto const remaining = *deadline_ - now;
    return remaining > duration::zero() ? remaining : duration::zero();
  }

  /// Returns and clears the candidate prefix when its grace period expired.
  auto take_expired(time_point now = clock::now()) -> Option<size_t> {
    if (not deadline_ or now < *deadline_) {
      return None{};
    }
    auto result = frame_size_;
    reset();
    return result;
  }

  auto reset() -> void {
    deadline_ = None{};
    frame_size_ = None{};
  }

private:
  duration timeout_;
  Option<time_point> deadline_;
  Option<size_t> frame_size_;
};

/// Stateful NetFlow v5, NetFlow v9, and IPFIX decoder.
class Decoder {
public:
  Decoder();

  /// Constructs a decoder with contextual time for unfolding 32-bit UNIX and
  /// NTP timestamps across era boundaries.
  explicit Decoder(time reference_time);
  Decoder(Decoder const&);
  Decoder(Decoder&&) noexcept;
  auto operator=(Decoder const&) -> Decoder&;
  auto operator=(Decoder&&) noexcept -> Decoder&;
  ~Decoder();

  /// Determines the size of the first message in an unframed byte stream.
  /// After an incomplete result, the next call must extend the same prefix.
  auto frame(std::span<const std::byte> bytes, bool end_of_input)
    -> FrameResult;

  auto decode_message(std::span<const std::byte> bytes, Option<Peer> peer,
                      diagnostic_handler& dh) -> DecodeResult;

  /// Drops buffered data sets and emits one warning for each one.
  auto finish(diagnostic_handler& dh) -> void;

  /// Snapshots templates and bounded buffered message groups. Restored groups
  /// retain generation expiry but begin a fresh wall-clock TTL.
  auto snapshot(Serde& serde) -> void;

private:
  struct State;
  Box<State> state_;
};

} // namespace netflow

} // namespace tenzir
