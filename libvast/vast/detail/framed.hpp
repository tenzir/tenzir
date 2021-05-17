//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"

#include <caf/typed_actor.hpp>

#include <atomic>

namespace vast::detail {

struct eof {
  // TODO: add some unique tag so downstreams can distinguish sources
};

struct flush {
  using callback_actor
    = caf::typed_actor<caf::reacts_to<atom::stream_done>>::pointer;

  struct shared_counter {
    std::atomic<size_t> count;
    callback_actor source;
  };

  // It would be nicer if we could just use a `std::shared_ptr` without the
  // separate `shared_counter`, but we'd need some kind of
  // `destruct_unless_unique()` function to make it work.
  std::shared_ptr<shared_counter> counter;

  // It would also be nicer if we could just rely on automatic reference
  // counting (increasing the count in a copy constructor and descreasing it in
  // the destructor) and signal the source from the destructor once we destroy
  // the last instance, but then this message would have to store its own
  // reference to the actor system.
  void multiplex(size_t n);

  // Decreases the shared count and returns `sink` if this is the last instance,
  // or nullptr otherwise.
  callback_actor terminate_one() &&;
};

template <typename T>
struct body {
  T content;
};

template <typename T>
using stream_frame = caf::variant<
  // Notifies downstreams that this source will not send any additional data.
  eof,
  // Notifies the original sender after all connected sinks have received all
  // data that was sent before the "flush" message.
  flush,
  // Regular data sent over the stream.
  body<T>>;

enum class stream_control_header : uint8_t {
  data,
  flush,
  eof,
};

/// Adds minimal framing around the template type `T` when sending it
/// through a caf stream. This enables the sender to insert an `eof`
/// message into the stream after all regular data has been sent, and
/// enables the receiver to trigger logic upon the receipt of an `eof`
/// which is otherwise not reliably possible in a stream stage.
//  TODO: Switch to the variant-based implementation above
template <typename T>
class framed {
public:
  framed() = default;

  /* implicit */ framed(T&& t)
    : header(stream_control_header::data), body(std::move(t)) {
  }

  static framed make_eof() {
    return framed{};
  }

  // --- data members -------------------------------------------

  enum stream_control_header header = stream_control_header::eof;

  // If required, this can be placed into a union to avoid the
  // requirement of a default constructor in the `eof` case.
  T body = {};

  // Only set if header == stream_control_header::call_me_back
  std::optional<call_me_back> callback;

  // --- concepts -----------------------------------------------

  template <typename Inspector>
  friend auto inspect(Inspector& f, framed<T>& sc) {
    return f(sc.header, sc.body);
  }
};

} // namespace vast::detail
