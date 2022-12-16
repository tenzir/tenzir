//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/inspection_common.hpp"

namespace vast::detail {

enum class stream_control_header : uint8_t {
  data,
  eof,
};

template <class Inspector>
auto inspect(Inspector& f, stream_control_header& x) {
  return detail::inspect_enum(f, x);
}

/// Adds minimal framing around the template type `T` when sending it
/// through a caf stream. This enables the sender to insert an `eof`
/// message into the stream after all regular data has been sent, and
/// enables the receiver to trigger logic upon the receipt of an `eof`
/// which is otherwise not reliably possible in a stream stage.
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

  // --- concepts -----------------------------------------------

  template <typename Inspector>
  friend auto inspect(Inspector& f, framed<T>& sc) {
    return detail::apply_all(f, sc.header, sc.body);
  }
};

} // namespace vast::detail