//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/system/report.hpp"
#include "vast/table_slice.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <chrono>
#include <cstddef>
#include <string_view>

namespace vast::format {

/// @relates reader
using reader_ptr = std::unique_ptr<reader>;

/// The base class for readers.
class reader {
public:
  // -- member types -----------------------------------------------------------

  struct defaults {
    const char* category;
    const char* input = "-";
  };

  using reader_clock = std::chrono::steady_clock;

  /// A function object for consuming parsed table slices.
  class consumer {
  public:
    virtual ~consumer();

    virtual void operator()(table_slice) = 0;

    virtual std::optional<table_slice> on_before_consuming(table_slice slice) = 0;
  };

  // -- constructors, destructors, and assignment operators --------------------

  /// Produces a reader for the specified format.
  /// @param input_format The output format.
  /// @param options Config options for the concrete reader.
  /// @returns An owning pointer to the reader or an error.
  static caf::expected<std::unique_ptr<format::reader>>
  make(std::string input_format, const caf::settings& options);

  /// @param id Implementation ID for the table slice builder.
  reader(const caf::settings& options);

  virtual void reset(std::unique_ptr<std::istream> in) = 0;

  virtual ~reader();

  // -- properties -------------------------------------------------------------

  /// Reads up to `max_events` events and calls the consumer `f` for each
  /// produced slice, where each slice has up to `max_slice_size` events.
  /// @returns the number of parsed events on success, `0` if the
  ///          underlying format has currently no event (e.g., when it's
  ///          idling), and an error otherwise.
  /// @pre max_events > 0
  /// @pre max_slice_size > 0
  template <class F>
  std::pair<caf::error, size_t> read(
    size_t max_events, size_t max_slice_size, F f,
    std::function<std::optional<table_slice>(table_slice)> on_before_fun
    = [](auto slice) {
        return slice;
      }) {
    VAST_ASSERT(max_events > 0);
    VAST_ASSERT(max_slice_size > 0);
    struct consumer_impl : consumer {
      void operator()(table_slice x) override {
        produced += x.rows();
        f_(std::move(x));
      }
      std::optional<table_slice> on_before_consuming(table_slice slice) override {
        return on_before_consuming_(slice);
      }
      consumer_impl(F& fun,  std::function<std::optional<table_slice>(table_slice)>& on_before_fun)
        : f_(fun), on_before_consuming_(on_before_fun), produced(0) {
        // nop
      }
      F& f_;
       std::function<std::optional<table_slice>(table_slice)>& on_before_consuming_;
      size_t produced;
    };
    consumer_impl g{f, on_before_fun};
    if (auto err = read_impl(max_events, max_slice_size, g))
      return {err, g.produced};
    return {caf::none, g.produced};
  }

  // -- properties -------------------------------------------------------------

  /// Tries to set the module for events to read.
  /// @param x The new module.
  /// @returns `caf::none` on success.
  virtual caf::error module(vast::module x) = 0;

  /// Retrieves the currently used module.
  /// @returns The current module.
  [[nodiscard]] virtual vast::module module() const = 0;

  /// @returns The name of the reader type.
  [[nodiscard]] virtual const char* name() const = 0;

  /// @returns A report for the accountant.
  [[nodiscard]] virtual vast::system::report status() const;

protected:
  virtual caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f)
    = 0;

public:
  table_slice_encoding table_slice_type_
    = vast::defaults::import::table_slice_type;
  reader_clock::duration batch_timeout_ = vast::defaults::import::batch_timeout;
  reader_clock::duration read_timeout_ = vast::defaults::import::read_timeout;

protected:
  size_t batch_events_ = 0;
  reader_clock::time_point last_batch_sent_;
};

} // namespace vast::format
