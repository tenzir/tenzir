/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/fwd.hpp"
#include "vast/system/report.hpp"
#include "vast/table_slice.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <chrono>
#include <cstddef>
#include <string_view>

namespace vast::format {

/// The base class for readers.
class reader {
public:
  // -- member types -----------------------------------------------------------

  using reader_clock = std::chrono::steady_clock;

  enum class inputs { file, inet };

  /// Default input method for the reader
  struct defaults {
    constexpr static inputs input = inputs::file;
    constexpr static auto path = vast::defaults::import::read;
  };

  /// A function object for consuming parsed table slices.
  class consumer {
  public:
    virtual ~consumer();

    virtual void operator()(table_slice) = 0;
  };

  // -- constructors, destructors, and assignment operators --------------------

  /// @param id Implementation ID for the table slice builder.
  reader(const caf::settings& options);

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
  std::pair<caf::error, size_t> read(size_t max_events, size_t max_slice_size,
                                     F f) {
    VAST_ASSERT(max_events > 0);
    VAST_ASSERT(max_slice_size > 0);
    struct consumer_impl : consumer {
      void operator()(table_slice x) override {
        produced += x.rows();
        f_(std::move(x));
      }
      consumer_impl(F& fun) : f_(fun), produced(0) {
        // nop
      }
      F& f_;
      size_t produced;
    };
    consumer_impl g{f};
    if (auto err = read_impl(max_events, max_slice_size, g))
      return {err, g.produced};
    return {caf::none, g.produced};
  }

  // -- properties -------------------------------------------------------------

  /// Tries to set the schema for events to read.
  /// @param x The new schema.
  /// @returns `caf::none` on success.
  virtual caf::error schema(vast::schema x) = 0;

  /// Retrieves the currently used schema.
  /// @returns The current schema.
  virtual vast::schema schema() const = 0;

  /// @returns The name of the reader type.
  virtual const char* name() const = 0;

  /// @returns A report for the accountant.
  virtual vast::system::report status() const;

protected:
  virtual caf::error read_impl(size_t max_events, size_t max_slice_size,
                               consumer& f) = 0;

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
