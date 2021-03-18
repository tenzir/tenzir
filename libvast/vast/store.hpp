// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <caf/expected.hpp>

namespace vast {

/// A key-value store for events.
class store {
public:
  /// A session type for managing the state of a lookup.
  struct lookup {
    virtual ~lookup();

    /// Obtains the next slice containing events pertaining
    /// this lookup session.
    /// @returns caf::no_error when finished.
    /// @returns A new table slice upon every invocation.
    virtual caf::expected<table_slice> next() = 0;
  };

  virtual ~store();

  /// Adds a table slice to the store.
  /// @param xs The table slice to add.
  /// @returns No error on success.
  virtual caf::error put(table_slice xs) = 0;

  /// Starts an iterative extraction session.
  /// @param xs The IDs for the events to retrieve.
  /// @returns A pointer to lookup session.
  /// @relates lookup
  virtual std::unique_ptr<lookup> extract(const ids& xs) const = 0;

  /// Erases events from the store.
  /// @param xs The set of IDs to erase.
  /// @returns No error on success.
  virtual caf::error erase(const ids& xs) = 0;

  /// Retrieves a set of events.
  /// @param xs The IDs for the events to retrieve.
  /// @returns The table slice according to *xs*.
  virtual caf::expected<std::vector<table_slice>> get(const ids& xs) = 0;

  /// Flushes in-memory state to persistent storage.
  /// @returns No error on success.
  virtual caf::error flush() = 0;

  /// Fills `xs` with implementation-specific status information.
  virtual void inspect_status(caf::settings& xs, system::status_verbosity v)
    = 0;
};

} // namespace vast
