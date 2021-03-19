//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/report.hpp"
#include "vast/system/type_registry.hpp"

namespace vast::system {

/// Initializes the state.
template <class Source>
void init(Source* self, type_registry_actor type_registry,
          std::string type_filter) {
  // Figure out which schemas we need.
  auto& st = self->state;
  if (type_registry) {
    self->request(type_registry, caf::infinite, atom::get_v)
      .await([=](type_set types) {
        auto& st = self->state;
        auto is_valid = [&](const auto& layout) {
          return detail::starts_with(layout.name(), type_filter);
        };
        // First, merge and de-duplicate the local schema with types from the
        // type-registry.
        auto merged_schema = schema{};
        for (auto& type : st.local_schema)
          if (auto&& layout = caf::get_if<vast::record_type>(&type))
            if (is_valid(*layout))
              merged_schema.add(std::move(*layout));
        // Second, filter valid types from all available record types.
        for (auto& type : types)
          if (auto&& layout = caf::get_if<vast::record_type>(&type))
            if (is_valid(*layout))
              merged_schema.add(*layout);
        // Third, try to set the new schema.
        if (auto err = st.reader->schema(std::move(merged_schema));
            err && err != caf::no_error)
          VAST_ERROR("{} failed to set schema {}", self, err);
      });
  } else {
    // We usually expect to have the type registry at the ready, but if we
    // don't we fall back to only using the schemas from disk.
    VAST_WARN("{} failed to retrieve registered types and only "
              "considers types local to the import command",
              self);
    if (auto err = st.reader->schema(std::move(st.local_schema));
        err && err != caf::no_error)
      VAST_ERROR("{} failed to set schema {}", self, err);
  }
}

template <class Source>
void send_report(Source* self) {
  auto& st = self->state;
  // Send the reader-specific status report to the accountant.
  if (auto status = st.reader->status(); !status.empty())
    self->send(st.accountant, std::move(status));
  // Send the source-specific performance metrics to the accountant.
  if (st.metrics.events > 0) {
    auto r = performance_report{{{std::string{st.name}, st.metrics}}};
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
    for (const auto& [key, m] : r) {
      if (auto rate = m.rate_per_sec(); std::isfinite(rate))
        VAST_INFO("{} produced {} events at a rate of {} events/sec "
                  "in {}",
                  self, m.events, static_cast<uint64_t>(rate),
                  to_string(m.duration));
      else
        VAST_INFO("{} produced {} events in {}", self, m.events,
                  to_string(m.duration));
    }
#endif
    st.metrics = measurement{};
    self->send(st.accountant, std::move(r));
  }
}

} // namespace vast::system
