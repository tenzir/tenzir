//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/instrumentation.hpp"
#include "vast/time.hpp"

#include <caf/fwd.hpp>
#include <caf/variant.hpp>

#include <cstdint>
#include <string>

namespace vast {

/// A set of tags to attach to a metrics event.
struct metrics_metadata : std::vector<std::pair<std::string, std::string>> {
  using super = std::vector<std::pair<std::string, std::string>>;
  using super::super;
};

struct data_point {
  std::string key;
  caf::variant<duration, time, int64_t, uint64_t, double> value;
  metrics_metadata metadata = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, data_point& s) {
    return f.object(s)
      .pretty_name("data_point")
      .fields(f.field("key", s.key), f.field("value", s.value),
              f.field("metadata", s.metadata));
  }
};

struct report {
  std::vector<data_point> data = {};
  metrics_metadata metadata = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, report& x) {
    return f.object(x).pretty_name("report").fields(
      f.field("data", x.data), f.field("metadata", x.metadata));
  }
};

struct performance_sample {
  std::string key;
  measurement value;
  metrics_metadata metadata = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, performance_sample& s) {
    return f.object(s)
      .pretty_name("performance_sample")
      .fields(f.field("key", s.key), f.field("value", s.value),
              f.field("metadata", s.metadata));
  }
};

struct performance_report {
  std::vector<performance_sample> data = {};
  metrics_metadata metadata = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, performance_report& x) {
    return f.object(x)
      .pretty_name("performance_report")
      .fields(f.field("data", x.data), f.field("metadata", x.metadata));
  }
};

} // namespace vast
