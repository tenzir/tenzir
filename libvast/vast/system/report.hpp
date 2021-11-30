//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/instrumentation.hpp"
#include "vast/time.hpp"

#include <caf/fwd.hpp>
#include <caf/variant.hpp>

#include <cstdint>
#include <string>

namespace vast::system {

/// A set of tags to attach to a metrics event.
// This is also declared in `actors.hpp`
using metrics_metadata = std::vector<std::pair<std::string, std::string>>;

struct data_point {
  std::string key;
  caf::variant<duration, time, int64_t, uint64_t, double> value;

  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f, data_point& s) {
    return f(caf::meta::type_name("data_point"), s.key, s.value);
  }
};

struct report {
  std::vector<data_point> data = {};
  metrics_metadata metadata = {};

  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f, report& x) {
    return f(caf::meta::type_name("report"), x.data, x.metadata);
  }
};

struct performance_sample {
  std::string key;
  measurement value;

  template <class Inspector>
  friend typename Inspector::result_type
  inspect(Inspector& f, performance_sample& s) {
    return f(caf::meta::type_name("performance_sample"), s.key, s.value);
  }
};

struct performance_report {
  std::vector<performance_sample> data = {};
  metrics_metadata metadata = {};

  template <class Inspector>
  friend typename Inspector::result_type
  inspect(Inspector& f, performance_report& x) {
    return f(caf::meta::type_name("performance_report"), x.data, x.metadata);
  }
};

} // namespace vast::system
