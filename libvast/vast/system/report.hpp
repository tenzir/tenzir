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

#include "vast/fwd.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/time.hpp"

#include <caf/fwd.hpp>
#include <caf/variant.hpp>

#include <cstdint>
#include <string>

namespace vast::system {

struct data_point {
  std::string key;
  caf::variant<std::string, duration, time, int64_t, uint64_t, double> value;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, data_point& s) {
  return f(caf::meta::type_name("data_point"), s.key, s.value);
}

using report = std::vector<data_point>;

struct performance_sample {
  std::string key;
  measurement value;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, performance_sample& s) {
  return f(caf::meta::type_name("performance_sample"), s.key, s.value);
}

using performance_report = std::vector<performance_sample>;

} // namespace vast::system
