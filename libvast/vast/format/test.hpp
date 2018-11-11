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

#include <random>
#include <unordered_map>

#include <caf/variant.hpp>

#include "vast/data.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/format/reader.hpp"
#include "vast/fwd.hpp"
#include "vast/schema.hpp"

#include "vast/detail/random.hpp"

namespace vast::format::test {

// A type-erased probability distribution.
using distribution =
  caf::variant<
    std::uniform_int_distribution<integer>,
    std::uniform_int_distribution<count>,
    std::uniform_real_distribution<long double>,
    std::normal_distribution<long double>,
    detail::pareto_distribution<long double>
  >;

// 64-bit linear congruential generator with MMIX/Knuth parameterization.
using lcg64 =
  std::linear_congruential_engine<
    uint64_t,
    6364136223846793005ull,
    1442695040888963407ull,
    std::numeric_limits<uint64_t>::max()
  >;

//using lcg = std::minstd_rand;
using lcg = lcg64;

// An event data template to be filled with randomness.
struct blueprint {
  vast::data data;
  std::vector<distribution> distributions;
};

/// Produces random events according to a given schema.
class reader : public format::reader {
public:
  /// Constructs a test reader.
  /// @param seed A seed for the random number generator.
  /// @param n The numer of events to generate.
  /// @param id The base event ID to start at.
  /// @param sch The event schema.
  reader(size_t seed, uint64_t n, vast::schema sch);

  /// Constructs a test reader.
  /// @param seed A seed for the random number generator.
  /// @param n The numer of events to generate.
  /// @param id The base event ID to start at.
  explicit reader(size_t seed = 0, uint64_t n = 100);

  expected<event> read() override;

  caf::error read(table_slice_builder& builder, size_t num);

  expected<void> schema(vast::schema sch) override;

  expected<vast::schema> schema() const override;

  const char* name() const override;

private:
  vast::schema schema_;
  std::mt19937_64 generator_;
  uint64_t num_events_;
  schema::const_iterator next_;
  std::unordered_map<type, blueprint> blueprints_;
};

} // namespace vast::format::test
