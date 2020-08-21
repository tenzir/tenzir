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

#include "vast/data.hpp"
#include "vast/detail/random.hpp"
#include "vast/format/multi_layout_reader.hpp"
#include "vast/format/reader.hpp"
#include "vast/fwd.hpp"
#include "vast/schema.hpp"

#include <caf/expected.hpp>
#include <caf/variant.hpp>

#include <random>
#include <unordered_map>

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
class reader : public multi_layout_reader {
public:
  using super = multi_layout_reader;

  /// Constructs a test reader.
  /// @param table_slice_type The ID for table slice type to build.
  /// @param options Additional options.
  /// @param in Input stream that should be nullptr. Exists for compatibility
  ///           reasons with other readers.
  explicit reader(caf::atom_value id, const caf::settings& options,
                  std::unique_ptr<std::istream> in = nullptr);

  void reset(std::unique_ptr<std::istream> in);

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  const char* name() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  vast::schema schema_;
  std::mt19937_64 generator_;
  size_t num_events_;
  schema::const_iterator next_;
  std::unordered_map<type, blueprint> blueprints_;
};

} // namespace vast::format::test
