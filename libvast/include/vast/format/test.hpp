//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/data.hpp"
#include "vast/detail/random.hpp"
#include "vast/format/multi_schema_reader.hpp"
#include "vast/format/reader.hpp"
#include "vast/module.hpp"

#include <caf/expected.hpp>
#include <caf/variant.hpp>

#include <random>
#include <unordered_map>

namespace vast::format::test {

// A type-erased probability distribution.
using distribution = caf::variant<std::uniform_int_distribution<int64_t>,
                                  std::uniform_int_distribution<uint64_t>,
                                  std::uniform_real_distribution<long double>,
                                  std::normal_distribution<long double>,
                                  detail::pareto_distribution<long double>>;

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
  std::unique_ptr<vast::data> data;
  std::vector<distribution> distributions;
};

/// Produces random events according to a given module.
class reader : public multi_schema_reader {
public:
  using super = multi_schema_reader;

  /// Constructs a test reader.
  /// @param options Additional options.
  /// @param in Input stream that should be nullptr. Exists for compatibility
  ///           reasons with other readers.
  explicit reader(const caf::settings& options, std::unique_ptr<std::istream> in
                                                = nullptr);

  void reset(std::unique_ptr<std::istream> in) override;

  caf::error module(vast::module mod) override;

  vast::module module() const override;

  const char* name() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  vast::module module_;
  std::mt19937_64 generator_;
  size_t num_events_;
  module::const_iterator next_;
  std::unordered_map<type, blueprint> blueprints_;
};

} // namespace vast::format::test
