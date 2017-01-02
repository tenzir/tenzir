#ifndef VAST_FORMAT_TEST_HPP
#define VAST_FORMAT_TEST_HPP

#include <random>
#include <unordered_map>

#include "vast/data.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/maybe.hpp"
#include "vast/schema.hpp"
#include "vast/variant.hpp"
#include "vast/detail/random.hpp"

namespace vast {
namespace format {
namespace test {

// A type-erased probability distribution.
using distribution =
  variant<
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
class reader {
public:
  /// Constructs a test reader.
  /// @param seed A seed for the random number generator.
  /// @param n The numer of events to generate.
  /// @param id The base event ID to start at.
  reader(size_t seed = 0, uint64_t n = 100, event_id id = 0);

  maybe<event> read();

  expected<void> schema(vast::schema sch);

  expected<vast::schema> schema() const;

  const char* name() const;

private:
  vast::schema schema_;
  std::mt19937_64 generator_;
  event_id id_;
  uint64_t num_events_;
  schema::const_iterator next_;
  std::unordered_map<type, blueprint> blueprints_;
};

} // namespace test
} // namespace format
} // namespace vast

#endif
