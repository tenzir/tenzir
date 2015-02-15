#ifndef VAST_ACTOR_SOURCE_TEST_H
#define VAST_ACTOR_SOURCE_TEST_H

#include <random>
#include <unordered_map>
#include "vast/data.h"
#include "vast/schema.h"
#include "vast/actor/source/synchronous.h"
#include "vast/util/random.h"
#include "vast/util/variant.h"

namespace vast {
namespace source {

using distribution = util::variant<
  std::uniform_int_distribution<integer>,
  std::uniform_int_distribution<count>,
  std::uniform_real_distribution<long double>,
  std::normal_distribution<long double>,
  util::pareto_distribution<long double>
>;

// 64-bit linear congruential generator with MMIX/Knuth parameterization.
using lcg64 =
  std::linear_congruential_engine<
    uint64_t,
    6364136223846793005ull,
    1442695040888963407ull,
    std::numeric_limits<uint64_t>::max()
  >;

/// A source that generates random events according to a given schema.
class test : public synchronous<test>
{
public:
  struct blueprint
  {
    record data;
    std::vector<distribution> dists;
  };

  /// Constructs a test source.
  /// @param id The base event ID.
  /// @param sch The schema containing the events to generate.
  /// @param events The numer of events to generate.
  test(schema sch, event_id id, uint64_t events);

  result<event> extract();
  std::string name() const override;

private:
  schema schema_;
  event_id id_;
  uint64_t events_;
  std::mt19937_64 generator_;
  schema::const_iterator next_;
  std::unordered_map<type, blueprint> blueprints_;
};

} // namespace source
} // namespace vast

#endif
