#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_FLAT_SET
#define VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_FLAT_SET

#include "vast/util/flat_set.hpp"

namespace vast {
namespace util {

template <class Processor, class T, class Compare, class Alloc>
void serialize(Processor& proc, flat_set<T, Compare, Alloc>& fs) {
  proc & fs.as_vector();
}

} // namespace vast
} // namespace util

#endif
