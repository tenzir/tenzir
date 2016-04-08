#ifndef VAST_CONCEPT_SERIALIZABLE_STD_CHRONO_HPP
#define VAST_CONCEPT_SERIALIZABLE_STD_CHRONO_HPP

#include <chrono>

#include "vast/concept/serializable/builtin.hpp"

namespace vast {

template <typename Serializer, typename Rep, typename Period>
void serialize(Serializer& sink, std::chrono::duration<Rep, Period> d) {
  serialize(sink, d.count());
}

template <typename Deserializer, typename Rep, typename Period>
void deserialize(Deserializer& source, std::chrono::duration<Rep, Period>& d) {
  Rep x;
  deserialize(source, x);
  d = std::chrono::duration<Rep, Period>(x);
}

template <typename Serializer, typename Clock, typename Duration>
void serialize(Serializer& sink, std::chrono::time_point<Clock, Duration> t) {
  serialize(sink, t.time_since_epoch());
}

template <typename Deserializer, typename Clock, typename Duration>
void deserialize(Deserializer& source,
                 std::chrono::time_point<Clock, Duration>& t) {
  Duration since_epoch;
  deserialize(source, since_epoch);
  t = std::chrono::time_point<Clock, Duration>(since_epoch);
}

} // namespace vast

#endif
