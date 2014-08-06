#ifndef VAST_SERIALIZATION_TIME_H
#define VAST_SERIALIZATION_TIME_H

#include <chrono>
#include "vast/serialization/arithmetic.h"

namespace vast {

template <typename Rep, typename Period>
void serialize(serializer& sink, std::chrono::duration<Rep, Period> d)
{
  sink << d.count();
}

template <typename Rep, typename Period>
void deserialize(deserializer& source, std::chrono::duration<Rep, Period>& d)
{
  Rep x;
  source >> x;
  d = std::chrono::duration<Rep, Period>(x);
}

template <typename Clock, typename Duration>
void serialize(serializer& sink, std::chrono::time_point<Clock, Duration> t)
{
  sink << t.time_since_epoch();
}

template <typename Clock, typename Duration>
void deserialize(deserializer& source, std::chrono::time_point<Clock, Duration>& t)
{
  Duration since_epoch;
  source >> since_epoch;
  t = std::chrono::time_point<Clock, Duration>(since_epoch);
}

} // namespace vast

#endif
