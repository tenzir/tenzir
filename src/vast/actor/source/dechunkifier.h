#ifndef VAST_ACTOR_SOURCE_DECHUNKIFIER_H
#define VAST_ACTOR_SOURCE_DECHUNKIFIER_H

#include "vast/actor/source/synchronous.h"

namespace vast {
namespace source {

/// Unpacks events of a chunk.
class dechunkifier : public synchronous<dechunkifier>
{
public:
  dechunkifier(chunk chk)
    : chunk_{std::move(chk)},
      reader_{chunk_}
  {
  }

  result<event> extract()
  {
    auto e = reader_.read();
    if (e.empty())
      done_ = true;

    return e;
  }

  bool done() const
  {
    return done_;
  }

  std::string name() const
  {
    return "dechunkifier";
  }

private:
  chunk chunk_;
  chunk::reader reader_;
  bool done_ = false;
};

} // namespace source
} // namespace vast

#endif
