#ifndef VAST_ACTOR_SOURCE_DECHUNKIFIER_HPP
#define VAST_ACTOR_SOURCE_DECHUNKIFIER_HPP

#include "vast/actor/source/base.hpp"

namespace vast {
namespace source {

/// Unpacks events of a chunk.
class dechunkifier : public base<dechunkifier> {
public:
  dechunkifier(chunk chk)
    : base<dechunkifier>{"dechunkifier"},
      chunk_{std::move(chk)},
      reader_{chunk_} {
  }

  result<event> extract() {
    auto e = reader_.read();
    if (e.empty())
      done(true);
    return e;
  }

private:
  chunk chunk_;
  chunk::reader reader_;
};

} // namespace source
} // namespace vast

#endif
