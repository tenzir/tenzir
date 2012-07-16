#ifndef VAST_STORE_INDEX_H
#define VAST_STORE_INDEX_H

#include <cppa/cppa.hpp>

namespace vast {
namespace store {

/// The event index.
class index : public cppa::sb_actor<index>
{
  friend class cppa::sb_actor<index>;

public:
  /// Spawns the index.
  /// @param directory The root directory of the index.
  index(cppa::actor_ptr archive, std::string const& directory);

private:
  cppa::actor_ptr archive_;
  cppa::behavior init_state;
};

} // namespace store
} // namespace vast

#endif
