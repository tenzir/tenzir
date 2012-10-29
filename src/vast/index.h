#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include <unordered_map>
#include <cppa/cppa.hpp>

namespace vast {

/// The event index.
class index : public cppa::sb_actor<index>
{
public:
  /// Spawns the index.
  /// @param directory The root directory of the index.
  index(std::string directory);

  cppa::behavior init_state;

private:
  std::string const dir_;
  std::unordered_map<std::string, cppa::actor_ptr> filaments_;
};

} // namespace vast

#endif
