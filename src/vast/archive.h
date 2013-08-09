#ifndef VAST_ARCHIVE_H
#define VAST_ARCHIVE_H

#include <unordered_map>
#include <cppa/cppa.hpp>

namespace vast {

/// The event archive. It stores events in the form of segments.
class archive : public cppa::event_based_actor
{
public:
  /// Spawns the archive.
  /// @param directory The root directory of the archive.
  /// @param max_segments The maximum number of segments to keep in memory.
  archive(std::string const& directory, size_t max_segments);

  /// Implements `cppa::event_based_actor::init`.
  virtual void init() final;

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

private:
  std::string directory_;
  cppa::actor_ptr segment_manager_;
};

} // namespace vast

#endif
