#ifndef VAST_INGESTOR_H
#define VAST_INGESTOR_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include "vast/uuid.h"
#include "vast/sink/segmentizer.h"

namespace vast {

/// The ingestor. This component manages different types of event sources, each
/// of which generate events in a different manner.
class ingestor : public cppa::event_based_actor
{
public:
  /// Spawns an ingestor.
  /// @param receiver The receiver
  ingestor(cppa::actor_ptr receiver,
           size_t max_events_per_chunk,
           size_t max_segment_size,
           size_t batch_size);

  /// Implements `cppa::event_based_actor::init`.
  virtual void init() final;

  /// Overrides `cppa::event_based_actor::on_exit`.
  virtual void on_exit() override;

private:
  template <typename Source, typename... Args>
  cppa::actor_ptr make_source(Args&&... args)
  {
    using namespace cppa;
    auto snk = spawn<sink::segmentizer>(self, max_events_per_chunk_,
                                        max_segment_size_);
    auto src = spawn<Source, detached>(snk, std::forward<Args>(args)...);
    send(src, atom("batch size"), batch_size_);
    src->link_to(snk);
    src->establish_backlink(snk);
    self->monitor(snk);
    sinks_.emplace(std::move(snk), 0);
    return src;
  }

  void init_source(cppa::actor_ptr src);

  cppa::actor_ptr receiver_;
  size_t max_events_per_chunk_;
  size_t max_segment_size_;
  size_t batch_size_;
  std::unordered_map<cppa::actor_ptr, size_t> sinks_;
};

} // namespace vast

#endif
