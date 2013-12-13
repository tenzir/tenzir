#ifndef VAST_INGESTOR_H
#define VAST_INGESTOR_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/uuid.h"
#include "vast/segmentizer.h"

namespace vast {

/// The ingestor. This component manages different types of event sources, each
/// of which generate events in a different manner.
class ingestor : public actor<ingestor>
{
public:
  /// Spawns an ingestor.
  ///
  /// @param receiver The actor receiving the generated segments.
  ///
  /// @param max_events_per_chunk The maximum number of events per chunk.
  ///
  /// @param max_segment_size The maximum size of a segment.
  ///
  /// @param batch_size The number of events a synchronous source buffers until
  /// relaying them to the segmentizer
  ingestor(cppa::actor_ptr receiver,
           size_t max_events_per_chunk,
           size_t max_segment_size,
           uint64_t batch_size);

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

  void act();
  char const* description() const;

private:
  template <typename Source, typename... Args>
  cppa::actor_ptr make_source(Args&&... args)
  {
    using namespace cppa;

    // FIXME: figure out why detaching the segmentizer yields a two-fold
    // performance increase in the ingestion rate.
    auto snk = spawn<segmentizer, monitored>(
        self, max_events_per_chunk_, max_segment_size_);

    auto src = spawn<Source, detached>(snk, std::forward<Args>(args)...);
    send(src, atom("batch size"), batch_size_);

    snk->link_to(src);

    sinks_.emplace(std::move(snk), 0);
    sources_.insert(src);

    return src;
  }

  cppa::actor_ptr receiver_;
  size_t max_events_per_chunk_;
  size_t max_segment_size_;
  uint64_t batch_size_;
  std::set<cppa::actor_ptr> sources_;
  std::unordered_map<cppa::actor_ptr, uint64_t> sinks_;
};

} // namespace vast

#endif
