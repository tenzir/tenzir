#ifndef VAST_QUERY_PROCESSOR_H
#define VAST_QUERY_PROCESSOR_H

#include <ze/component.h>
#include <ze/sink.h>
#include "vast/query/forward.h"

namespace vast {
namespace query {

/// Processes queries over continuous event streams.
class processor : public ze::component<ze::event>::sink
{
    processor(processor const&) = delete;
    processor& operator=(processor const&) = delete;

public:
    /// Constructs a processor.
    /// @param c The component the processor belongs to.
    processor(ze::component<ze::event>& c);

    void init();
    void submit(query const& q);

private:
    void process(ze::event_ptr&& event);
};

} // namespace query
} // namespace vast

#endif
