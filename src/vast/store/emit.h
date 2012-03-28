#ifndef VAST_STORE_EMIT_H
#define VAST_STORE_EMIT_H

#include "vast/store/loader.h"
#include "vast/comm/event_source.h"

namespace vast {
namespace store {

/// The emit component.
class emit : public ze::component<ze::event>
{
    emit(emit const&) = delete;
    emit& operator=(emit const&) = delete;

public:
    emit(ze::io& io);

    void init(fs::path const& directory);

    void run();

private:
    loader loader_;
};

} // namespace store
} // namespace vast

#endif
