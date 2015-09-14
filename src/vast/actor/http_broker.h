#ifndef VAST_ACTOR_HTTP_BROKER_H
#define VAST_ACTOR_HTTP_BROKER_H

#include <caf/io/broker.hpp>

#include "vast/caf.h"

namespace vast {

/// A broker translating HTTP messages into VAST operations.
/// @param self The actor handle.
/// @param node The NODE which spawned *self*.
/// @returns The actor behavior.
behavior http_broker(caf::io::broker* self, actor const& node);

} // namespace vast

#endif
