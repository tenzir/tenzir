#ifndef VAST_ERROR_H
#define VAST_ERROR_H

#include "vast/fwd.h"
#include "vast/util/error.h"

namespace vast {

using util::error;

void serialize(serializer& sink, error const& e);
void deserialize(deserializer& source, error& e);

} // namespace vast

#endif
