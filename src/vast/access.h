#ifndef VAST_ACCESS_H
#define VAST_ACCESS_H

namespace vast {

/// Provides clean access of private class internals. Used by various concepts.
struct access
{
  struct serializable;  // serialization.h
  struct parsable;      // util/parse.h
  struct printable;     // util/print.h
};

} // namespace vast

#endif
