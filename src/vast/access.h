#ifndef VAST_ACCESS_H
#define VAST_ACCESS_H

namespace vast {

/// Provides clean access of private class internals. Used by various concepts.
struct access
{
  struct convertible;
  struct serializable;
  struct parsable;
  struct printable;
};

} // namespace vast

#endif
