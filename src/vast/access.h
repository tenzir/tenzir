#ifndef VAST_ACCESS_H
#define VAST_ACCESS_H

namespace vast {

/// Wrapper to encapsulate the implementation of concepts requiring access to
/// private state.
struct access
{
  template <typename, typename = void>
  struct state;

  template <typename, typename = void>
  struct parsable;

  template <typename, typename = void>
  struct printable;

  template <typename, typename = void>
  struct convertible;
};

} // namespace vast

#endif
