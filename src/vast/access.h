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

  template <typename F, typename... Args>
  static auto call(F f, Args&&... args)
  {
    return f(std::forward<Args>(args)...);
  }
};

} // namespace vast

#endif
