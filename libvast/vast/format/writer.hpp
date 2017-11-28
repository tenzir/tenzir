#ifndef VAST_FORMAT_WRITER_HPP
#define VAST_FORMAT_WRITER_HPP

#include <vector>

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"

namespace vast {
namespace format {

/// Base class for writers.
template <class Derived>
class writer {
public:
  /// Writes events according to a specific format.
  /// @param xs The batch of events to write.
  /// @returns `no_error` on success.
  expected<void> write(const std::vector<event>& xs) {
    for (auto& x : xs) {
      auto r = static_cast<Derived*>(this)->process(x);
      if (!r)
        return r;
    }
    return no_error;
  }

  /// Writes an event according to a specific format.
  /// @param x The single events to write.
  /// @returns `no_error` on success.
  expected<void> write(const event& x) {
    return static_cast<Derived*>(this)->process(x);
  }

  /// Implemented by derived classes in case the flush operation is different
  /// from a no-op.
  /// @returns `no_error` on success.
  expected<void> flush() {
    return no_error;
  }
};

} // namespace format
} // namespace vast

#endif

