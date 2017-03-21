#ifndef VAST_OVERLAY_HPP
#define VAST_OVERLAY_HPP

#include <cstddef>
#include <vector>

#include "vast/chunk.hpp"

namespace vast {

/// A random-access abstraction on top of a packed byte sequence.
/// On the wire, the overlay has the following format:
///
///        4 bytes       variable      variable
///     +-------------+---........---+------...
///     | data offset | offset table | data
///     +-------------+---........---+------...
///             |                    ^
///             |--------------------|
///
/// The `data offset` is a varbyte encoded value that points to the beginning
/// of the `data` buffer. The `offset table` is a varbyte and delta-encoded
/// sequence of offsets, where each offset *i* maps to the buffer at location
/// `data + i`.
///
/// @relates packer unpacker
class overlay {
public:
  class offset_table {
  public:
    offset_table() = default;
    explicit offset_table(const char* ptr);
    size_t operator[](size_t i) const;
    size_t size() const;
  private:
    const char* table_;
    size_t size_ = -1;
  };

  /// Default-constructs an empty overlay.
  overlay() = default;

  /// Constructs an overlay from a chunk.
  /// @param chk The chunk to create an overlay from.
  explicit overlay(chunk_ptr chk);

  /// Accesses a block of memory at a particular offset.
  /// @param i The offset to get a pointer to.
  /// @pre `i < size()`
  const char* operator[](size_t i) const;

  /// @returns The number of elements in the overlay.
  size_t size() const;

  /// Retrieves a pointer to the underlying chunk.
  chunk_ptr chunk() const;

private:
  offset_table offsets_;
  chunk_ptr chunk_;
};

} // namespace vast

#endif
