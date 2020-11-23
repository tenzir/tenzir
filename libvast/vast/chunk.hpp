/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/byte.hpp"
#include "vast/detail/assert.hpp"
#include "vast/function.hpp"
#include "vast/fwd.hpp"
#include "vast/span.hpp"

#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include <type_traits>
#include <utility>

namespace vast {

/// A contiguous block of memory; essentially a span of bytes with a deleter.
class chunk final : public caf::ref_counted {
public:
  // -- member types -----------------------------------------------------------

  using value_type = byte;
  using view_type = span<const value_type>;
  using pointer = typename view_type::pointer;
  using size_type = typename view_type::size_type;
  using iterator = typename view_type::iterator;
  using deleter_type = unique_function<void() noexcept>;

  // -- constructors, destructors, and assignment operators --------------------

  /// Forbid all means of construction explicitly.
  chunk() noexcept = delete;
  chunk(const chunk&) noexcept = delete;
  chunk& operator=(const chunk&) noexcept = delete;
  chunk(chunk&&) noexcept = delete;
  chunk& operator=(chunk&&) noexcept = delete;

  /// Destroys the chunk and releases owned memory via the deleter.
  ~chunk() noexcept override;

  // -- factory functions ------------------------------------------------------

  /// Constructs a chunk of particular size and pointer to data.
  /// @param data The raw byte data.
  /// @param size The number of bytes *data* points to.
  /// @param deleter The function to delete the data.
  /// @returns A chunk pointer or `nullptr` on failure.
  static chunk_ptr
  make(const void* data, size_type size, deleter_type&& deleter) noexcept;

  /// Constructs a chunk of particular size and pointer to data.
  /// @param view The span holding the raw data.
  /// @param deleter The function to delete the data.
  /// @returns A chunk pointer or `nullptr` on failure.
  static chunk_ptr make(view_type view, deleter_type&& deleter) noexcept;

  /// Construct a chunk from a byte buffer, and bind the lifetime of the chunk
  /// to the buffer.
  /// @param buffer The byte buffer.
  /// @returns A chunk pointer or `nullptr` on failure.
  template <class Buffer, class = std::enable_if_t<
                            std::negation_v<std::is_lvalue_reference<Buffer>>>>
  static auto make(Buffer&& buffer) -> decltype(as_bytes(buffer), chunk_ptr{}) {
    // If the buffer is trivially-move-assignable, put a copy on the heap first.
    if constexpr (std::is_trivially_move_assignable_v<Buffer>) {
      auto copy = std::make_unique<Buffer>(std::move(buffer));
      auto view = as_bytes(*copy);
      return make(view, [copy = std::move(copy)]() noexcept {
        static_cast<void>(copy);
      });
    } else {
      auto view = as_bytes(buffer);
      return make(view, [buffer = std::move(buffer)]() noexcept {
        static_cast<void>(buffer);
      });
    }
  }

  /// Construct a chunk from a byte buffer, and binds the lifetime of the chunk
  /// to the buffer.
  /// @param buffer The byte buffer.
  /// @returns A chunk pointer or `nullptr` on failure.
  template <class Buffer,
            class = std::enable_if_t<
              std::negation_v<std::is_lvalue_reference<
                Buffer>> && sizeof(*std::data(std::declval<Buffer>())) == 1>>
  static auto make(Buffer&& buffer)
    -> decltype(std::data(buffer), std::size(buffer), chunk_ptr{}) {
    // If the buffer is trivially-move-assignable, put a copy on the heap first.
    if constexpr (std::is_trivially_move_assignable_v<Buffer>) {
      auto copy = std::make_unique<Buffer>(std::move(buffer));
      const auto data = std::data(*copy);
      const auto size = std::size(*copy);
      return make(data, size, [copy = std::move(copy)]() noexcept {
        static_cast<void>(copy);
      });
    } else {
      const auto data = std::data(buffer);
      const auto size = std::size(buffer);
      return make(data, size, [buffer = std::move(buffer)]() noexcept {
        static_cast<void>(buffer);
      });
    }
  }

  /// Avoid the common mistake of binding ownership to a span.
  template <class Byte, size_t Extent>
  static auto make(span<Byte, Extent>&&) = delete;

  /// Avoid the common mistake of binding ownership to a string view.
  static auto make(std::string_view&&) = delete;

  /// Memory-maps a chunk from a read-only file.
  /// @param filename The name of the file to memory-map.
  /// @param size The number of bytes to map. If 0, map the entire file.
  /// @param offset Where to start in terms of number of bytes from the start.
  /// @returns A chunk pointer or `nullptr` on failure.
  static chunk_ptr
  mmap(const path& filename, size_type size = 0, size_type offset = 0);

  // -- container facade -------------------------------------------------------

  /// @returns The pointer to the chunk.
  pointer data() const noexcept;

  /// @returns The size of the chunk.
  size_type size() const noexcept;

  /// @returns A pointer to the first byte in the chunk.
  iterator begin() const noexcept;

  /// @returns A pointer to one past the last byte in the chunk.
  iterator end() const noexcept;

  // -- accessors --------------------------------------------------------------

  /// Creates a new chunk that structurally shares the data of this chunk.
  /// @param start The offset from the beginning where to begin the new chunk.
  /// @param length The length of the slice, beginning at *start*.
  /// @returns A new chunk over the subset.
  /// @pre `start < size()`
  chunk_ptr
  slice(size_type start, size_type length
                         = std::numeric_limits<size_type>::max()) const;

  /// Adds an additional step for deleting this chunk.
  /// @param step Function object that gets called after all previous deletion
  /// steps ran. It must be nothrow-invocable, as it gets called during the
  /// destructor of chunk.
  template <class Step>
  void add_deletion_step(Step&& step) noexcept {
    static_assert(std::is_nothrow_invocable_r_v<void, Step>,
                  "'Step' must have the signature 'void () noexcept'");
    if (deleter_) {
      auto g = [first = std::move(deleter_),
                second = std::forward<Step>(step)]() mutable noexcept {
        std::invoke(std::move(first));
        std::invoke(std::move(second));
      };
      deleter_ = std::move(g);
    } else {
      deleter_ = std::move(step);
    }
  }

  // -- concepts --------------------------------------------------------------

  friend span<const byte> as_bytes(const chunk_ptr& x) noexcept;
  friend caf::error write(const path& filename, const chunk_ptr& x);
  friend caf::error read(const path& filename, chunk_ptr& x);
  friend caf::error inspect(caf::serializer& sink, const chunk_ptr& x);
  friend caf::error inspect(caf::deserializer& source, chunk_ptr& x);

private:
  // -- implementation details -------------------------------------------------

  /// Constructs a chunk from a span and a deleter.
  /// @param view The span holding the raw data.
  /// @param deleter The function to delete the data.
  chunk(view_type view, deleter_type&& deleter) noexcept;

  /// A sized view on the raw data.
  const view_type view_;

  /// The function to delete the data.
  deleter_type deleter_;
};

} // namespace vast
