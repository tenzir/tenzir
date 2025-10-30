#include "tenzir/allocator_config.hpp"

#if TENZIR_SELECT_ALLOCATOR != TENZIR_SELECT_ALLOCATOR_NONE

#  include "tenzir/allocator.hpp"

#  include <cerrno>
#  include <cstddef>
#  include <cstring>
#  include <limits>
#  include <mimalloc.h>
#  include <new>

namespace {

/// We know that mimallocs default alignment is 16:
/// https://github.com/microsoft/mimalloc/blob/v3.1.5/include/mimalloc/types.h#L32-L34
/// This header unfortunately is not installed.
/// We now need to ensure that this is at least as strict as the default
/// alignment of the system malloc, in order to maintain the alignment
/// guarantees on our malloc override. This is not great as it decouples us from
/// the actual value used by mimalloc, however it will only be an issue if we
/// ever compile on a system where the default alignment is 32 bytes.
static_assert(alignof(std::max_align_t) <= 16,
              "Unexpectedly large default alignment");

[[nodiscard]] constexpr auto
multiply_overflows(std::size_t lhs, std::size_t rhs,
                   std::size_t& result) noexcept -> bool {
#  if defined(__has_builtin)
#    if __has_builtin(__builtin_mul_overflow)
  return __builtin_mul_overflow(lhs, rhs, &result);
#    endif
#  endif
  if (lhs == 0 || rhs == 0) {
    result = 0;
    return false;
  }
  if (lhs > std::numeric_limits<std::size_t>::max() / rhs) {
    return true;
  }
  result = lhs * rhs;
  return false;
}

[[nodiscard]] constexpr auto is_power_of_two(std::size_t value) noexcept
  -> bool {
  return std::popcount(value) == 1;
}

[[nodiscard]] constexpr auto
is_valid_posix_alignment(std::size_t alignment) noexcept -> bool {
  return is_power_of_two(alignment) && alignment % sizeof(void*) == 0;
}

[[nodiscard]] auto allocate_bytes(std::size_t size) noexcept -> void* {
  auto* ptr = tenzir::memory::c_allocator().allocate(size);
  if (ptr == nullptr && size != 0) {
    errno = ENOMEM;
  }
  return ptr;
}

[[nodiscard]] auto
allocate_bytes(std::size_t size, std::align_val_t alignment) noexcept -> void* {
  auto* ptr = tenzir::memory::c_allocator().allocate(size, alignment);
  if (ptr == nullptr && size != 0) {
    errno = ENOMEM;
  }
  return ptr;
}

auto deallocate_bytes(void* ptr) noexcept -> void {
  tenzir::memory::c_allocator().deallocate(ptr);
}

[[nodiscard]] auto reallocate_bytes(void* ptr, std::size_t new_size) noexcept
  -> void* {
  if (ptr == nullptr) {
    return allocate_bytes(new_size);
  }
  if (new_size == 0) {
    deallocate_bytes(ptr);
    return nullptr;
  }
  auto* result = tenzir::memory::c_allocator().reallocate(ptr, new_size);
  if (result == nullptr) {
    errno = ENOMEM;
  }
  return result;
}

} // namespace

extern "C" {

[[gnu::hot, gnu::malloc, gnu::alloc_size(1)]]
auto malloc(std::size_t size) -> void* {
  return allocate_bytes(size);
}

[[gnu::hot, gnu::malloc, gnu::alloc_size(1, 2)]]
auto calloc(std::size_t count, std::size_t size) -> void* {
  auto* ptr = tenzir::memory::c_allocator().calloc(count, size);
  if (ptr == nullptr && count != 0 && size != 0) {
    errno = ENOMEM;
  }
  return ptr;
}

[[gnu::hot, gnu::alloc_size(2)]]
auto realloc(void* ptr, std::size_t new_size) -> void* {
  return reallocate_bytes(ptr, new_size);
}

[[gnu::hot, gnu::alloc_size(2, 3)]]
auto reallocarray(void* ptr, std::size_t count, std::size_t size) -> void* {
  std::size_t total = 0;
  if (multiply_overflows(count, size, total)) {
    errno = ENOMEM;
    return nullptr;
  }
  return reallocate_bytes(ptr, total);
}

[[gnu::hot]]
auto free(void* ptr) -> void {
  deallocate_bytes(ptr);
}

#  if TENZIR_LINUX
[[gnu::hot]]
auto malloc_usable_size(const void* ptr) -> std::size_t {
  return tenzir::memory::c_allocator().size(ptr);
}
#  elif TENZIR_MACOS
[[gnu::hot]]
auto malloc_size(const void* ptr) -> std::size_t {
  return tenzir::memory::c_allocator().size(ptr);
}
#  endif

[[gnu::hot, gnu::alloc_size(2), gnu::alloc_align(1)]]
auto aligned_alloc(std::size_t alignment, std::size_t size) -> void* {
  if (! is_power_of_two(alignment) || size % alignment != 0) {
    errno = EINVAL;
    return nullptr;
  }
  return allocate_bytes(size, std::align_val_t{alignment});
}

[[gnu::hot]]
auto memalign(std::size_t alignment, std::size_t size) -> void* {
  if (! is_power_of_two(alignment) || alignment % sizeof(void*) != 0) {
    errno = EINVAL;
    return nullptr;
  }
  return allocate_bytes(size, std::align_val_t{alignment});
}

[[gnu::hot, gnu::nonnull(1)]]
auto posix_memalign(void** out, std::size_t alignment, std::size_t size)
  -> int {
  if (not is_valid_posix_alignment(alignment)) {
    return EINVAL;
  }
  auto* ptr = allocate_bytes(size, std::align_val_t{alignment});
  if (ptr == nullptr && size != 0) {
    *out = nullptr;
    return ENOMEM;
  }
  *out = ptr;
  return 0;
}

} // extern "C"

#endif
