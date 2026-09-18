//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/allocator.hpp"

#include <boost/unordered/unordered_flat_map.hpp>

#include <cstddef>
#include <functional>
#include <new>
#include <string>
#include <unordered_map>
#include <vector>

namespace tenzir::nova::storage {

/// Template satisfying the C++ Named Requirement "Allocator" for use
/// AllocatorAware containers. Internally, this uses
/// `tenzir::memory::nova_structure_allocator`.
template <typename T>
struct CppStructureAllocator {
  using value_type = T;

  constexpr CppStructureAllocator() noexcept = default;

  template <typename U>
  constexpr explicit(false)
    CppStructureAllocator(const CppStructureAllocator<U>&) noexcept {
  }

  [[nodiscard]] auto allocate(std::size_t n) -> T* {
    auto& allocator = memory::nova_structure_allocator();
    void* ptr;
    if constexpr (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
      ptr = allocator.allocate(n * sizeof(T), std::align_val_t{alignof(T)});
    } else {
      ptr = allocator.allocate(n * sizeof(T));
    }
    if (not ptr) {
      throw std::bad_alloc();
    }
    return static_cast<T*>(ptr);
  }

  auto deallocate(T* ptr, std::size_t) noexcept -> void {
    memory::nova_structure_allocator().deallocate(ptr);
  }

  friend auto operator==(const CppStructureAllocator&,
                         const CppStructureAllocator&) noexcept -> bool {
    return true;
  }
};

template <typename T>
using Vector = std::vector<T, CppStructureAllocator<T>>;

template <typename Key, typename Value, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
using UnorderedMap
  = std::unordered_map<Key, Value, Hash, Equal,
                       CppStructureAllocator<std::pair<const Key, Value>>>;

template <typename Key, typename Value, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
using UnorderedFlatMap
  = boost::unordered_flat_map<Key, Value, Hash, Equal,
                              CppStructureAllocator<std::pair<Key, Value>>>;

template <typename CharT = char, typename Traits = std::char_traits<CharT>>
using String = std::basic_string<CharT, Traits, CppStructureAllocator<CharT>>;

} // namespace tenzir::nova::storage
