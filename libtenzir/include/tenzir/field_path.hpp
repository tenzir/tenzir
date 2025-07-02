//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"

#include <fmt/core.h>

#include <charconv>
#include <string_view>
#include <utility>

namespace tenzir {

/// A simple utility that allows the easy creation and printing of field paths
/// in recursive operations on data/records/...
/// The API idea is that your recursive function accepts a `field_path::view p`
/// and recursion works via `p.extended_with("nested")`
class field_path {
public:
  field_path() = default;
  field_path(std::string base_path) : path_{std::move(base_path)} {
  }
  class view {
  public:
    /// Views can only be created from paths
    view() = delete;
    /// Since this view tracks and reverts modifications to a backing string,
    /// you must not copy it. Use `view.as_is()` to create a view to the same
    /// path.
    view(const view&) = delete;

    view(view&& other)
      : path_{std::exchange(other.path_, nullptr)},
        extension_length_{std::exchange(other.extension_length_, 0)} {
    }

    ~view() {
      /// Destruction of the view removes the extension from the base path
      if (not path_) {
        return;
      }
      path_->resize(path_->size() - extension_length_);
    }

    auto as_is() const -> view {
      TENZIR_ASSERT(path_, "must not use a moved-from `field_path::view`");
      return view{path_, 0};
    }

    /// Produces a new `view` that extends the path with `.{field_name}`
    auto extended_with(std::string_view field_name) -> view {
      TENZIR_ASSERT(path_, "must not use a moved-from `field_path::view`");
      if (field_name.empty()) {
        return view{path_, 0};
      }
      auto is_extension = not path_->empty();
      if (is_extension) {
        path_->append(size_t{1}, '.');
      }
      path_->append(field_name);
      return view{path_, field_name.size() + is_extension};
    }

    auto extended_with(size_t index) -> view {
      TENZIR_ASSERT(path_, "must not use a moved-from `field_path::view`");
      constexpr static auto buff_size
        = std::numeric_limits<size_t>::digits10 + 2;
      char buff[buff_size];
      auto* const buff_begin = std::data(buff) + 1;
      auto* const buff_end = std::data(buff) + buff_size - 1;
      const auto result = std::to_chars(buff_begin, buff_end, index);
      TENZIR_ASSERT(result.ec == std::errc{},
                    "failed to create path for index `{}` at `{}`: {}", index,
                    *path_, std::make_error_condition(result.ec).message());
      TENZIR_ASSERT(result.ptr < buff_end,
                    "failed to create path for index `{}` at `{}`", index,
                    *path_);
      TENZIR_ASSERT(result.ptr > buff_begin,
                    "failed to create path for index `{}` at `{}`", index,
                    *path_);
      buff[0] = '[';
      *result.ptr = ']';
      return extended_with(std::string_view{buff_begin, result.ptr + 1});
    }

  private:
    friend class field_path;
    friend class ::fmt::formatter<view>;
    view(std::string* p, size_t extension_length)
      : path_{p}, extension_length_{extension_length} {
    }

    std::string* path_;
    size_t extension_length_ = 0;
  };

  operator view() {
    return view{&path_, 0};
  }

  auto as_view() -> view {
    return static_cast<view>(*this);
  }

  static auto unknown_path() -> field_path {
    return {"<unknown>"};
  }

private:
  std::string path_;
};

} // namespace tenzir

template <>
struct fmt::formatter<::tenzir::field_path::view>
  : private fmt::formatter<std::string> {
  using base = fmt::formatter<std::string>;

  using base::parse;

  template <class FormatContext>
  auto format(const ::tenzir::field_path::view& v, FormatContext& ctx) const {
    return base::format(*v.path_, ctx);
  }
};
