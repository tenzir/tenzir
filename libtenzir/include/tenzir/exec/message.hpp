//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/element_type.hpp"
#include "tenzir/exec/checkpoint.hpp"
#include "tenzir/exec/exhausted.hpp"
#include "tenzir/variant.hpp"

#include <caf/flow/observable.hpp>

namespace tenzir::exec {

template <class T>
struct message : variant<checkpoint, exhausted, T> {
  // Validate that T is a supported element type. Note that this is not
  // expressed as a concept on the template as message is forward-declared.
  static_assert(element_type<T>);

  using super = variant<checkpoint, exhausted, T>;
  using super::super;

  friend auto inspect(auto& f, message<T>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

template <>
struct message<void> : variant<checkpoint, exhausted> {
  using super = variant<checkpoint, exhausted>;
  using super::super;

  template <class U>
    requires(not std::is_void_v<U>)
  explicit(false) operator message<U>() && {
    return as<checkpoint>(std::move(*this));
  }

  friend auto inspect(auto& f, message<void>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

template <element_type T>
using stream = caf::typed_stream<message<T>>;

template <element_type T>
using observable = caf::flow::observable<message<T>>;

template <class T>
struct as_stream {
  using type = stream<T>;
};

using stream_types = caf::detail::tl_map_t<element_types, as_stream>;

struct any_stream : detail::tl_apply_t<stream_types, variant> {
  using super = detail::tl_apply_t<stream_types, variant>;
  using super::super;

  auto to_element_type_tag() const -> element_type_tag {
    return match([]<element_type T>(const stream<T>&) -> element_type_tag {
      return tag_v<T>;
    });
  }
};

} // namespace tenzir::exec
