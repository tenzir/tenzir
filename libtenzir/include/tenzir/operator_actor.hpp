//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/variant.hpp"

#include <caf/scheduled_actor/flow.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_stream.hpp>

namespace tenzir::exec {

struct checkpoint {
  friend auto inspect(auto& f, checkpoint& x) -> bool {
    return f.object(x).fields();
  }
};

struct exhausted {};

// WITHOUT END
// observable<variant<checkpoint, T>>

// WITH END
// observable<variant<checkpoint, end, T>>
// observable<variant<end, pair<checkpoint, observable<T>>>
// observable<variant<end, checkpoint, observable<T>>

// HOW TO SIGNAL PREVIOUS
// -> observable<variant<checkpoint, end, T>>
// <- stop

// what does `stop` do?

// from kafka | head 10?

// DB transform operator

template <class T>
struct message : variant<checkpoint, exhausted, T> {
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

template <class T>
using stream = caf::typed_stream<message<T>>;

template <class T>
using observable = caf::flow::observable<message<T>>;

template <class T>
struct as_stream {
  using type = stream<T>;
};

using message_types = caf::type_list<void, table_slice>;
using stream_types = caf::detail::tl_map_t<message_types, as_stream>;
using any_stream = detail::tl_apply_t<stream_types, variant>;

struct operator_actor_traits {
  using signatures = caf::type_list<
    // Initial setup.
    auto(handshake hs)->caf::result<handshake_response>,
    // Post-commit.
    auto(checkpoint cp)->caf::result<void>,
    // Signal that the actual output is no longer relevant, only checkpoints.
    auto(atom::stop)->caf::result<void>>;
};

/// Handler for when an operator declares that it doesn't need any more input.
using operator_stop_actor
  = caf::typed_actor<auto(atom::stop)->caf::result<void>>;

using operator_actor = caf::typed_actor<operator_actor_traits>;

// TODO: is this the rollback manager?
using checkpoint_receiver_actor
  = caf::typed_actor<auto(checkpoint, chunk_ptr)->caf::result<void>>;

using operator_shutdown_actor
  = caf::typed_actor<auto(atom::done)->caf::result<void>>;

struct handshake {
  handshake() = default;

  explicit(false) handshake(any_stream input) : input{std::move(input)} {
  }

  any_stream input;

  friend auto inspect(auto& f, handshake& x) -> bool {
    return f.apply(x.input);
  }
};

struct handshake_response {
  any_stream output;

  friend auto inspect(auto& f, handshake_response& x) -> bool {
    return f.apply(x.output);
  }
};

} // namespace tenzir::exec
