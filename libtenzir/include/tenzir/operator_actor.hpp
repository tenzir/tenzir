//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

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

// WITHOUT END
// observable<variant<checkpoint, T>>

// WITH END
// observable<variant<checkpoint, end, T>>
// observable<variant<end, pair<checkpoint, observable<T>>>

// HOW TO SIGNAL PREVIOUS
// -> observable<variant<checkpoint, end, T>>
// <- stop

// what does `stop` do?

// from kafka | head 10?

// DB transform operator

template <class T>
struct msg : variant<checkpoint, T> {
  using super = variant<checkpoint, T>;
  using super::super;

  friend auto inspect(auto& f, msg<T>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

template <>
struct msg<void> : variant<checkpoint> {
  using super = variant<checkpoint>;
  using super::super;

  template <class U>
    requires(not std::is_void_v<U>)
  explicit(false) operator msg<U>() && {
    return as<checkpoint>(std::move(*this));
  }

  friend auto inspect(auto& f, msg<void>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

template <class T>
using msg_stream = caf::typed_stream<msg<T>>;

template <class T>
struct as_msg_stream {
  using type = msg_stream<T>;
};

using msg_types = caf::type_list<void, table_slice>;
using msg_stream_types = caf::detail::tl_map_t<msg_types, as_msg_stream>;
using any_msg_stream = detail::tl_apply_t<msg_stream_types, variant>;

struct operator_actor_traits {
  using signatures = caf::type_list<
    //
    auto(handshake hs)->caf::result<handshake_response>>;
};

using operator_actor = caf::typed_actor<operator_actor_traits>;

struct handshake {
  handshake() = default;

  explicit(false) handshake(any_msg_stream input) : input{std::move(input)} {
  }

  // TODO: is this the rollback manager?
  using checkpoint_receiver_actor
    = caf::typed_actor<auto(checkpoint, chunk_ptr)->caf::result<void>>;

  variant<caf::typed_stream<msg<void>>, caf::typed_stream<msg<table_slice>>>
    input;

  // FIXME: can this be nullptr? when do we actually load this state?
  chunk_ptr state;

  // FIXME: actually set this:
  checkpoint_receiver_actor checkpoint_receiver;

  friend auto inspect(auto& f, handshake& x) -> bool {
    return f.object(x).fields(
      f.field("input", x.input), f.field("state", x.state),
      f.field("checkpoint_receiver", x.checkpoint_receiver));
  }
};

struct handshake_response {
  variant<caf::typed_stream<msg<void>>, caf::typed_stream<msg<table_slice>>>
    output;
  friend auto inspect(auto& f, handshake_response& x) -> bool {
    return f.apply(x.output);
  }
};

} // namespace tenzir::exec
