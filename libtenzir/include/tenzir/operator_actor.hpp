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

template <class T>
struct message : variant<checkpoint, T> {
  using super = variant<checkpoint, T>;
  using super::super;

  friend auto inspect(auto& f, message<T>& x) -> bool {
    return f.apply(static_cast<super&>(x));
  }
};

template <>
struct message<void> : variant<checkpoint> {
  using super = variant<checkpoint>;
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

struct operator_actor_traits {
  using signatures = caf::type_list<
    //
    auto(handshake hs)->caf::result<handshake_response>>;
};

using operator_actor = caf::typed_actor<operator_actor_traits>;

struct handshake {
  handshake() = default;

  explicit(false) handshake(variant<caf::typed_stream<message<void>>,
                                    caf::typed_stream<message<table_slice>>>
                              input)
    : input{std::move(input)} {
  }

  // TODO: is this the rollback manager?
  using checkpoint_receiver_actor
    = caf::typed_actor<auto(checkpoint, chunk_ptr)->caf::result<void>>;

  variant<caf::typed_stream<message<void>>,
          caf::typed_stream<message<table_slice>>>
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
  variant<caf::typed_stream<message<void>>,
          caf::typed_stream<message<table_slice>>>
    output;
  friend auto inspect(auto& f, handshake_response& x) -> bool {
    return f.apply(x.output);
  }
};

} // namespace tenzir::exec
