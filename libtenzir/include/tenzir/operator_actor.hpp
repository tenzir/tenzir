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

namespace tenzir {

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
struct message<void> : checkpoint {
  friend auto inspect(auto& f, message<void>& x) -> bool {
    return f.apply(static_cast<checkpoint&>(x));
  }
};

struct operator_actor_traits {
  using signatures = caf::type_list<
    //
    auto(handshake hs)->caf::result<handshake_response>>;
};

using operator_actor = caf::typed_actor<operator_actor_traits>;

struct handshake {
  explicit(false) handshake(variant<caf::typed_stream<message<void>>,
                                    caf::typed_stream<message<table_slice>>>
                              input)
    : input{std::move(input)} {
  }

  variant<caf::typed_stream<message<void>>,
          caf::typed_stream<message<table_slice>>>
    input;

  friend auto inspect(auto& f, handshake& x) -> bool {
    return f.apply(x.input);
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

} // namespace tenzir
