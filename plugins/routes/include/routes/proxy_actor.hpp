//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include "routes/connection.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/status.hpp>
#include <tenzir/table_slice.hpp>

#include <caf/fwd.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <optional>

namespace tenzir::plugins::routes {

struct proxy_actor_traits {
  using signatures
    = caf::type_list<auto(atom::get)->caf::result<table_slice>,
                     auto(atom::put, table_slice slice)->caf::result<void>>;
};

using proxy_actor = caf::typed_actor<proxy_actor_traits>;

struct named_input_actor {
  input_name name;
  proxy_actor handle;

  template <class Inspector>
  friend auto inspect(Inspector& f, named_input_actor& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.routes.named_input_actor")
      .fields(f.field("name", x.name), f.field("handle", x.handle));
  }
};

struct named_output_actor {
  output_name name;
  proxy_actor handle;

  template <class Inspector>
  friend auto inspect(Inspector& f, named_output_actor& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.routes.named_output_actor")
      .fields(f.field("name", x.name), f.field("handle", x.handle));
  }
};

class proxy {
public:
  [[maybe_unused]] static constexpr auto name = "proxy";

  explicit proxy(proxy_actor::pointer self);
  auto make_behavior() -> proxy_actor::behavior_type;

private:
  auto get() -> caf::result<table_slice>;
  auto put(table_slice slice) -> caf::result<void>;

  proxy_actor::pointer self_ = {};

  // TODO: Consider using a small bounded queue instead of a single optional.
  std::optional<table_slice> queue_;
  std::optional<caf::typed_response_promise<table_slice>> get_rp_;
  std::optional<caf::typed_response_promise<void>> put_rp_;
};

} // namespace tenzir::plugins::routes
