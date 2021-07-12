//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/expression.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_actor_view.hpp>

namespace vast {

/// A wrapper for an expression related command.
struct query {
  query() = default;
  query(query&&) = default;
  query(const query&) = default;

  struct count {
    enum mode { estimate, exact };
    system::receiver_actor<uint64_t> sink;
    enum mode mode = {};

    friend bool operator==(const count& lhs, const count& rhs) {
      return lhs.sink == rhs.sink && lhs.mode == rhs.mode;
    }

    template <class Inspector>
    friend auto inspect(Inspector& f, count& x) {
      return f(caf::meta::type_name("vast.query.count"), x.sink, x.mode);
    }
  };

  struct extract {
    enum mode { drop_ids, preserve_ids };
    system::receiver_actor<table_slice> sink;
    mode policy = {};

    friend bool operator==(const extract& lhs, const extract& rhs) {
      return lhs.sink == rhs.sink && lhs.policy == rhs.policy;
    }

    template <class Inspector>
    friend auto inspect(Inspector& f, extract& x) {
      return f(caf::meta::type_name("vast.query.extract"), x.sink, x.policy);
    }
  };

  struct erase {
    friend bool operator==(const erase&, const erase&) {
      return true;
    }

    template <class Inspector>
    friend auto inspect(Inspector& f, erase&) {
      return f(caf::meta::type_name("vast.query.erase"));
    }
  };

  using command = caf::variant<erase, count, extract>;

  command cmd;
  expression expr = {};

  query(const command& cmd, const expression& expr) : cmd(cmd), expr(expr) {
  }

  query(command&& cmd, expression&& expr)
    : cmd(std::move(cmd)), expr(std::move(expr)) {
  }

  // -- Helper functions to make query creation less boiler-platey.

  template <class Actor>
  static query
  make_count(const Actor& sink, enum count::mode m, expression expr) {
    return {count{caf::actor_cast<system::receiver_actor<uint64_t>>(sink), m},
            std::move(expr)};
  }

  template <class Actor>
  static query
  make_extract(const Actor& sink, extract::mode p, expression expr) {
    return {extract{caf::actor_cast<system::receiver_actor<table_slice>>(sink),
                    p},
            std::move(expr)};
  }

  static query make_erase(expression expr) {
    return {erase{}, std::move(expr)};
  }

  // -- Helper functions to make query creation less boiler-platey.

  friend bool operator==(const query& lhs, const query& rhs) {
    return lhs.cmd == rhs.cmd && lhs.expr == rhs.expr;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, query& q) {
    return f(caf::meta::type_name("vast.query"), q.cmd, q.expr);
  }
};

} // namespace vast
