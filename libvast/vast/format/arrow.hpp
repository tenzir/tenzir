/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <string>

#include "plasma/client.h"
#include "plasma/common.h"

#include "arrow/api.h"
#include "arrow/builder.h"

#include "vast/data.hpp"
#include "vast/expected.hpp"
#include "vast/type.hpp"

namespace vast {

class event;

namespace format {
namespace arrow {

/// Converts events into Arrow Record batches and writes them into a Plasma
/// store.

class writer {
public:
  writer() = default;

  /// Constructs an Arrow writer that connects to a (local) plasma store.
  /// @param plasma_socket The path to the local Plasma listening socket.
  writer(const std::string& plasma_socket);

  ~writer();

  expected<void> write(const std::vector<event>& e);

  expected<void> write(const std::vector<event>& e,
                       std::vector<plasma::ObjectID>& oids);

  expected<void> write(const event& x);

  expected<void> flush();

  const char* name() const;

  /// Checks whether the writer is connected to the Plasma store.
  /// @returns `true` if the connection to the Plasma store is alive.
  bool connected() const;

private:
  expected<plasma::ObjectID> make_object(const void* data, size_t size);

  bool connected_;

  plasma::PlasmaClient plasma_client_;

  std::vector<event> buffer_;
};

/// Converts a VAST type to an Arrow schema for a `RecordBatch`.
struct convert_visitor {
  using result_type = std::shared_ptr<::arrow::Field>;

  result_type operator()(const boolean_type&);

  result_type operator()(const count_type&);

  result_type operator()(const integer_type&);

  result_type operator()(const real_type&);

  result_type operator()(const string_type&);

  result_type operator()(const pattern_type&);

  result_type operator()(const address_type&);

  result_type operator()(const port_type&);

  result_type operator()(const subnet_type&);

  result_type operator()(const timespan_type&);

  result_type operator()(const timestamp_type&);

  result_type operator()(const vector_type& t);

  result_type operator()(const set_type& t);

  template <class T>
  result_type operator()(const T&) {
    return ::arrow::field("none", ::arrow::null());
  }
};

struct insert_visitor_helper {
  using result_type = ::arrow::Status;

  // Arrow builder for the current field
  ::arrow::ArrayBuilder* builder;

  insert_visitor_helper(::arrow::ArrayBuilder* b);

  result_type operator()(const boolean_type&, boolean d);

  result_type operator()(const count_type&, count d);

  result_type operator()(const integer_type&, int d);

  result_type operator()(const real_type&, real d);

  result_type operator()(const string_type&, std::string d);

  result_type operator()(const pattern_type&, const pattern& d);

  result_type operator()(const address_type&, const address& d);

  result_type operator()(const port_type&, const port& d);

  result_type operator()(const subnet_type&, const subnet& d);

  result_type operator()(const timespan_type&, const timespan& d);

  result_type operator()(const timestamp_type&, const timestamp& d);

  result_type operator()(const none_type&, caf::none_t);

  // Matched for all complex types
  template <class T, class D>
  result_type operator()(const T& t, const D& d) {
    return append_to_list(t, d);
  }

  // Cast the current builder to Listbuilder if T set_type or vector_type
  // and appends all Items for D else returns a TypeError
  template <class T, class D>
  result_type append_to_list(const T& t, const D& d) {
    if constexpr (
      (std::is_same_v<T, set_type> || std::is_same_v<T, vector_type>)&&(
        std::is_same_v<D, std::vector<data>> || std::is_same_v<D, set>)) {
      auto lbuilder = static_cast<::arrow::ListBuilder*>(builder);
      auto status = lbuilder->Reserve(d.size());
      if (!status.ok())
        return status;
      status = lbuilder->Append();
      if (!status.ok())
        return status;
      for (auto v : d) {
        format::arrow::insert_visitor_helper a(lbuilder->value_builder());
        status = caf::visit(a, t.value_type, v);
        if (!status.ok())
          return status;
      }
      return result_type::OK();
    } else
      return result_type::TypeError("Invalid Type");
  }
};

// Visitor to cast the field builder to the correct buildertype and append data
struct insert_visitor {
  using result_type = ::arrow::Status;

  std::shared_ptr<::arrow::RecordBatchBuilder> rbuilder;

  // Index of the current builder
  u_int64_t cbuilder = 0;

  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b);

  insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b,
                 u_int64_t cbuilder);

  result_type operator()(const record_type& t, const std::vector<data>& d);

  result_type operator()(const count_type& t, count d);

  result_type operator()(const integer_type& t, integer d);

  result_type operator()(const real_type& t, real d);

  result_type operator()(const type& t, const data& d);

  result_type operator()(const string_type& t, std::string d);

  result_type operator()(const boolean_type& t, bool d);

  result_type operator()(const timestamp_type& t, const timestamp& d);

  result_type operator()(const timespan_type& t, const timespan& d);

  result_type operator()(const subnet_type& t, const subnet& d);

  result_type operator()(const address_type& t, const address& d);

  result_type operator()(const port_type& t, const port& d);

  // caf::none_t data -> AppendNull
  result_type operator()(const none_type&, caf::none_t);

  result_type operator()(const count_type& t, caf::none_t);

  result_type operator()(const integer_type& t, caf::none_t d);

  result_type operator()(const real_type& t, caf::none_t);

  result_type operator()(const string_type& t, caf::none_t);

  result_type operator()(const boolean_type& t, caf::none_t);

  result_type operator()(const timespan_type& t, caf::none_t);

  result_type operator()(const address_type& t, caf::none_t);

  result_type operator()(const port_type& t, caf::none_t);

  result_type operator()(const vector_type& t, caf::none_t);

  result_type operator()(const set_type& t, caf::none_t);

  // Matched for all complex types
  template <class T, class D>
  result_type operator()(const T& t, const D& d) {
    auto lbuilder = rbuilder->GetField(cbuilder);
    auto a = insert_visitor_helper(lbuilder);
    return a.append_to_list(t, d);
  }
};

} // namespace arrow
} // namespace format
} // namespace vast
