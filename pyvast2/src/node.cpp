//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/fwd.hpp"

#include "vast/command.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/actor_function_view.hpp"
#include "vast/format/arrow.hpp"
#include "vast/format/json.hpp"
#include "vast/query.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/connect_to_node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/query_cursor.hpp"
#include "vast/system/remote_command.hpp"
#include "vast/uuid.hpp"

#include <caf/actor_system.hpp>
#include <caf/scoped_actor.hpp>

#if VAST_ENABLE_OPENSSL
#  include <caf/openssl/all.hpp>
#endif

#include <pybind11/iostream.h>
#include <pybind11/pybind11.h>

#include <stdexcept>

namespace py = pybind11;

namespace vast::system {

struct Node {
  Node(std::string endpoint) : endpoint(std::move(endpoint)) {
    auto maybe_node_actor = connect_to_node(self, content(cfg));
    if (!maybe_node_actor) {
      throw std::domain_error{"failed to connect"};
    }
    this->node = *maybe_node_actor;
  }

  void status() {
    auto inv = invocation{caf::settings{}, "status", {}};
    auto result = remote_command(inv, system);
  }

  void export_(const std::string& expr_string) {
    auto components = get_node_components<index_actor>(self, node);
    if (!components)
      throw std::domain_error{"failed to get index handle"};
    auto&& [index] = *components;
    VAST_ASSERT(index);
    auto expr = to<expression>(expr_string);
    if (!expr)
      throw std::domain_error{"failed to parse expression"};
    auto q = query::make_extract(self, query::extract::drop_ids, *expr);
    auto f = detail::make_actor_function_view(self, index);
    auto res = f(atom::evaluate_v, q);
    if (!res)
      throw std::domain_error{"failed to initiate query"};
    auto id = res->id;
    auto partitions = res->candidate_partitions;
    auto scheduled = res->scheduled_partitions;
    if (partitions == 0)
      return;
    VAST_ASSERT(scheduled <= partitions);
    bool all_done = false;
    self
      ->do_receive(
        [&](const table_slice& slice) {
          py::print("received", slice.layout().name(), "with", slice.rows(),
                    "events");
        },
        [&, index = index](atom::done) {
          if (partitions > scheduled) {
            auto next_batch_size = std::min(partitions - scheduled, 2u);
            VAST_DEBUG("client command requests next batch of {} partitions",
                       next_batch_size);
            self->send(index, id, next_batch_size);
            scheduled += next_batch_size;
          } else {
            VAST_DEBUG("client command finished receiving data");
            all_done = true;
          }
        })
      .until([&] {
        return all_done;
      });
  }

  configuration cfg = {};
  caf::actor_system system{cfg};
  caf::scoped_actor self{system};
  node_actor node = {};

  std::string endpoint;
};

} // namespace vast::system

// NOLINTNEXTLINE
PYBIND11_MODULE(pyvast, m) {
  m.doc() = "pybind11 pyvast plugin"; // optional module docstring
  py::class_<vast::system::Node>(m, "Node")
    .def(py::init<std::string>())
    .def(
      "status", &vast::system::Node::status,
      py::call_guard<py::scoped_ostream_redirect, py::scoped_estream_redirect>())
    .def("export", &vast::system::Node::export_,
         py::call_guard<py::scoped_ostream_redirect,
                        py::scoped_estream_redirect>());
}
