//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/fwd.hpp"

#include "vast/command.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/connect_to_node.hpp"
#include "vast/system/remote_command.hpp"

#include <caf/actor_system.hpp>
#include <caf/scoped_actor.hpp>

#if VAST_ENABLE_OPENSSL
#  include <caf/openssl/all.hpp>
#endif

#include <pybind11/iostream.h>
#include <pybind11/pybind11.h>

#include <stdexcept>

namespace py = pybind11;

struct Node {
  Node(std::string endpoint) : endpoint(std::move(endpoint)) {
    auto maybe_node_actor = vast::system::connect_to_node(self, content(cfg));
    if (!maybe_node_actor) {
      throw std::domain_error{"failed to connect"};
    }
    this->node_actor = *maybe_node_actor;
  }

  void status() {
    auto inv = vast::invocation{caf::settings{}, "status", {}};
    auto result = vast::system::remote_command(inv, system);
  }

  vast::system::configuration cfg = {};
  caf::actor_system system{cfg};
  caf::scoped_actor self{system};
  vast::system::node_actor node_actor = {};

  std::string endpoint;
};

// NOLINTNEXTLINE
PYBIND11_MODULE(pyvast, m) {
  m.doc() = "pybind11 pyvast plugin"; // optional module docstring
  py::class_<Node>(m, "Node")
    .def(py::init<std::string>())
    .def("status", &Node::status,
         py::call_guard<py::scoped_ostream_redirect,
                        py::scoped_estream_redirect>());
}
