//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "web/configuration.hpp"

#include <restinio/all.hpp>
#include <restinio/tls.hpp>

namespace vast::plugins::web {

using router_t = restinio::router::express_router_t<>;

/// Traits class for the dev server.
struct dev_traits_t : public restinio::default_single_thread_traits_t {
  using request_handler_t = restinio::router::express_router_t<>;
};

/// The dev server class.
using dev_server_t = restinio::http_server_t<dev_traits_t>;

/// Traits class for the TLS server.
using tls_traits_t = restinio::single_thread_tls_traits_t<
  restinio::asio_timer_manager_t, restinio::single_threaded_ostream_logger_t,
  restinio::router::express_router_t<>>;

/// The TLS server class.
using tls_server_t = restinio::http_server_t<tls_traits_t>;

/// A class representing either a dev or a TLS server.
// Need to work with pointers since `restinio::http_server_t` is immovable.
using server_t
  = std::variant<std::unique_ptr<tls_server_t>, std::unique_ptr<dev_server_t>>;

server_t make_server(server_config config, std::unique_ptr<router_t> router,
                     restinio::io_context_holder_t io_context);

} // namespace vast::plugins::web
