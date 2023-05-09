//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/connector.hpp"

#include "vast/data.hpp"
#include "vast/detail/weak_run_delayed.hpp"
#include "vast/logger.hpp"
#include "vast/system/connect_request.hpp"

#include <caf/io/middleman.hpp>
#include <caf/io/network/interfaces.hpp>
#include <caf/openssl/all.hpp>
#include <fmt/format.h>

#include <optional>

namespace vast::system {

namespace {

auto formatted_resolved_host_suffix(const std::string& host) -> std::string {
  const auto resolved_host
    = caf::io::network::interfaces::native_address(host)->first;
  if (std::equal(host.begin(), host.end(), resolved_host.begin(),
                 resolved_host.end(), [](char lhs, char rhs) {
                   return std::tolower(lhs) == std::tolower(rhs);
                 })) {
    return {};
  }
  return fmt::format(" ({})", resolved_host);
}

bool is_recoverable_error_enum(caf::sec err_enum) {
  switch (err_enum) {
    case caf::sec::none:
    case caf::sec::unexpected_message:
    case caf::sec::unexpected_response:
    case caf::sec::request_receiver_down:
    case caf::sec::no_such_group_module:
    case caf::sec::no_actor_published_at_port:
    case caf::sec::unexpected_actor_messaging_interface:
    case caf::sec::state_not_serializable:
    case caf::sec::unsupported_sys_key:
    case caf::sec::unsupported_sys_message:
    case caf::sec::disconnect_during_handshake:
    case caf::sec::cannot_forward_to_invalid_actor:
    case caf::sec::no_route_to_receiving_node:
    case caf::sec::failed_to_assign_scribe_from_handle:
    case caf::sec::failed_to_assign_doorman_from_handle:
    case caf::sec::cannot_close_invalid_port:
    case caf::sec::cannot_connect_to_node:
    case caf::sec::cannot_open_port:
    case caf::sec::network_syscall_failed:
    case caf::sec::invalid_argument:
    case caf::sec::invalid_protocol_family:
    case caf::sec::cannot_publish_invalid_actor:
    case caf::sec::cannot_spawn_actor_from_arguments:
    case caf::sec::end_of_stream:
    case caf::sec::no_context:
    case caf::sec::unknown_type:
    case caf::sec::no_proxy_registry:
    case caf::sec::runtime_error:
    case caf::sec::remote_linking_failed:
    case caf::sec::cannot_add_upstream:
    case caf::sec::upstream_already_exists:
    case caf::sec::invalid_upstream:
    case caf::sec::cannot_add_downstream:
    case caf::sec::downstream_already_exists:
    case caf::sec::invalid_downstream:
    case caf::sec::no_downstream_stages_defined:
    case caf::sec::stream_init_failed:
    case caf::sec::invalid_stream_state:
    case caf::sec::unhandled_stream_error:
    case caf::sec::bad_function_call:
    case caf::sec::feature_disabled:
    case caf::sec::cannot_open_file:
    case caf::sec::socket_invalid:
    case caf::sec::socket_disconnected:
    case caf::sec::socket_operation_failed:
    case caf::sec::unavailable_or_would_block:
    case caf::sec::malformed_basp_message:
    case caf::sec::serializing_basp_payload_failed:
    case caf::sec::redundant_connection:
    case caf::sec::remote_lookup_failed:
    case caf::sec::no_tracing_context:
    case caf::sec::all_requests_failed:
    case caf::sec::field_invariant_check_failed:
    case caf::sec::field_value_synchronization_failed:
    case caf::sec::invalid_field_type:
    case caf::sec::unsafe_type:
    case caf::sec::save_callback_failed:
    case caf::sec::load_callback_failed:
    case caf::sec::conversion_failed:
    case caf::sec::connection_closed:
    case caf::sec::type_clash:
    case caf::sec::unsupported_operation:
    case caf::sec::no_such_key:
    case caf::sec::broken_promise:
    case caf::sec::connection_timeout:
    case caf::sec::action_reschedule_failed:
      return true;
    case caf::sec::incompatible_versions:
    case caf::sec::incompatible_application_ids:
    case caf::sec::request_timeout:
      return false;
  }
  return false;
}

bool is_recoverable_error(const caf::error& err) {
  if (err.category() != caf::type_id_v<caf::sec>)
    return true;
  const auto err_code = std::underlying_type_t<caf::sec>{err.code()};
  auto err_enum = caf::sec{caf::sec::none};
  if (!caf::from_integer(err_code, err_enum)) {
    VAST_WARN("unable to retrieve error code for a remote node connection "
              "error:{}",
              err);
    return true;
  }
  return is_recoverable_error_enum(err_enum);
}

std::optional<caf::timespan> calculate_remaining_time(
  const std::optional<std::chrono::steady_clock::time_point>& deadline) {
  if (!deadline)
    return caf::infinite;
  const auto now = std::chrono::steady_clock::now();
  if (now >= *deadline)
    return std::nullopt;
  return *deadline - now;
}

bool should_retry(const caf::error& err,
                  const std::optional<caf::timespan>& remaining_time,
                  caf::timespan delay) {
  return remaining_time && *remaining_time > delay && is_recoverable_error(err);
}

std::string format_time(caf::timespan timespan) {
  if (caf::is_infinite(timespan))
    return "infinite";
  return fmt::to_string(data{timespan});
}

void log_connection_failed(connect_request request,
                           caf::timespan remaining_time,
                           caf::timespan retry_delay) {
  VAST_INFO("client faild to connect to remote node {}:{}{}; attempting to "
            "reconnect in {} (remaining time: {})",
            request.host, request.port,
            formatted_resolved_host_suffix(request.host),
            format_time(retry_delay), format_time(remaining_time));
}

connector_actor::behavior_type make_no_retry_behavior(
  connector_actor::stateful_pointer<connector_state> self,
  std::optional<std::chrono::steady_clock::time_point> deadline) {
  return {
    [self, deadline](atom::connect,
                     connect_request request) -> caf::result<node_actor> {
      const auto remaining_time = calculate_remaining_time(deadline);
      if (!remaining_time)
        return caf::make_error(ec::timeout,
                               fmt::format("{} couldn't connect to VAST node"
                                           "within a given deadline",
                                           *self));
      auto rp = self->make_response_promise<node_actor>();
      self
        ->request(self->state.middleman, *remaining_time, caf::connect_atom_v,
                  request.host, request.port)
        .then(
          [rp, request](const caf::node_id&, caf::strong_actor_ptr& node,
                        const std::set<std::string>&) mutable {
            VAST_INFO("client connected to VAST node at {}:{}", request.host,
                      request.port);
            rp.deliver(caf::actor_cast<node_actor>(std::move(node)));
          },
          [rp, request](caf::error& err) mutable {
            rp.deliver(caf::make_error(
              ec::system_error,
              fmt::format("failed to connect to VAST node at {}:{}: {}",
                          request.host, request.port, std::move(err))));
          });
      return rp;
    },
  };
}

} // namespace

connector_actor::behavior_type
connector(connector_actor::stateful_pointer<connector_state> self,
          std::optional<caf::timespan> retry_delay,
          std::optional<std::chrono::steady_clock::time_point> deadline) {
  self->state.middleman = self->system().has_openssl_manager()
                            ? self->system().openssl_manager().actor_handle()
                            : self->system().middleman().actor_handle();
  if (!retry_delay)
    return make_no_retry_behavior(std::move(self), deadline);
  return {
    [self, delay = *retry_delay, deadline](
      atom::connect, connect_request request) -> caf::result<node_actor> {
      const auto remaining_time = calculate_remaining_time(deadline);
      if (!remaining_time)
        return caf::make_error(ec::timeout,
                               fmt::format("{} couldn't connect to VAST node "
                                           "within a given deadline",
                                           *self));
      VAST_INFO("client connects to {}:{}{}", request.host, request.port,
                formatted_resolved_host_suffix(request.host));
      auto rp = self->make_response_promise<node_actor>();
      self
        ->request(self->state.middleman, *remaining_time, caf::connect_atom_v,
                  request.host, request.port)
        .then(
          [rp, req = request](const caf::node_id&, caf::strong_actor_ptr& node,
                              const std::set<std::string>&) mutable {
            VAST_INFO("client connected to VAST node at {}:{}", req.host,
                      req.port);
            rp.deliver(caf::actor_cast<node_actor>(std::move(node)));
          },
          [self, rp, request, delay, deadline](caf::error& err) mutable {
            const auto remaining_time = calculate_remaining_time(deadline);
            if (should_retry(err, remaining_time, delay)) {
              log_connection_failed(request, *remaining_time, delay);
              detail::weak_run_delayed(
                self, delay, [self, rp, request]() mutable {
                  rp.delegate(static_cast<connector_actor>(self),
                              atom::connect_v, std::move(request));
                });
            } else
              rp.deliver(caf::make_error(
                ec::system_error,
                fmt::format("failed to connect to VAST node at {}:{}: {}",
                            request.host, request.port, std::move(err))));
          });
      return rp;
    },
  };
}

} // namespace vast::system
