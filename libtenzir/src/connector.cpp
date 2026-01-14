//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/connector.hpp"

#include "tenzir/connect_request.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/logger.hpp"

#include <caf/io/middleman.hpp>
#include <caf/io/network/interfaces.hpp>
#include <caf/openssl/all.hpp>
#include <caf/sec.hpp>
#include <fmt/format.h>

#include <optional>

namespace tenzir {

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
    case caf::sec::invalid_stream:
    case caf::sec::cannot_resubscribe_stream:
    case caf::sec::stream_aborted:
    case caf::sec::bad_function_call:
    case caf::sec::feature_disabled:
    case caf::sec::cannot_open_file:
    case caf::sec::socket_invalid:
    case caf::sec::socket_disconnected:
    case caf::sec::socket_operation_failed:
    case caf::sec::unavailable_or_would_block:
    case caf::sec::malformed_message:
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
    case caf::sec::invalid_observable:
    case caf::sec::too_many_observers:
    case caf::sec::disposed:
    case caf::sec::cannot_open_resource:
    case caf::sec::protocol_error:
    case caf::sec::logic_error:
    case caf::sec::invalid_delegate:
    case caf::sec::invalid_request:
    case caf::sec::future_timeout:
    case caf::sec::invalid_utf8:
    case caf::sec::backpressure_overflow:
    case caf::sec::too_many_worker_failures:
    case caf::sec::cannot_combine_empty_observables:
    case caf::sec::mail_cache_closed:
    case caf::sec::resource_destroyed:
      return true;
    case caf::sec::incompatible_versions:
    case caf::sec::incompatible_application_ids:
    case caf::sec::request_timeout:
      return false;
  }
  return false;
}

bool is_recoverable_error(const caf::error& err) {
  if (err.category() != caf::type_id_v<caf::sec>) {
    return true;
  }
  const auto err_code = std::underlying_type_t<caf::sec>{err.code()};
  auto err_enum = caf::sec{caf::sec::none};
  if (not caf::from_integer(err_code, err_enum)) {
    TENZIR_WARN("unable to retrieve error code for a remote node connection "
                "error:{}",
                err);
    return true;
  }
  return is_recoverable_error_enum(err_enum);
}

std::optional<caf::timespan> calculate_remaining_time(
  const std::optional<std::chrono::steady_clock::time_point>& deadline) {
  if (not deadline) {
    return caf::infinite;
  }
  const auto now = std::chrono::steady_clock::now();
  if (now >= *deadline) {
    return std::nullopt;
  }
  return *deadline - now;
}

bool should_retry(const caf::error& err,
                  const std::optional<caf::timespan>& remaining_time,
                  caf::timespan delay) {
  return remaining_time && *remaining_time > delay && is_recoverable_error(err);
}

std::string format_time(caf::timespan timespan) {
  if (caf::is_infinite(timespan)) {
    return "infinite";
  }
  return fmt::to_string(data{timespan});
}

void log_connection_failed(connect_request request, caf::error err,
                           caf::timespan remaining_time,
                           caf::timespan retry_delay) {
  TENZIR_WARN(
    "client failed to connect to remote node {}:{}{}: {}; attempting to "
    "reconnect in {} (remaining time: {})",
    request.host, request.port, formatted_resolved_host_suffix(request.host),
    err, format_time(retry_delay), format_time(remaining_time));
}

connector_actor::behavior_type make_no_retry_behavior(
  connector_actor::stateful_pointer<connector_state> self,
  std::optional<std::chrono::steady_clock::time_point> deadline,
  bool internal_connection) {
  auto log_level
    = internal_connection ? spdlog::level::trace : spdlog::level::info;
  return {
    [self, deadline, log_level](
      atom::connect, connect_request request) -> caf::result<node_actor> {
      const auto remaining_time = calculate_remaining_time(deadline);
      if (not remaining_time) {
        return caf::make_error(ec::timeout,
                               fmt::format("{} couldn't connect to node"
                                           "within a given deadline",
                                           *self));
      }
      auto rp = self->make_response_promise<node_actor>();
      self->mail(caf::connect_atom_v, request.host, request.port)
        .request(self->state().middleman, *remaining_time)
        .then(
          [rp, request, log_level](const caf::node_id&,
                                   caf::strong_actor_ptr& node,
                                   const std::set<std::string>&) mutable {
            detail::logger()->log(log_level,
                                  "client connected to node at {}:{}",
                                  request.host, request.port);
            rp.deliver(caf::actor_cast<node_actor>(std::move(node)));
          },
          [rp, request](caf::error& err) mutable {
            rp.deliver(caf::make_error(
              ec::system_error,
              fmt::format("failed to connect to node at {}:{}: {}",
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
          std::optional<std::chrono::steady_clock::time_point> deadline,
          bool internal_connection) {
  self->state().middleman = self->system().has_openssl_manager()
                              ? self->system().openssl_manager().actor_handle()
                              : self->system().middleman().actor_handle();
  if (not retry_delay) {
    return make_no_retry_behavior(self, deadline, internal_connection);
  }
  auto log_level
    = internal_connection ? spdlog::level::trace : spdlog::level::info;
  return {
    [self, delay = *retry_delay, deadline, log_level](
      atom::connect, connect_request request) -> caf::result<node_actor> {
      const auto remaining_time = calculate_remaining_time(deadline);
      if (not remaining_time) {
        return caf::make_error(ec::timeout,
                               fmt::format("{} couldn't connect to node "
                                           "within a given deadline",
                                           *self));
      }
      detail::logger()->log(log_level, "client connects to {}:{}{}",
                            request.host, request.port,
                            formatted_resolved_host_suffix(request.host));
      auto rp = self->make_response_promise<node_actor>();
      auto handle_error = [self, rp, request, delay,
                           deadline](const caf::error& err) mutable {
        const auto remaining_time = calculate_remaining_time(deadline);
        if (should_retry(err, remaining_time, delay)) {
          log_connection_failed(request, err, *remaining_time, delay);
          detail::weak_run_delayed(self, delay, [self, rp, request]() mutable {
            rp.delegate(static_cast<connector_actor>(self), atom::connect_v,
                        std::move(request));
          });
        } else {
          rp.deliver(caf::make_error(
            ec::system_error,
            fmt::format("failed to connect to node at {}:{}: {}", request.host,
                        request.port, err)));
        }
      };

      self->mail(caf::connect_atom_v, request.host, request.port)
        .request(self->state().middleman, *remaining_time)
        .then(
          [rp, request, handle_error,
           log_level](const caf::node_id&, caf::strong_actor_ptr& node,
                      const std::set<std::string>&) mutable {
            if (node) {
              detail::logger()->log(log_level,
                                    "client connected to node at {}:{}",
                                    request.host, request.port);
              rp.deliver(caf::actor_cast<node_actor>(std::move(node)));
              return;
            }
            handle_error(caf::make_error(
              ec::system_error, "failed to connect to node: invalid handle"));
          },
          handle_error);
      return rp;
    },
  };
}

} // namespace tenzir
