//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/data.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/location.hpp>
#include <tenzir/option.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/tls_options.hpp>

#include <folly/io/async/EventBase.h>
#include <nats/nats.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::nats {

struct nats_options_deleter {
  auto operator()(natsOptions* ptr) const noexcept -> void {
    if (ptr) {
      natsOptions_Destroy(ptr);
    }
  }
};

struct nats_connection_deleter {
  auto operator()(natsConnection* ptr) const noexcept -> void {
    if (ptr) {
      natsConnection_Destroy(ptr);
    }
  }
};

struct js_ctx_deleter {
  auto operator()(jsCtx* ptr) const noexcept -> void {
    if (ptr) {
      jsCtx_Destroy(ptr);
    }
  }
};

struct nats_subscription_deleter {
  auto operator()(natsSubscription* ptr) const noexcept -> void {
    if (ptr) {
      natsSubscription_Destroy(ptr);
    }
  }
};

struct nats_msg_deleter {
  auto operator()(natsMsg* ptr) const noexcept -> void {
    if (ptr) {
      natsMsg_Destroy(ptr);
    }
  }
};

struct js_msg_meta_data_deleter {
  auto operator()(jsMsgMetaData* ptr) const noexcept -> void {
    if (ptr) {
      jsMsgMetaData_Destroy(ptr);
    }
  }
};

using nats_options_ptr = std::unique_ptr<natsOptions, nats_options_deleter>;
using nats_connection_ptr
  = std::unique_ptr<natsConnection, nats_connection_deleter>;
using js_ctx_ptr = std::unique_ptr<jsCtx, js_ctx_deleter>;
using nats_subscription_ptr
  = std::unique_ptr<natsSubscription, nats_subscription_deleter>;
using nats_msg_ptr = std::unique_ptr<natsMsg, nats_msg_deleter>;
using js_msg_meta_data_ptr
  = std::unique_ptr<jsMsgMetaData, js_msg_meta_data_deleter>;

struct auth_config {
  Option<std::string> user;
  Option<std::string> password;
  Option<std::string> token;
  Option<std::string> credentials;
  Option<std::string> seed;
  Option<std::string> credentials_memory;
};

struct connection_config {
  std::string url;
  auth_config auth;
};

auto default_url() -> secret&;
auto set_default_url_from_global_config(record const& global_config,
                                        std::string_view plugin_name)
  -> caf::error;

auto has_url_scheme(std::string_view url) -> bool;
auto tls_enabled_from_url(std::string_view url) -> bool;
auto normalize_url(std::string url, bool tls_enabled) -> std::string;

auto nats_status_string(natsStatus status) -> std::string;
auto nats_last_error_string(natsStatus fallback) -> std::string;
auto emit_nats_error(diagnostic_builder diag, natsStatus status,
                     diagnostic_handler& dh) -> void;

auto validate_auth_record(Option<located<data>> const& auth,
                          diagnostic_handler& dh) -> failure_or<void>;
auto apply_auth(natsOptions* options, auth_config const& auth,
                location auth_location, diagnostic_handler& dh)
  -> failure_or<void>;

auto apply_tls(natsOptions* options, tls_options const& tls,
               diagnostic_handler& dh) -> failure_or<void>;

auto make_nats_options(connection_config const& config,
                       Option<located<data>> const& tls_arg,
                       location url_location, diagnostic_handler& dh,
                       folly::EventBase& event_base)
  -> failure_or<nats_options_ptr>;

auto configure_folly_event_loop(natsOptions* options,
                                folly::EventBase& event_base) -> natsStatus;
auto initialize_folly_event_loop_adapter() -> void;

} // namespace tenzir::plugins::nats
