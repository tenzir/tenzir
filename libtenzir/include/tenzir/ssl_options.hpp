//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/argument_parser2.hpp"
#include "tenzir/curl.hpp"

namespace tenzir {

struct ssl_options {
  ssl_options();

  located<bool> tls = located{true, location::unknown};
  std::optional<location> skip_peer_verification;
  std::optional<location> skip_hostname_verification;
  std::optional<located<std::string>> cacert;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> keyfile;

  auto add_tls_options(argument_parser2&) -> void;
  auto validate(const located<std::string>& url,
                diagnostic_handler&) -> failure_or<void>;
  /// Applies the options to a `curl::easy` object, potentially using
  /// `cacert_fallback`, if none is set explicitly.
  auto apply_to(curl::easy& easy, std::string_view cacert_fallback
                                  = {}) const -> caf::error;
  /// Applies the options to a `curl::easy` object, potentially getting
  /// `tenzir.cacert` as a `cacert_fallbacl` if none is set explicitly.
  auto apply_to(curl::easy& easy, operator_control_plane&) const -> caf::error;

  /// Queries `tenzir.cacert` from the config.
  static auto
  query_cacert_fallback(operator_control_plane& ctrl) -> std::string;

  /// updates this->cacert to `tenzir.cacert` from the config, if it is not
  /// already set.
  auto update_cacert(operator_control_plane& ctrl) -> void;

  friend auto inspect(auto& f, ssl_options& x) -> bool {
    return f.object(x).fields(
      f.field("tls", x.tls),
      f.field("skip_peer_verification", x.skip_peer_verification),
      f.field("skip_hostname_verification", x.skip_hostname_verification),
      f.field("cacert", x.cacert), f.field("certfile", x.certfile),
      f.field("keyfile", x.keyfile));
  }
};

} // namespace tenzir
