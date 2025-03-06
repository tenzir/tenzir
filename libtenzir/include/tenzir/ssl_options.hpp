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

  std::optional<located<bool>> tls;
  std::optional<location> skip_peer_verification;
  std::optional<location> skip_hostname_verification;
  std::optional<located<std::string>> cacert;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> keyfile;

  auto set_fallback_cacert(std::string_view) -> void;
  auto add_tls_options(argument_parser2&) -> void;
  auto validate(diagnostic_handler&) -> failure_or<void>;
  auto apply_to(curl::easy& easy) const -> caf::error;
};

} // namespace tenzir
