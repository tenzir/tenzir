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
  ssl_options( std::string_view default_cacert );

  located<std::string> url;
  std::optional<located<bool>> tls;
  std::optional<location> skip_peer_verification;
  std::optional<located<std::string>> cacert;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> keyfile;

  auto add_url_option( argument_parser2&, bool positional ) -> void;
  auto add_tls_options( argument_parser2& ) -> void;
  auto validate( diagnostic_handler& ) -> failure_or<void>;
  auto apply_to( curl::easy& easy, diagnostic_handler& dh ) -> bool;
};
}
