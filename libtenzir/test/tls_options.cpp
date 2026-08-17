//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tls_options.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <caf/actor_system_config.hpp>

#include <filesystem>
#include <fstream>
#include <tuple>

using namespace tenzir;

TEST("TLS enablement can come from node config") {
  auto cfg = caf::actor_system_config{};
  caf::put(cfg.content, "tenzir.tls.enable", true);
  auto opts = tls_options{{.tls_default = false, .is_server = true}};
  auto dh = collecting_diagnostic_handler{};
  auto resolved = opts.resolve(cfg, dh);
  REQUIRE(resolved);
  REQUIRE(dh.empty());
  CHECK(resolved->tls.inner);
}

TEST("mTLS client CA can come from node config") {
  auto path = std::filesystem::temp_directory_path()
              / fmt::format("tenzir-tls-options-{}.pem", uuid::random());
  auto file = std::ofstream{path};
  file << "test CA";
  file.close();
  auto cfg = caf::actor_system_config{};
  caf::put(cfg.content, "tenzir.tls.tls-client-ca", path.string());
  auto opts = tls_options{
    located{data{record{{"require_client_cert", true}}}, location::unknown},
    {.is_server = true},
  };
  auto dh = collecting_diagnostic_handler{};
  auto resolved = opts.resolve(cfg, dh);
  std::ignore = std::filesystem::remove(path);
  REQUIRE(resolved);
  REQUIRE(dh.empty());
  REQUIRE(resolved->tls_client_ca);
  CHECK_EQUAL(resolved->tls_client_ca->inner, path.string());
}
