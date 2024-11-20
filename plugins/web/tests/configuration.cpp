//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/configuration.hpp"

#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/validate.hpp>

namespace {

// TODO: Build a test that uses convert() to go form `caf::settings`
// to `tenzir::record`, to make the logic closer to what's actually
// used in the plugin code.

tenzir::record extract_config(const std::string& config) {
  auto data = tenzir::from_yaml(config);
  REQUIRE_NOERROR(data);
  REQUIRE(is<tenzir::record>(*data));
  auto inner = as<tenzir::record>(*data).at("web");
  REQUIRE(is<tenzir::record>(inner));
  return as<tenzir::record>(inner);
}

} // namespace

TEST(dev mode config validation) {
  auto record = extract_config(R"_(
web:
  bind: localhost
  port: +8000
  mode: dev
  )_");
  auto config = tenzir::plugins::web::configuration{};
  TENZIR_INFO("{}", record);
  CHECK_EQUAL(validate(record, tenzir::plugins::web::configuration::schema(),
                       tenzir::validate::strict),
              caf::error{});
  REQUIRE_EQUAL(convert(record, config), caf::error{});
  CHECK_EQUAL(config.bind_address, "localhost");
  CHECK_EQUAL(config.port, 8000);
  CHECK_EQUAL(config.mode, "dev");
  auto server_config = convert_and_validate(config);
  REQUIRE_NOERROR(server_config);
  CHECK_EQUAL(server_config->bind_address, "localhost");
  CHECK_EQUAL(server_config->port, 8000);
  CHECK_EQUAL(server_config->require_tls, false);
  CHECK_EQUAL(server_config->require_localhost, false);
  CHECK_EQUAL(server_config->require_clientcerts, false);
  CHECK_EQUAL(server_config->require_authentication, false);
  auto invalid_data = extract_config(R"_(
web:
  # Attempting to bind to non-local IP
  bind: 0.0.0.0
  port: 8000
  mode: dev
  )_");
  auto invalid_config = tenzir::plugins::web::configuration{};
  CHECK_ERROR(convert(invalid_data, invalid_config));
  auto invalid_data2 = extract_config(R"_(
web:
  # Invalid mode
  mode: depeche
  bind: 127.0.0.1
  port: 8000
  )_");
  auto invalid_config2 = tenzir::plugins::web::configuration{};
  CHECK_ERROR(convert(invalid_data2, invalid_config2));
}

TEST(tls mode config validation) {
  auto data = extract_config(R"_(
web:
  bind: localhost
  port: +443
  mode: server
  certfile: server.pem
  keyfile: server.key
  )_");
  auto config = tenzir::plugins::web::configuration{};
  CHECK_EQUAL(validate(data, tenzir::plugins::web::configuration::schema(),
                       tenzir::validate::strict),
              caf::error{});
  REQUIRE_EQUAL(convert(data, config), caf::error{});
  CHECK_EQUAL(config.bind_address, "localhost");
  CHECK_EQUAL(config.port, 443);
  CHECK_EQUAL(config.mode, "server");
  // TODO: Create temporary files for `server.pem` and `server.key`
  // so we can check that `convert_and_validate()` works.
  auto invalid_data = extract_config(R"_(
web:
  bind: 0.0.0.0
  port: 443
  mode: server
  certfile: server.pem
  # Missing 'keyfile'
  )_");
  auto invalid_config = tenzir::plugins::web::configuration{};
  CHECK_ERROR(convert(invalid_data, invalid_config));
}
