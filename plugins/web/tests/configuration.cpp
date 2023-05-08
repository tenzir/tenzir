//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/configuration.hpp"

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/plugin.hpp>
#include <vast/test/test.hpp>
#include <vast/validate.hpp>

namespace {

// TODO: Build a test that uses convert() to go form `caf::settings`
// to `vast::record`, to make the logic closer to what's actually
// used in the plugin code.

vast::record extract_config(const std::string& config) {
  auto data = vast::from_yaml(config);
  REQUIRE_NOERROR(data);
  REQUIRE(caf::holds_alternative<vast::record>(*data));
  auto inner = caf::get<vast::record>(*data).at("web");
  REQUIRE(caf::holds_alternative<vast::record>(inner));
  return caf::get<vast::record>(inner);
}

} // namespace

TEST(dev mode config validation) {
  auto record = extract_config(R"_(
web:
  bind: localhost
  port: +8000
  mode: dev
  )_");
  auto config = vast::plugins::web::configuration{};
  VAST_INFO("{}", record);
  CHECK_EQUAL(validate(record, vast::plugins::web::configuration::schema(),
                       vast::validate::strict),
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
  auto invalid_config = vast::plugins::web::configuration{};
  CHECK_ERROR(convert(invalid_data, invalid_config));
  auto invalid_data2 = extract_config(R"_(
web:
  # Invalid mode
  mode: depeche
  bind: 127.0.0.1
  port: 8000
  )_");
  auto invalid_config2 = vast::plugins::web::configuration{};
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
  auto config = vast::plugins::web::configuration{};
  CHECK_EQUAL(validate(data, vast::plugins::web::configuration::schema(),
                       vast::validate::strict),
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
  auto invalid_config = vast::plugins::web::configuration{};
  CHECK_ERROR(convert(invalid_data, invalid_config));
}
