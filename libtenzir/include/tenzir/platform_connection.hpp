// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/controller/transport.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>

#include <string>

namespace tenzir {

struct PlatformConnection {
  std::string platform_url;
  std::string deployment_id;
  std::string key_file;
};

/// The platform this process runs a deployment for, if it runs one.
///
/// The deployment sets it once it knows, so that operators that call the
/// platform on its behalf, such as `ingest`, need not get it through the
/// pipeline.
auto deployment_platform() -> Option<PlatformConnection>;
auto set_deployment_platform(Option<PlatformConnection> connection) -> void;

/// Reads the deployment key, refusing anything that is not one line of text.
auto read_deployment_key(std::string const& path) -> Option<std::string>;

/// Opens the platform's API with the deployment key, for the generated calls.
auto connect_platform_api(PlatformConnection const& connection)
  -> Task<Result<Box<Transport>, std::string>>;

} // namespace tenzir
