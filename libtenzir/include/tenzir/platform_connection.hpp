// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/arc.hpp>
#include <tenzir/async/mutex.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/controller/generated/api.hpp>
#include <tenzir/controller/transport.hpp>
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>

#include <string>

namespace tenzir {

/// Who a deployment is on the platform, which nests every route of a
/// deployment under its organization and its workspace.
struct DeploymentIdentity {
  OrganizationId org_id;
  WorkspaceId workspace_id;
  DeploymentId deployment_id;

  /// The path of the generated calls that address this deployment.
  auto path() const -> DeploymentPath;

  /// `/organizations/<org>/workspaces/<workspace>/deployments/<id>`, with each
  /// segment percent-escaped, for the routes that are built by hand.
  auto route() const -> std::string;
};

/// Asks the platform whose key `transport` sends, and accepts only deployment
/// `expected`: a key of another deployment, of a user, or of nobody is an
/// error that says so.
auto read_deployment_identity(Transport& transport,
                              DeploymentId const& expected)
  -> Task<Result<DeploymentIdentity, std::string>>;

struct PlatformConnection {
  /// A connection that knows its identity already and never asks for it, for
  /// talking through a transport that answers only what a test is about.
  static auto confirmed_as(DeploymentIdentity identity) -> PlatformConnection;

  std::string platform_url;
  std::string deployment_id;
  std::string key_file;

  /// The deployment's identity, asked of the platform the first time it is
  /// needed, over the API that discovery finds. Every copy of this connection
  /// remembers it once the platform confirmed it. A failed lookup is not
  /// remembered, so that the next one asks again: an unreachable platform or a
  /// key that is about to be rotated must not leave the deployment without one.
  auto identity() const -> Task<Result<DeploymentIdentity, std::string>>;

  /// The same, asked over `transport` if it is not known yet.
  auto identity(Transport& transport) const
    -> Task<Result<DeploymentIdentity, std::string>>;

  /// Shared by all copies. Not a queue, because every user needs the one
  /// answer, whoever asked first. Nothing holds it across a lookup, so users
  /// that start at once may each ask; they all get the same answer.
  mutable Arc<Mutex<Option<DeploymentIdentity>>> confirmed{std::in_place};
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
