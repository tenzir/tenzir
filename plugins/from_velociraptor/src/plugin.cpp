//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/uuid.hpp>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include "velociraptor.grpc.pb.h"
#include "velociraptor.pb.h"

namespace tenzir::plugins::velociraptor {

using namespace std::chrono_literals;

namespace {

/// The ID of an Organization.
constexpr auto default_org_id = "root";

/// The maximum number of rows per response.
constexpr auto default_max_rows = uint64_t{1'000};

/// The number of seconds to wait on responses.
constexpr auto default_max_wait = std::chrono::seconds{1};

/// A VQL request.
struct request {
  std::string name;
  std::string vql;

  friend auto inspect(auto& f, request& x) -> bool {
    return f.object(x).pretty_name("request").fields(f.field("name", x.name),
                                                     f.field("vql", x.vql));
  }
};

/// The arguments passed to the operator.
struct operator_args {
  uint64_t max_rows;
  std::chrono::seconds max_wait;
  std::string org_id;
  std::vector<request> requests;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("max_rows", x.max_rows), f.field("max_wait", x.max_wait),
              f.field("org_id", x.org_id), f.field("requests", x.requests));
  }
};

/// Christoph Lobmeyer (https://github.com/lo-chr) devised this query and
/// provided the use case to subscribe to a specific set of artifacts from
/// multiple clients.
constexpr auto subscribe_artifact_vql = R"__(
LET subscribe_artifact = "{}"

LET completions = SELECT *
                  FROM watch_monitoring(artifact="System.Flow.Completion")
                  WHERE Flow.artifacts_with_results =~ subscribe_artifact

SELECT *
FROM foreach(
  row=completions,
  query={{
     SELECT *
     FROM foreach(
       row=Flow.artifacts_with_results,
       query={{
         SELECT *
         FROM if(
          condition=(_value =~ subscribe_artifact),
          then={{
             SELECT
               {{
                 SELECT *
                 FROM source(
                   client_id=ClientId,
                   flow_id=Flow.session_id,
                   artifact=_value)
               }} AS HuntResult,
               _value AS Artifact,
               client_info(client_id=ClientId).os_info.hostname AS Hostname,
               timestamp(epoch=now()) AS timestamp,
               ClientId,
               Flow.session_id AS FlowId
             FROM source(
               client_id=ClientId,
               flow_id=Flow.session_id,
               artifact=_value)
             GROUP BY
               artifact
          }})
        }})
  }})
)__";

auto make_subscribe_query(std::string_view artifact) -> std::string {
  return fmt::format(subscribe_artifact_vql, artifact);
}

/// Parses a response as table slice.
auto parse(const proto::VQLResponse& response)
  -> caf::expected<std::vector<table_slice>> {
  auto builder = series_builder{};
  auto us = std::chrono::microseconds(response.timestamp());
  auto timestamp = time{std::chrono::duration_cast<duration>(us)};
  // Velociraptor sends a stream of responses that consists of "control"
  // and "data" messages. If the response payload is empty, then we have a
  // control message, otherwise we have a data message.
  if (not response.response().empty()) {
    TENZIR_DEBUG("got a data message");
    // There's an opportunity for improvement here, as we are not (yet)
    // making use of the additional types provided in the response. We
    // should synthesize a schema from that and provide that as hint to
    // the series builder.
    auto json = from_json(response.response());
    if (not json) {
      return caf::make_error(ec::parse_error,
                             "Velociraptor response not in JSON format");
    }
    const auto* objects = try_as<list>(&*json);
    if (objects == nullptr) {
      return caf::make_error(ec::parse_error,
                             "expected JSON array in Velociraptor response");
    }
    for (const auto& object : *objects) {
      const auto* rec = try_as<record>(&object);
      if (rec == nullptr) {
        return caf::make_error(ec::parse_error,
                               "expected objects in Velociraptor response");
      }
      auto row = builder.record();
      row.field("timestamp").data(timestamp);
      row.field("query_id").data(response.query_id());
      row.field("query").data(record{
        {"name", response.query().name()},
        {"vql", response.query().vql()},
      });
      row.field("part").data(response.part());
      auto resp = row.field("response").record();
      for (const auto& [field, value] : *rec) {
        resp.field(field).data(make_view(value));
      }
    }
    return builder.finish_as_table_slice("velociraptor.response");
  }
  if (not response.log().empty()) {
    TENZIR_DEBUG("got a control message");
    auto row = builder.record();
    row.field("timestamp").data(timestamp);
    row.field("log").data(response.log());
    return builder.finish_as_table_slice("velociraptor.log");
  }
  return caf::make_error(ec::unspecified, "empty Velociraptor response");
}

class velociraptor_operator final
  : public crtp_operator<velociraptor_operator> {
public:
  velociraptor_operator() = default;

  velociraptor_operator(operator_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto
  operator()(operator_control_plane& ctrl) const -> generator<table_slice> {
    const auto* ca_certificate
      = get_if<std::string>(&config_, "ca_certificate");
    if (ca_certificate == nullptr) {
      diagnostic::error("no 'ca_certificate' found in config file")
        .hint("generate a valid config file `velociraptor config api_client`")
        .emit(ctrl.diagnostics());
      co_return;
    }
    const auto* client_private_key
      = get_if<std::string>(&config_, "client_private_key");
    if (client_private_key == nullptr) {
      diagnostic::error("no 'client_private_key' found in config file")
        .hint("generate a valid config file `velociraptor config api_client`")
        .emit(ctrl.diagnostics());
      co_return;
    }
    const auto* client_cert = get_if<std::string>(&config_, "client_cert");
    if (client_private_key == nullptr) {
      diagnostic::error("no 'client_cert' found in config file")
        .hint("generate a valid config file `velociraptor config api_client`")
        .emit(ctrl.diagnostics());
      co_return;
    }
    const auto* api_connection_string
      = get_if<std::string>(&config_, "api_connection_string");
    if (api_connection_string == nullptr) {
      diagnostic::error("no 'api_connection_string' found in config file")
        .hint("generate a valid config file `velociraptor config api_client`")
        .emit(ctrl.diagnostics());
      co_return;
    }
    TENZIR_DEBUG("establishing gRPC channel to {}", *api_connection_string);
    auto credentials = grpc::SslCredentials({
      .pem_root_certs = *ca_certificate,
      .pem_private_key = *client_private_key,
      .pem_cert_chain = *client_cert,
    });
    auto channel_args = grpc::ChannelArguments{};
    // Overriding the target name is necessary to connect by IP address
    // because Velociraptor uses self-signed certs.
    channel_args.SetSslTargetNameOverride("VelociraptorServer");
    auto channel = grpc::CreateCustomChannel(*api_connection_string,
                                             credentials, channel_args);
    auto stub = proto::API::NewStub(channel);
    auto args = proto::VQLCollectorArgs{};
    for (const auto& request : args_.requests) {
      TENZIR_DEBUG("staging request {}: {}", request.name, request.vql);
      auto* query = args.add_query();
      query->set_name(request.name);
      query->set_vql(request.vql);
    }
    args.set_max_row(args_.max_rows);
    args.set_max_wait(args_.max_wait.count());
    args.set_org_id(args_.org_id);
    TENZIR_DEBUG("submitting request: max_row = {}, max_wait = {}, org_id = "
                 "{}",
                 args_.max_rows, args_.max_wait, args_.org_id);
    auto context = grpc::ClientContext{};
    auto completion_queue = grpc::CompletionQueue{};
    auto reader = stub->AsyncQuery(&context, args, &completion_queue, nullptr);
    auto done = false;
    auto read = true;
    auto response = proto::VQLResponse{};
    auto input_tag = uint64_t{0};
    co_yield {};
    while (not done) {
      TENZIR_DEBUG("reading response");
      if (read) {
        ++input_tag;
        reader->Read(&response, reinterpret_cast<void*>(input_tag));
        read = false;
      }
      auto deadline = std::chrono::system_clock::now() + 250ms;
      auto output_tag = uint64_t{0};
      auto ok = false;
      auto result = completion_queue.AsyncNext(
        reinterpret_cast<void**>(&output_tag), &ok, deadline);
      switch (result) {
        case grpc::CompletionQueue::SHUTDOWN: {
          TENZIR_DEBUG("drained completion queue");
          TENZIR_ASSERT(not ok);
          done = true;
          break;
        }
        case grpc::CompletionQueue::GOT_EVENT: {
          TENZIR_DEBUG("got event #{} (ok = {})", output_tag, ok);
          if (ok) {
            if (output_tag == input_tag) {
              if (auto slices = parse(response)) {
                for (const auto& slice : *slices) {
                  co_yield slice;
                }
              } else {
                diagnostic::warning(
                  "failed to parse Velociraptor gRPC response")
                  .note("{}", slices.error())
                  .note("response: '{}'", response.response())
                  .note("query_id: '{}'", response.query_id())
                  .note("part: '{}'", response.part())
                  .note("query name: '{}'", response.query().name())
                  .note("query VQL: '{}'", response.query().vql())
                  .note("timestamp: '{}'", response.timestamp())
                  .note("total_rows: '{}'", response.total_rows())
                  .note("log: '{}'", response.log())
                  .emit(ctrl.diagnostics());
              }
              read = true;
            }
          } else {
            // When `ok` is false, future calls to Next() will never return true
            // again, so we can exit our loop.
            done = true;
          }
          break;
        }
        case grpc::CompletionQueue::TIMEOUT: {
          TENZIR_ASSERT(not ok);
          co_yield {};
          break;
        }
      }
    }
    auto status = grpc::Status{};
    reader->Finish(&status, nullptr);
    if (not status.ok()) {
      diagnostic::warning("failed to finish Velociraptor gRPC stream")
        .note("{}", status.error_message())
        .emit(ctrl.diagnostics());
    }
  }

  auto name() const -> std::string override {
    return "velociraptor";
  }

  auto detached() const -> bool override {
    return true;
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, velociraptor_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_;
  record config_;
};

class plugin final : public operator_plugin<velociraptor_operator>,
                     public virtual operator_factory_plugin {
public:
  auto initialize(const record& unused_plugin_config,
                  const record& global_config) -> caf::error override {
    if (not unused_plugin_config.empty()) {
      return diagnostic::error("`{}.yaml` is unused; Use `velociraptor.yaml` "
                               "instead",
                               this->name())
        .to_error();
    }
    auto c
      = try_get_only<tenzir::record>(global_config, "plugins.velociraptor");
    if (not c) {
      return c.error();
    }
    if (*c) {
      config_ = **c;
    }
    return caf::none;
  }

  auto signature() const -> operator_signature override {
    return {
      .source = true,
    };
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = operator_args{};
    auto org_id = std::optional<located<std::string>>{};
    auto request_name = std::optional<located<std::string>>{};
    auto max_rows = std::optional<located<uint64_t>>{};
    auto subscribe = std::optional<located<std::string>>{};
    auto max_wait = std::optional<located<duration>>{};
    auto query = std::optional<located<std::string>>{};
    auto profile = std::optional<located<std::string>>{};
    argument_parser2::operator_("from_velociraptor")
      .add("request_name", request_name)
      .add("org_id", org_id)
      .add("query", query)
      .add("max_rows", max_rows)
      .add("subscribe", subscribe)
      .add("max_wait", max_wait)
      .add("profile", profile)
      .parse(inv, ctx)
      .ignore();

    if (max_wait && max_wait->inner < 1s) {
      diagnostic::error("`max_wait` too low")
        .primary(max_wait->source)
        .hint("value must be great than 1s")
        .emit(ctx);
      return failure::promise();
    }
    if (query) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = std::move(query->inner),
      });
    }
    if (subscribe) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = make_subscribe_query(subscribe->inner),
      });
    }
    if (args.requests.empty()) {
      diagnostic::error("no artifact subscription or VQL expression provided")
        .hint("specify `subscribe=<artifact>` for a subscription")
        .hint("specify `query=<vql>` to run a VQL expression")
        .emit(ctx);
      return failure::promise();
    }
    args.org_id = org_id ? org_id->inner : default_org_id;
    args.max_rows = max_rows ? max_rows->inner : default_max_rows;
    args.max_wait = std::chrono::duration_cast<std::chrono::seconds>(
      max_wait ? max_wait->inner : default_max_wait);
    const auto available_profiles = [&]() -> std::vector<std::string_view> {
      const auto* profiles = get_if<record>(&config_, "profiles");
      if (not profiles) {
        return {};
      }
      auto result = std::vector<std::string_view>{};
      result.reserve(profiles->size());
      for (const auto& [key, _] : *profiles) {
        result.push_back(key);
      }
      return result;
    }();
    if (profile) {
      if (available_profiles.empty()) {
        diagnostic::error("no profiles configured")
          .primary(profile->source)
          .emit(ctx);
        return failure::promise();
      }
      auto profile_config = try_get_only<record>(
        config_, fmt::format("profiles.{}", profile->inner));
      if (not profile_config) {
        diagnostic::error("profile `{}` is invalid: {}", profile->inner,
                          profile_config.error())
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(available_profiles, ", "))
          .emit(ctx);
        return failure::promise();
      }
      if (not *profile_config) {
        diagnostic::error("profile `{}` does not exist", profile->inner)
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(available_profiles, ", "))
          .emit(ctx);
        return failure::promise();
      }
      return std::make_unique<velociraptor_operator>(std::move(args),
                                                     **profile_config);
    }
    if (available_profiles.empty()) {
      return std::make_unique<velociraptor_operator>(std::move(args), config_);
    }
    // If we have profiles configured but no --profile set, we default to the
    // first configured profile.
    auto profile_config = try_get_only<record>(
      config_, fmt::format("profiles.{}", available_profiles.front()));
    if (not profile_config or not *profile_config) {
      diagnostic::error("profile `{}` is invalid", available_profiles.front())
        .note("implicitly used the first configured profile")
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<velociraptor_operator>(std::move(args),
                                                   **profile_config);
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    auto parser = argument_parser{name(), "https://docs.tenzir.com/operators/"
                                          "velociraptor"};
    auto org_id = std::optional<located<std::string>>{};
    auto request_name = std::optional<located<std::string>>{};
    auto max_rows = std::optional<located<uint64_t>>{};
    auto subscribe = std::optional<located<std::string>>{};
    auto max_wait = std::optional<located<duration>>{};
    auto query = std::optional<located<std::string>>{};
    auto profile = std::optional<located<std::string>>{};
    parser.add("-n,--request-name", request_name, "<string>");
    parser.add("-o,--org-id", org_id, "<string>");
    parser.add("-q,--query", query, "<vql>");
    parser.add("-r,--max-rows", max_rows, "<uint64>");
    parser.add("-s,--subscribe", subscribe, "<artifact>");
    parser.add("-w,--max-wait", max_wait, "<duration>");
    parser.add("--profile", profile, "<profile>");
    parser.parse(p);
    if (max_wait && max_wait->inner < 1s) {
      diagnostic::error("--max-wait too low")
        .primary(max_wait->source)
        .hint("value must be great than 1s")
        .throw_();
    }
    if (query) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = std::move(query->inner),
      });
    }
    if (subscribe) {
      args.requests.push_back(request{
        .name = request_name ? std::move(request_name->inner)
                             : fmt::to_string(uuid::random()),
        .vql = make_subscribe_query(subscribe->inner),
      });
    }
    if (args.requests.empty()) {
      diagnostic::error("no artifact subscription or VQL expression provided")
        .hint("use -s,--subscribe <artifact> for a subscription")
        .hint("use -q,--query <vql> to run a VQL expression")
        .throw_();
    }
    args.org_id = org_id ? org_id->inner : default_org_id;
    args.max_rows = max_rows ? max_rows->inner : default_max_rows;
    args.max_wait = std::chrono::duration_cast<std::chrono::seconds>(
      max_wait ? max_wait->inner : default_max_wait);
    const auto available_profiles = [&]() -> std::vector<std::string_view> {
      const auto* profiles = get_if<record>(&config_, "profiles");
      if (not profiles) {
        return {};
      }
      auto result = std::vector<std::string_view>{};
      result.reserve(profiles->size());
      for (const auto& [key, _] : *profiles) {
        result.push_back(key);
      }
      return result;
    }();
    if (profile) {
      if (available_profiles.empty()) {
        diagnostic::error("no profiles configured")
          .primary(profile->source)
          .throw_();
      }
      auto profile_config = try_get_only<record>(
        config_, fmt::format("profiles.{}", profile->inner));
      if (not profile_config) {
        diagnostic::error("profile `{}` is invalid: {}", profile->inner,
                          profile_config.error())
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(available_profiles, ", "))
          .throw_();
      }
      if (not *profile_config) {
        diagnostic::error("profile `{}` does not exist", profile->inner)
          .primary(profile->source)
          .hint("available profiles: {}", fmt::join(available_profiles, ", "))
          .throw_();
      }
      return std::make_unique<velociraptor_operator>(std::move(args),
                                                     **profile_config);
    }
    if (available_profiles.empty()) {
      return std::make_unique<velociraptor_operator>(std::move(args), config_);
    }
    // If we have profiles configured but no --profile set, we default to the
    // first configured profile.
    auto profile_config = try_get_only<record>(
      config_, fmt::format("profiles.{}", available_profiles.front()));
    if (not profile_config or not *profile_config) {
      diagnostic::error("profile `{}` is invalid", available_profiles.front())
        .note("implicitly used the first configured profile")
        .throw_();
    }
    return std::make_unique<velociraptor_operator>(std::move(args),
                                                   **profile_config);
  }

  auto name() const -> std::string override {
    return "from_velociraptor";
  }

  auto operator_name() const -> std::string override {
    return "velociraptor";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::velociraptor

TENZIR_REGISTER_PLUGIN(tenzir::plugins::velociraptor::plugin)
