//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "velociraptor/velociraptor.grpc.pb.h"
#include "velociraptor/velociraptor.pb.h"

#include <tenzir/argument_parser.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/uuid.hpp>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

namespace tenzir::plugins::velociraptor {

namespace {

/// The ID of an Organization.
constexpr auto default_org_id = "root";

/// The maximum number of rows per response.
constexpr auto default_max_rows = uint64_t{1'000};

/// The number of seconds to wait on responses.
constexpr auto default_max_wait = uint64_t{1};

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
  uint64_t max_wait;
  std::string org_id;
  std::vector<request> requests;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("max_rows", x.max_rows), f.field("max_wait", x.max_wait),
              f.field("org_id", x.org_id), f.field("requests", x.requests));
  }
};

class velociraptor_operator final
  : public crtp_operator<velociraptor_operator> {
public:
  velociraptor_operator() = default;

  velociraptor_operator(operator_args args, record config)
    : args_{std::move(args)}, config_{std::move(config)} {
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
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
    // Overriding the target name is necessary to connect by IP address because
    // Velociraptor uses self-signed certs.
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
    args.set_max_wait(args_.max_wait);
    args.set_org_id(args_.org_id);
    TENZIR_DEBUG("submitting request: max_row = {}, max_wait = {}, org_id = {}",
                 args_.max_rows, args_.max_wait, args_.org_id);
    auto context = grpc::ClientContext{};
    auto reader = stub->Query(&context, args);
    TENZIR_DEBUG("processing response");
    proto::VQLResponse response;
    while (reader->Read(&response)) {
      auto builder = series_builder{};
      TENZIR_DEBUG("processing response item");
      auto us = std::chrono::microseconds(response.timestamp());
      auto timestamp = time{std::chrono::duration_cast<duration>(us)};
      // Velociraptor sends a stream of responses that consists of "control" and
      // "data" messages. If the response payload is empty, then we have a
      // control message, otherwise we have a data message.
      if (not response.response().empty()) {
        TENZIR_DEBUG("got a data message");
        // There's an opportunity for improvement here, as we are not (yet)
        // making use of the additional types provided in the response. We
        // should synthesize a schema from that and provide that as hint to the
        // series builder.
        auto json = from_json(response.response());
        if (not json) {
          diagnostic::warning("failed to process Velociraptor RPC respone")
            .note("{}", response.response())
            .emit(ctrl.diagnostics());
          continue;
        }
        const auto* objects = caf::get_if<list>(&*json);
        if (objects == nullptr) {
          diagnostic::warning("expected list in Velociraptor JSON response")
            .note("{}", response.response())
            .emit(ctrl.diagnostics());
          continue;
        }
        for (const auto& object : *objects) {
          const auto* rec = caf::get_if<record>(&object);
          if (rec == nullptr) {
            diagnostic::warning("expected objects in Velociraptor response")
              .note("{}", response.response())
              .emit(ctrl.diagnostics());
            continue;
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
          for (const auto& [field, value] : *rec)
            resp.field(field).data(make_view(value));
        }
        for (auto& slice :
             builder.finish_as_table_slice("velociraptor.response"))
          co_yield slice;
      } else if (not response.log().empty()) {
        TENZIR_DEBUG("got a control message");
        TENZIR_WARN("{}", response.log());
        auto row = builder.record();
        row.field("timestamp").data(timestamp);
        row.field("query_id").data(response.query_id());
        row.field("log").data(response.log());
        for (auto& slice :
             builder.finish_as_table_slice("velociraptor.response"))
          co_yield slice;
      }
    }
    auto status = reader->Finish();
    if (not status.ok())
      diagnostic::error("failed to process gRPC response")
        .note("{}", status.error_message())
        .emit(ctrl.diagnostics());
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

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
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

class plugin final : public operator_plugin<velociraptor_operator> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto signature() const -> operator_signature override {
    return {
      .source = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto args = operator_args{};
    auto parser = argument_parser{name(), "https://docs.tenzir.com/operators/"
                                          "velociraptor"};
    auto org_id = std::optional<located<std::string>>{};
    auto request_name = std::optional<located<std::string>>{};
    auto max_rows = std::optional<located<uint64_t>>{};
    auto max_wait = std::optional<located<uint64_t>>{};
    auto request_vql = std::string{};
    parser.add("-n,--request-name", request_name, "<string>");
    parser.add("-o,--org-id", org_id, "<string>");
    parser.add("-r,--max-rows", max_rows, "<uint64>");
    parser.add("-w,--max-wait", max_wait, "<uint64>");
    parser.add(request_vql, "<query>");
    parser.parse(p);
    if (not request_name) {
      request_name = located<std::string>{};
      request_name->inner = fmt::to_string(uuid::random());
    }
    args.requests = {{
      .name = std::move(request_name->inner),
      .vql = std::move(request_vql),
    }};
    args.org_id = org_id ? org_id->inner : default_org_id;
    args.max_rows = max_rows ? max_rows->inner : default_max_rows;
    args.max_wait = max_wait ? max_wait->inner : default_max_wait;
    return std::make_unique<velociraptor_operator>(std::move(args), config_);
  }

  auto name() const -> std::string override {
    return "velociraptor";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::velociraptor

TENZIR_REGISTER_PLUGIN(tenzir::plugins::velociraptor::plugin)
