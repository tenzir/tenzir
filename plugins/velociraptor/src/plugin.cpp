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

/// The IP address of the Velociraptor gRPC endpoint.
constexpr auto default_server = "localhost";

/// The port of the Velociraptor gRPC endpoint.
constexpr auto default_port = 8001;

/// The ID of an Org, e.g., "O3CLG".
constexpr auto default_org_id = "";

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
  std::string server;
  uint16_t port;
  uint64_t max_rows;
  uint64_t max_wait;
  std::string org_id;
  std::vector<request> requests;

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("server", x.server), f.field("port", x.port),
              f.field("max_rows", x.max_rows), f.field("max_wait", x.max_wait),
              f.field("org_id", x.org_id), f.field("requests", x.requests));
  }
};

class velociraptor_operator final
  : public crtp_operator<velociraptor_operator> {
public:
  velociraptor_operator() = default;

  velociraptor_operator(operator_args args) : args_{std::move(args)} {
    // TODO: fill in details from config.
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto endpoint = fmt::format("{}:{}", args_.server, args_.port);
    auto channel
      = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    auto stub = API::NewStub(channel);
    TENZIR_DEBUG("submitting request");
    auto args = VQLCollectorArgs{};
    for (const auto& request : args_.requests) {
      auto* query = args.add_query();
      query->set_name(request.name);
      query->set_vql(request.vql);
    }
    args.set_max_row(args_.max_rows);
    args.set_max_wait(args_.max_wait);
    args.set_org_id(args_.org_id);
    auto context = grpc::ClientContext{};
    auto reader = stub->Query(&context, args);
    TENZIR_DEBUG("processing response");
    VQLResponse response;
    while (reader->Read(&response)) {
      auto builder = series_builder{};
      TENZIR_DEBUG("processing response item");
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
      for (auto& slice : builder.finish_as_table_slice("velociraptor.response"))
        co_yield slice;
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
    auto server = std::optional<located<std::string>>{};
    auto port = std::optional<located<uint16_t>>{};
    auto org_id = std::optional<located<std::string>>{};
    auto request_name = std::optional<located<std::string>>{};
    auto max_rows = std::optional<located<uint64_t>>{};
    auto max_wait = std::optional<located<uint64_t>>{};
    auto request_vql = std::string{};
    parser.add("-n,--request-name", request_name, "<string>");
    parser.add("-o,--org-id", org_id, "<string>");
    parser.add("-p,--port", port, "<number>");
    parser.add("-r,--max-rows", max_rows, "<uint64>");
    parser.add("-s,--server", server, "<string>");
    parser.add("-w,--max-wait", max_wait, "<uint64>");
    parser.add(request_vql, "<query>");
    parser.parse(p);
    args.org_id = org_id ? org_id->inner : default_org_id;
    args.server = server ? server->inner : default_server;
    args.port = port ? port->inner : default_port;
    if (not request_name) {
      request_name = located<std::string>{};
      request_name->inner = fmt::to_string(uuid::random());
    }
    args.requests = {{
      .name = std::move(request_name->inner),
      .vql = std::move(request_vql),
    }};
    return std::make_unique<velociraptor_operator>(std::move(args));
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
