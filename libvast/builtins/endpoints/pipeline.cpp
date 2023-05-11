//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>
#include <vast/system/builtin_rest_endpoints.hpp>
#include <vast/system/node.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::pipeline {

static auto const* SPEC_V0 = R"_(
/pipeline/update:
  post:
    summary: Update pipeline state
    description: |
      Update the state of the pipeline.
      All values that are not explicitly included in the request are left in their
      old state.
    requestBody:
      description: Body for the update endpoint
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [id]
            properties:
              id:
                type: string
                example: "7"
                description: The id of the pipeline to be updated.
              state:
                type: string
                enum: [starting, running, stopping, stopped]
                example: "running"
              name:
                type: string
                description: Update the human-readable name of the pipeline to this value.
                example: "zeek-monitoring-pipeline"
              restart_with_node:
                type: boolean
                description: Check if the pipeline should be restarted when the VAST Node is restarted.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              example: {}
      400:
        description: Invalid arguments.
        content:
          application/json:
            schema:
              type: object
              required: [error]
              properties:
                error:
                  type: string
                  example: "Invalid arguments"
                  description: The error message.
/pipeline/create:
  post:
    summary: Create a new pipeline
    description: Creates a new pipeline.
    requestBody:
      description: Body for the create endpoint
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [definition]
            properties:
              definition:
                type: string
                example: "export | where foo | publish /bar"
                description: The pipeline definition.
              autostart:
                type: boolean
                default: true
              name:
                type: string
                description: The human-readable name of the pipeline.
                default: "[an auto-generated id]"
                example: "zeek-monitoring-pipeline"
              restart_with_node:
                type: boolean
                default: false
                description: Check if the pipeline should be restarted when the VAST Node is restarted.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              properties:
                id:
                  type: string
                  description: The ID of the successfully created pipeline.
      400:
        description: Invalid arguments.
        content:
          application/json:
            schema:
              type: object
              required: [error]
              properties:
                error:
                  type: string
                  example: "Invalid arguments"
                  description: The error message.
/pipeline/delete:
  post:
    summary: Delete an existing pipeline
    description: Deletes an existing pipeline.
    requestBody:
      description: Body for the delete endpoint
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [id]
            properties:
              id:
                type: string
                example: "7"
                description: The id of the pipeline to be deleted.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              example: {}
      400:
        description: Invalid arguments.
        content:
          application/json:
            schema:
              type: object
              required: [error]
              properties:
                error:
                  type: string
                  example: "Invalid arguments"
                  description: The error message.
/pipeline/list:
  post:
    summary: List all existing pipelines
    description: Lists all existing pipelines.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              description: An array of all pipelines with additional information about their singular operators in order of execution.
              properties:
                pipelines:
                  type: array
                  items:
                    type: object
                    properties:
                      id:
                        type: string
                        description: The pipeline ID.
                      name:
                        type: string
                        description: The human-readable name of the pipeline to this value.
                      definition:
                        type: string
                        description: The pipeline definition.
                      state:
                        type: string
                        enum: [starting, running, stopping, stopped]
                      error:
                        type: string
                        description: The error that the pipeline may have encountered during running.
                      operators:
                        type: array
                        items:
                          type: object
                          properties:
                            id:
                              type: string
                              description: The pipeline operator ID.
                            definition:
                              type: string
                              description: The pipeline operator definition.
                            instrumented:
                              type: boolean
                              description: Flag that enables subscribing to this operator's metrics and warnings under /pipeline/<pipeline-id>/<operator-id>.
              example:
                pipelines:
                - id: "7"
                  name: "user-assigned-name"
                  definition: "export | where foo | publish /bar"
                  state: "starting"
                  error: null
                  operators:
                    - id: "0"
                      definition: "export"
                      instrumented: false
                    - id: "1"
                      definition: "where foo"
                      instrumented: true
                    - id: "2"
                      definition: "publish /bar"
                      instrumented: true
                - id: "8"
                  name: "wrong-pipeline"
                  definition: "export asdf"
                  state: "stopped"
                  error: "format 'asdf' not found"
                  operators:
                    - id: "0"
                      definition: "export asdf"
                      instrumented: false
      400:
        description: Invalid arguments.
        content:
          application/json:
            schema:
              type: object
              required: [error]
              properties:
                error:
                  type: string
                  example: "Invalid arguments"
                  description: The error message.
/serve:
  post:
    summary: Return data from a pipeline
    description: Returns events from an existing pipeline. The pipeline definition must include a serve operator.
    requestBody:
      description: Body for the serve endpoint
      required: true
      content:
        application/json:
          schema:
            type: object
            required: [serve_id]
            properties:
              serve_id:
                type: string
                example: "query1"
                description: The id that was passed to the serve operator.
              continuation_token:
                type: string
                example: "340ce2j"
                description: The continuation token that was returned with the last response. For the initial request this is null.
              max_events:
                type: integer
                example: 50
                description: The maximum number of events returned. If unset, the number is unlimited
              timeout:
                type: string
                example: "100ms"
                default: "100ms"
                description: The maximum amount of time spent on the request. Hitting the timeout is not an error.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              properties:
                next_continuation_token:
                  type: string
                  description: A token to access the next pipeline data batch, null if the pipeline is completed.
                  example: "340ce2j"
                schemas:
                  type: array
                  items:
                    type: object
                    properties:
                      schema-ref:
                        type: string
                        description: The unique schema identifier.
                      definition:
                        type: object
                        description: The schema definition in JSON format.
                  description: The schemas that the served events are based on.
                  example:
                  - schema-ref: "c631d301e4b18f4"
                    definition:
                      record:
                        - timestamp: "time"
                          schema: "string"
                          schema_id: "string"
                          events: "uint64"
                data:
                  type: array
                  items:
                    type: object
                    properties:
                      schema_id:
                        type: string
                        description: The unique schema identifier.
                      data:
                        type: object
                        description: The actual served data in JSON format.
                  description: The served events.
                  example:
                  - schema_id: c631d301e4b18f4
                    data:
                      timestamp: "2023-04-26T12:00:00Z"
                      schema: "zeek.conn"
                      schema_id: "ab2371bas235f1"
                      events: 50
                  - schema_id: c631d301e4b18f4
                    data:
                      timestamp: "2023-04-26T12:05:00Z"
                      schema: "suricata.dns"
                      schema_id: "cd4771bas235f1"
                      events: 50
      400:
        description: Invalid arguments.
        content:
          application/json:
            schema:
              type: object
              required: [error]
              properties:
                error:
                  type: string
                  example: "Invalid arguments"
                  description: The error message.
    )_";

class plugin final : public virtual rest_endpoint_plugin {
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "api-pipeline";
  };

  [[nodiscard]] auto prefix() const -> std::string override {
    return "";
  }

  [[nodiscard]] auto openapi_specification(api_version version) const
    -> data override {
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(SPEC_V0);
    VAST_ASSERT(result);
    return *result;
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] auto rest_endpoints() const
    -> const std::vector<rest_endpoint>& override {
    static auto endpoints = std::vector<vast::rest_endpoint>{
      {
        .endpoint_id
        = static_cast<uint64_t>(system::pipeline_endpoints::update),
        .method = http_method::post,
        .path = "/pipeline/update",
        .params = vast::record_type{{"id", vast::string_type{}},
                                    {"state", vast::string_type{}},
                                    {"name", vast::string_type{}},
                                    {"restart_with_node", vast::bool_type{}}},
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id
        = static_cast<uint64_t>(system::pipeline_endpoints::create),
        .method = http_method::post,
        .path = "/pipeline/create",
        .params = vast::record_type{{"definition", vast::string_type{}},
                                    {"autostart", vast::bool_type{}},
                                    {"name", vast::string_type{}},
                                    {"restart_with_node", vast::bool_type{}}},
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id
        = static_cast<uint64_t>(system::pipeline_endpoints::delete_),
        .method = http_method::post,
        .path = "/pipeline/delete",
        .params = vast::record_type{{"id", vast::string_type{}}},
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id = static_cast<uint64_t>(system::pipeline_endpoints::list),
        .method = http_method::post,
        .path = "/pipeline/list",
        .params = {},
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id = static_cast<uint64_t>(system::pipeline_endpoints::serve),
        .method = http_method::post,
        .path = "/serve",
        .params = vast::record_type{{"serve_id", vast::string_type{}},
                                    {"continuation_token", vast::string_type{}},
                                    {"max_events", vast::int64_type{}},
                                    {"timeout", vast::string_type{}}},
        .version = api_version::v0,
        .content_type = http_content_type::json,
      }};
    return endpoints;
  }

  auto handler([[maybe_unused]] caf::actor_system& system,
               [[maybe_unused]] system::node_actor node) const
    -> system::rest_handler_actor override {
    die("unimplemented");
  }
};

} // namespace vast::plugins::rest_api::pipeline

VAST_REGISTER_PLUGIN(vast::plugins::rest_api::pipeline::plugin)
