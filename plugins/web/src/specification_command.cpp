//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/server_command.hpp"

namespace tenzir::plugins::web {

auto openapi_record() -> record {
  auto paths = record{};
  for (auto const* plugin : plugins::get<rest_endpoint_plugin>()) {
    auto spec = plugin->openapi_specification();
    TENZIR_ASSERT_CHEAP(caf::holds_alternative<record>(spec));
    for (auto& [key, value] : caf::get<record>(spec))
      paths.emplace(key, value);
  }
  std::sort(paths.begin(), paths.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.first < rhs.first;
  });
  auto schemas = from_yaml(R"_(
Metrics:
  type: object
  description: Information about pipeline data ingress and egress.
  properties:
    total:
      type: object
      description: Information about total pipeline metrics (e.g. total ingress/egress)
      properties:
        ingress:
          type: object
          description: Information about the total ingress. Contains no values when the source operator has not run.
          properties:
            unit:
              type: string
              enum:
                - bytes
                - events
              description: The unit of the input data.
              example: bytes
            internal:
              type: boolean
              description: Whether the operator's ingress is internal.
              example: false
            num_elements:
              type: integer
              description: The total amount of elements that entered the pipeline source.
              example: 109834
            num_batches:
              type: integer
              description: The total amount of batches that were generated during data processing in the pipeline source.
              example: 2
            num_approx_bytes:
              type: integer
              description: The total amount of bytes that entered the pipeline source. For events, this value is an approximation.
              example: 30414
            total_seconds:
              type: number
              description: The total duration of the pipeline source operation in seconds.
              example: 1.233998321
            processing_seconds:
              type: number
              description: The total duration of the pipeline source data processing in seconds.
              example: 1.179999992
        egress:
          type: object
          description: Information about the total egress. Contains no values when the sink operator has not run.
          properties:
            unit:
              type: string
              enum:
                - bytes
                - events
              description: The unit of the output data.
              example: bytes
            internal:
              type: boolean
              description: Whether the operator's egress is internal.
              example: false
            num_elements:
              type: integer
              description: The total amount of elements that entered the pipeline sink.
              example: 30414
            num_batches:
              type: integer
              description: The total amount of batches that were generated during data processing in the pipeline sink.
              example: 1
            num_approx_bytes:
              type: integer
              description: The total amount of bytes that entered the pipeline sink. For events, this value is an approximation.
              example: 30414
            total_seconds:
              type: number
              description: The total duration of the pipeline sink operation in seconds.
              example: 2.945935512
            processing_seconds:
              type: number
              description: The total duration of the pipeline sink data processing in seconds.
              example: 1.452123512
CreateParameters:
  type: object
  required:
    - definition
  properties:
    definition:
      type: string
      example: export | where foo | publish /bar
      description: The pipeline definition.
    name:
      type: string
      description: The human-readable name of the pipeline.
      default: "[an auto-generated id]"
      example: zeek-monitoring-pipeline
    hidden:
      type: boolean
      description: |
        A flag specifying whether this pipeline is hidden.
        Hidden pipelines are not persisted and will not show up in the /pipeline/list endpoint response.
      default: false
      example: false
    ttl:
      type: string
      description: |
        A duration string specifying the maximum time for this pipeline to exist. No value means the pipeline is allowed to exist forever.
        This parameter must be defined if the `hidden` parameter is true.
      default: ~
      example: 5.0m
    autostart:
      $ref: "#/components/schemas/PipelineAutostart"
    autodelete:
      $ref: "#/components/schemas/PipelineAutodelete"
LaunchParameters:
  allOf:
    - $ref: "#/components/schemas/CreateParameters"
    - type: object
      required:
        - serve_id
      properties:
        serve_id:
          type: string
          description: The identifier for the `server` operator.
          example: "4ada2434-32asfe2s"
        serve_buffer_size:
          type: integer
          description: The maximum number of events to keep in the `serve` operator.
          example: 4000
Diagnostics:
  type: array
  items:
    $ref: '#/components/schemas/Diagnostic'
Diagnostic:
  type: object
  properties:
    severity:
      type: string
      enum: ["error", "warning", "note"]
    message:
      type: string
      example: "unknown option `--frobnify`"
    annotation:
      type: array
      items:
        $ref: '#/components/schemas/Annotation'
    notes:
      type: array
      items:
        $ref: '#/components/schemas/Note'
    rendered:
      type: string
      example: "\u001b[1m\u001b[31merror\u001b[39m: unknown option `--frobnify`\u001b[0m\n"
Annotation:
  type: object
  properties:
    primary:
      type: boolean
    text:
      type: string
      example: "this option does not exist"
      description: A potentially empty label.
    source:
      $ref: '#/components/schemas/Location'
Note:
  type: object
  properties:
    kind:
      type: string
      enum: ["note", "usage", "hint", "docs"]
      example: "usage"
    message:
      type: string
      example: "file <path> [-f|--follow] [-m|--mmap] [-t|--timeout <duration>]"
Location:
  type: object
  description: A region in the source code, defined by byte offsets.
  properties:
    begin:
      type: number
      example: 42
    end:
      type: number
      example: 48
PipelineLabel:
  type: object
  properties:
    text:
      type: string
      description: The pipeline label text.
      example: zeek
    color:
      type: string
      description: The pipeline label color.
      example: 3F1A24
PipelineLabels:
  type: array
  description: The user-provided labels for this pipeline.
  items:
    $ref: "#/components/schemas/PipelineLabel"
PipelineAutostart:
  type: object
  description: Flags that specify on which state to restart the pipeline.
  properties:
    created:
      type: boolean
      description: Autostart the pipeline upon creation.
      default: false
      example: true
    completed:
      type: boolean
      description: Autostart the pipeline upon completion.
      default: false
      example: false
    failed:
      type: boolean
      description: Autostart the pipeline upon failure.
      default: false
      example: false
    stopped:
      type: boolean
      description: Autostart the pipeline when it stops before completing.
      default: false
      example: true
PipelineAutodelete:
  type: object
  description: Flags that specify on which state to delete the pipeline.
  properties:
    completed:
      type: boolean
      description: Autodelete the pipeline upon completion.
      default: false
      example: false
    failed:
      type: boolean
      description: Autodelete the pipeline upon failure.
      default: false
      example: true
    stopped:
      type: boolean
      description: Autodelete the pipeline when it stops before completing.
      default: false
      example: false
PipelineInfo:
  type: object
  properties:
    id:
      type: string
      description: The pipeline ID.
    name:
      type: string
      description: The human-readable name of the pipeline.
    definition:
      type: string
      description: The pipeline definition.
    state:
      type: string
      enum:
        - created
        - running
        - paused
        - failed
        - stopped
    error:
      type: string
      description: The error that the pipeline may have encountered during running.
    diagnostics:
      $ref: '#/components/schemas/Diagnostics'
    metrics:
      $ref: '#/components/schemas/Metrics'
    labels:
      $ref: "#/components/schemas/PipelineLabels"
    autostart:
      $ref: "#/components/schemas/PipelineAutostart"
    autodelete:
      $ref: "#/components/schemas/PipelineAutodelete"
    ttl_expires_in_ns:
      type: integer
      description: If a TTL exists for this pipeline, the remaining TTL in nanoseconds.
      example: 23400569058
  )_");
  TENZIR_ASSERT_CHEAP(schemas);
  // clang-format off
  auto openapi = record{
    {"openapi", "3.0.0"},
    {"info",
     record{
       {"title", "Tenzir Rest API"},
       {"version", "\"0.1\""},
       {"description", R"_(
This API can be used to interact with a Tenzir Node in a RESTful manner.

All API requests must be authenticated with a valid token, which must be
supplied in the `X-Tenzir-Token` request header. The token can be generated
on the command-line using the `tenzir-ctl web generate-token` command.)_"},
     }},
    {"servers", list{{
      record{{"url", "https://tenzir.example.com/api/v0"}},
    }}},
    {"security", list {{
      record {{"TenzirToken", list{}}},
    }}},
    {"components", record{
      {"schemas", std::move(*schemas)},
      {"securitySchemes",
        record{{"TenzirToken", record {
            {"type", "apiKey"},
            {"in", "header"},
            {"name", "X-Tenzir-Token"}
        }}}},
    }},
    {"paths", std::move(paths)},
  };
  // clang-format on
  return openapi;
}

std::string generate_openapi_json() noexcept {
  auto record = openapi_record();
  auto json = to_json(record);
  TENZIR_ASSERT_CHEAP(json);
  return *json;
}

std::string generate_openapi_yaml() noexcept {
  auto record = openapi_record();
  auto yaml = to_yaml(record);
  TENZIR_ASSERT_CHEAP(yaml);
  return *yaml;
}

auto specification_command(const tenzir::invocation&, caf::actor_system&)
  -> caf::message {
  auto yaml = generate_openapi_yaml();
  fmt::print("---\n{}\n", yaml);
  return {};
}

} // namespace tenzir::plugins::web
