#pragma once

#include <caf/stream_stage.hpp>

#include <type_traits>

template <template <class...> class Template, class Instantiation>
struct is_specialization_of : std::false_type {};

template <template <class...> class Template, class... Args>
struct is_specialization_of<Template, Template<Args...>> : std::true_type {};

template <typename StreamStage>
void shutdown_stream_stage(StreamStage& stage) requires(
  is_specialization_of<caf::stream_stage, StreamStage>::value) {
  // First we call `shutdown()` to notify all upstream
  stage->shutdown();
  // First, we move
  stage->out().fan_out_flush();
  // Next, we need to `close()`
  // Note that this will remove all clean outbound paths, so we need
  // to call `fan_out_flush()` beforehand or we might lose the data
  // from the global buffer.
  stage->out().close();
  stage->out().force_emit_batches();
}
