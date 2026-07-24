//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/feather.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/make_byte_reader.hpp"
#include "tenzir/table_slice.hpp"

#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <memory>
#include <queue>

namespace tenzir::detail {

namespace {

class callback_listener : public arrow::ipc::Listener {
public:
  callback_listener() = default;

  auto OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> record_batch)
    -> arrow::Status override {
    record_batch_buffer.push(std::move(record_batch));
    return arrow::Status::OK();
  }

  std::queue<std::shared_ptr<arrow::RecordBatch>> record_batch_buffer;
};

} // namespace

auto parse_feather(generator<chunk_ptr> input, diagnostic_handler& dh)
  -> generator<table_slice> {
  auto byte_reader = make_byte_reader(std::move(input));
  auto listener = std::make_shared<callback_listener>();
  auto stream_decoder
    = arrow::ipc::StreamDecoder(listener, arrow_ipc_read_options());
  auto truncated_bytes = size_t{0};
  auto decoded_once = false;
  while (true) {
    auto required_size
      = detail::narrow_cast<size_t>(stream_decoder.next_required_size());
    if (required_size == 0) {
      co_return;
    }
    auto payload = byte_reader(required_size);
    if (not payload) {
      co_yield {};
      continue;
    }
    truncated_bytes += payload->size();
    if (payload->size() < required_size) {
      if (truncated_bytes != 0 and payload->size() != 0) {
        // Ideally this always would be just a warning, but the stream decoder
        // happily continues to consume invalid bytes. E.g., trying to read a
        // JSON file with this parser will just swallow all bytes, emitting this
        // one error at the very end. Not a single time does consuming a buffer
        // actually fail. We should probably look into limiting the memory usage
        // here, as the stream decoder will keep consumed-but-not-yet-converted
        // buffers in memory.
        diagnostic::warning("truncated {} trailing bytes", truncated_bytes)
          .severity(decoded_once ? severity::warning : severity::error)
          .emit(dh);
      }
      co_return;
    }
    auto decode_result
      = stream_decoder.Consume(as_arrow_buffer(std::move(payload)));
    if (not decode_result.ok()) {
      diagnostic::error("{}", decode_result.ToStringWithoutContextLines())
        .note("failed to decode the byte stream into a record batch")
        .emit(dh);
      co_return;
    }
    while (not listener->record_batch_buffer.empty()) {
      decoded_once = true;
      truncated_bytes = 0;
      auto batch = listener->record_batch_buffer.front();
      listener->record_batch_buffer.pop();
      auto validate_status = batch->Validate();
      TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
      // We check whether the name metadatum from Tenzir's conversion to record
      // batches is still present. If it is not, then we stop parsing because we
      // cannot feasibly continue.
      // TODO: Implement a best-effort conversion for record batches coming from
      // other tools to Tenzir's supported subset and required metadata.
      const auto& metadata = batch->schema()->metadata();
      if (not metadata
          or std::find(metadata->keys().begin(), metadata->keys().end(),
                       "TENZIR:name:0")
               == metadata->keys().end()) {
        diagnostic::error("not implemented")
          .note("cannot convert Feather without Tenzir metadata")
          .emit(dh);
        co_return;
      }
      co_yield table_slice(batch);
    }
  }
}

} // namespace tenzir::detail
