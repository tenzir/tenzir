//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <arrow/array.h>
#include <arrow/compute/cast.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <caf/expected.hpp>

#include <queue>

namespace tenzir::plugins::feather2 {
namespace {

class callback_listener : public arrow::ipc::Listener {
public:
  callback_listener() = default;

  arrow::Status OnRecordBatchDecoded(
    std::shared_ptr<arrow::RecordBatch> record_batch) override {
    record_batch_buffer.push(std::move(record_batch));
    return arrow::Status::OK();
  }

  std::queue<std::shared_ptr<arrow::RecordBatch>> record_batch_buffer;
};

auto parse_feather(generator<chunk_ptr> input, operator_control_plane& ctrl)
  -> generator<table_slice> {
  auto byte_reader = make_byte_reader(std::move(input));
  auto listener = std::make_shared<callback_listener>();
  auto stream_decoder = arrow::ipc::StreamDecoder(listener);
  while (true) {
    auto required_size
      = detail::narrow_cast<size_t>(stream_decoder.next_required_size());
    auto payload = byte_reader(required_size);
    if (!payload) {
      co_yield {};
      continue;
    }
    const auto done = payload->size() < required_size;
    auto decode_result
      = stream_decoder.Consume(as_arrow_buffer(std::move(payload)));
    if (!decode_result.ok()) {
      diagnostic::error("failed to decode the byte stream into a record batch")
        .note("{}", decode_result.ToString())
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (!listener->record_batch_buffer.empty()) {
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
          .emit(ctrl.diagnostics());
        co_return;
      }
      co_yield table_slice(batch);
    }
    if (done) {
      co_return;
    }
  }
}

auto print_feather(
  table_slice input, operator_control_plane& ctrl,
  const std::shared_ptr<arrow::ipc::RecordBatchWriter>& stream_writer,
  const std::shared_ptr<arrow::io::BufferOutputStream>& sink)
  -> generator<chunk_ptr> {
  auto batch = to_record_batch(input);
  auto validate_status = batch->Validate();
  TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
  auto stream_writer_status = stream_writer->WriteRecordBatch(*batch);
  if (!stream_writer_status.ok()) {
    diagnostic::error("failed to write a record batch to the stream")
      .note("{}", stream_writer_status.ToString())
      .emit(ctrl.diagnostics());
    co_return;
  }
  // We must finish the clear the buffer because the provided APIs do not offer
  // a scrape and rewrite on the allocated same memory.
  auto finished_buffer_result = sink->Finish();
  if (!finished_buffer_result.ok()) {
    diagnostic::error("failed to close the stream and return the buffer")
      .note("{}", finished_buffer_result.status().ToString())
      .emit(ctrl.diagnostics());
    co_return;
  }
  co_yield chunk::make(finished_buffer_result.MoveValueUnsafe());
  // The buffer is reinit with newly allocated memory because the API does not
  // offer a Reset that just clears the original data.
  auto reset_buffer_result = sink->Reset();
  if (!reset_buffer_result.ok()) {
    diagnostic::error("failed to reset buffer")
      .note("{}", reset_buffer_result.ToString())
      .emit(ctrl.diagnostics());
  }
}

class feather2_parser final : public plugin_parser {
public:
  feather2_parser() = default;
  auto name() const -> std::string override {
    return "feather2";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_feather(std::move(input), ctrl);
  }

  friend auto inspect(auto& f, feather2_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class feather2_printer final : public plugin_printer {
public:
  feather2_printer() = default;

  auto name() const -> std::string override {
    // FIXME: Rename this and the file to just feather.
    return "feather2";
  }

  auto instantiate([[maybe_unused]] type input_schema,
                   operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    auto sink = arrow::io::BufferOutputStream::Create().MoveValueUnsafe();
    const arrow::ipc::IpcWriteOptions& options
      = arrow::ipc::IpcWriteOptions::Defaults();
    auto schema = input_schema.to_arrow_schema();
    auto stream_writer_result
      = arrow::ipc::MakeStreamWriter(sink, schema, options);
    if (!stream_writer_result.ok()) {
      return diagnostic::error("{}", stream_writer_result.status().ToString())
        .note("failed to create Feather stream writer")
        .to_error();
    }
    auto stream_writer = stream_writer_result.MoveValueUnsafe();
    return printer_instance::make(
      [&ctrl, sink = std::move(sink), stream_writer = std::move(stream_writer)](
        table_slice slice) -> generator<chunk_ptr> {
        return print_feather(std::move(slice), ctrl, stream_writer, sink);
      });
  }

  auto allows_joining() const -> bool override {
    return false;
  };

  friend auto inspect(auto& f, feather2_printer& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual parser_plugin<feather2_parser>,
                     public virtual printer_plugin<feather2_printer> {
  auto name() const -> std::string override {
    return "feather2";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"feather2", "https://docs.tenzir.com/next/"
                                              "formats/feather2"};
    parser.parse(p);
    return std::make_unique<feather2_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"feather2", "https://docs.tenzir.com/next/"
                                              "formats/feather2"};
    parser.parse(p);
    return std::make_unique<feather2_printer>();
  }
};

} // namespace
} // namespace tenzir::plugins::feather2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather2::plugin)
