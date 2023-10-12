//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"
#include "tenzir/format/arrow.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/type.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <boost/asio.hpp>
#include <boost/process.hpp>
#include <caf/detail/scope_guard.hpp>

#include <mutex>
#include <queue>
#include <thread>

namespace bp = boost::process;

namespace tenzir::plugins::python {
namespace {

auto python_preamble = R"_(

import sys
import pyarrow as pa
import types
import pytenzir.utils.arrow
#import requests
#import json

def log(*args):
  prefix = "    "
  z = " ".join(map(str, args))
  y = prefix + " " + z.replace("\n", "\n" + prefix + " ")
  print(y, file=sys.stderr)

istream = pa.input_stream(sys.stdin.buffer)
ostream = pa.output_stream(sys.stdout.buffer)

def execute_user_code(__batch, __code):
  __d = __batch.to_pydict()
  __result = {}
  for __i in range(__batch.num_rows):
    for __key, __values in __d.items():
      locals()[__key] = __values[__i]
    # =============
    exec(__code, globals(), locals())
    # =============
    for __key, __value in dict(locals()).items():
      if isinstance(__value, types.ModuleType):
        continue
      if not __key.startswith("_"):
        if __key not in __result:
          __result[__key] = []
        # TODO: Key might be missing.
        __result[__key].append(__value)
  return __result


while True:
  try:
    reader = pa.ipc.RecordBatchStreamReader(istream)
    batch = reader.read_next_batch()
    result = execute_user_code(batch, CODE)
    result_batch = pa.RecordBatch.from_pydict(result)
    writer = pa.ipc.RecordBatchStreamWriter(ostream, result_batch.schema)
    writer.write(result_batch)
    sys.stdout.flush()
  except pa.lib.ArrowInvalid:
    break


)_";

class arrow_fd_istream : public ::arrow::io::InputStream {
public:
  explicit arrow_fd_istream(int fd) : fd_{fd} {
  }

  arrow::Status Close() override {
    TENZIR_ASSERT_CHEAP(::close(fd_) == 0);
    fd_ = -1;
    return arrow::Status::OK();
  }

  bool closed() const override {
    return fd_ == -1;
  }

  ::arrow::Result<int64_t> Tell() const override {
    return pos_;
  }

  ::arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    auto n = ::read(fd_, out, nbytes);
    TENZIR_ASSERT_CHEAP(n >= 0);
    pos_ += n;
    return n;
  }

  ::arrow::Result<std::shared_ptr<::arrow::Buffer>>
  Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          ::arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                          Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
    buffer->ZeroPadding();
    return std::move(buffer);
  }

private:
  int fd_;
  int64_t pos_ = 0;
};

class python_operator final : public crtp_operator<python_operator> {
public:
  python_operator() = default;

  explicit python_operator(std::string code, bool script_mode)
    : code_{std::move(code)}, script_mode_(script_mode) {
  }

  auto execute_nonscript(generator<table_slice> input,
                         operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // auto preamble = chunk::mmap("./preamble.py");
    // TENZIR_ASSERT_CHEAP(preamble);
    // auto preamble_string = std::string{reinterpret_cast<const
    // char*>((*preamble)->data()), (*preamble)->size()};
    try {
      bp::pipe std_out;
      bp::pipe std_in;
      auto path = bp::search_path("python");
      TENZIR_INFO("path: {}", path.string());
      auto child
        = bp::child{path, "-c", "CODE = '''" + code_ + "'''" + python_preamble,
                    bp::std_out > std_out, bp::std_in < std_in};
      for (auto&& slice : input) {
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        auto batch = to_record_batch(slice);
        auto stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(
                        stream, slice.schema().to_arrow_schema())
                        .ValueOrDie();
        auto status = writer->WriteRecordBatch(*batch);
        TENZIR_ASSERT_CHEAP(status.ok());
        auto result = stream->Finish().ValueOrDie();
        std_in.write(reinterpret_cast<const char*>(result->data()),
                     detail::narrow<int>(result->size()));
        auto file = arrow_fd_istream{std_out.native_source()};
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(&file);
        if (!reader.status().ok()) {
          ctrl.abort(caf::make_error(
            ec::logic_error, fmt::format("failed to open reader: {}",
                                         reader.status().CodeAsString())));
          co_return;
        }
        auto foo = (*reader)->ReadNext();
        if (!foo.status().ok()) {
          ctrl.abort(caf::make_error(ec::logic_error,
                                     fmt::format("failed to read batch: {}",
                                                 foo.status().CodeAsString())));
          co_return;
        }
        auto output = table_slice{foo->batch};
        auto new_type = type{"tenzir.python", output.schema()};
        auto actual_result = arrow::RecordBatch::Make(
          new_type.to_arrow_schema(), output.rows(), foo->batch->columns());
        output = table_slice{actual_result, new_type};
        co_yield output;
      }
      std_in.close();
      child.wait();
    } catch (const std::exception& ex) {
      ctrl.abort(caf::make_error(ec::logic_error, fmt::to_string(ex.what())));
    }
    co_return;
  }

  auto execute_script(generator<table_slice> input,
                      operator_control_plane& ctrl) const
    -> generator<table_slice> {
    TENZIR_TODO(); // TODO!
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    if (script_mode_)
      return execute_script(std::move(input), ctrl);
    else
      return execute_nonscript(std::move(input), ctrl);
  }

  auto to_string() const -> std::string override {
    return code_;
  }

  auto name() const -> std::string override {
    return "python";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    // Note: The `unordered` means that we do not necessarily return the first
    // `limit_` events.
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, python_operator& x) -> bool {
    return f.apply(x.code_);
  }

private:
  std::string code_ = {};
  bool script_mode_ = {};
};

class plugin final : public virtual operator_plugin<python_operator> {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto command = std::string{};
    auto script_mode = bool{};
    auto parser = argument_parser{"python", "https://docs.tenzir.com/next/"
                                            "operators/transformations/python"};
    parser.add(command, "<command>");
    parser.add("--script", script_mode);
    parser.parse(p);
    return std::make_unique<python_operator>(std::move(command), script_mode);
  }
};

} // namespace
} // namespace tenzir::plugins::python

TENZIR_REGISTER_PLUGIN(tenzir::plugins::python::plugin)
