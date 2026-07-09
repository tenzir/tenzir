//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// `write_json2` is the experimental `tenzir2::TableSlice` ("events2")
// counterpart to `write_json`. It consumes `tenzir2::TableSlice` events and
// produces `chunk_ptr` bytes, printing each row as a JSON object using
// `json_printer2_row_view`.
//
// This is intentionally minimal: newline-delimited JSON (one object per row,
// trailing newline), no options, one chunk per incoming slice.

#include <tenzir/async.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/printable/tenzir/json2_row_view.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

#include <tenzir2/table_slice.hpp>

#include <string>

namespace tenzir::plugins::write_json2 {

namespace {

/// Arguments for `write_json2`. The operator currently takes none.
struct WriteJson2Args {};

/// Runtime operator: prints each row of a `tenzir2::TableSlice` as a JSON
/// object, emitting newline-delimited JSON bytes.
class WriteJson2 final : public Operator<tenzir2::TableSlice, chunk_ptr> {
public:
  WriteJson2() = default;

  explicit WriteJson2(WriteJson2Args) {
  }

  auto process(tenzir2::TableSlice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto const& rows = input.data_;
    auto const length = rows.length();
    if (length == 0) {
      co_return;
    }
    auto printer = json_printer2_row_view{};
    auto buffer = std::string{};
    for (auto i = std::ptrdiff_t{0}; i < length; ++i) {
      // A row of the table is a `record` view, which converts to the erased
      // `array_row_view_<data>` the printer accepts.
      printer.load_new(rows.get(i));
      auto const b = printer.bytes();
      buffer.append(reinterpret_cast<char const*>(b.data()), b.size());
      buffer.push_back('\n');
    }
    auto meta = chunk_metadata{.content_type = "application/x-ndjson"};
    co_await push(chunk::make(std::move(buffer), meta));
  }

private:
};

class write_json2_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_json2";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteJson2Args, WriteJson2>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::write_json2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::write_json2::write_json2_plugin)
