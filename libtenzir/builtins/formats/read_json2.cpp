//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// `read_json2` is the experimental `tenzir2::TableSlice` ("events2") counterpart
// to `read_json`. It consumes `chunk_ptr` bytes and parses a stream of
// concatenated JSON objects into `tenzir2::TableSlice` events using simdjson.
//
// This is intentionally minimal: no NDJSON, no arrays-of-objects mode, and no
// options. JSON values are typed directly from simdjson (number ->
// int64/uint64/double, bool -> bool, string -> string as-is, null -> null,
// array -> list, object -> nested record); there is no string type-inference.

#include <tenzir/async.hpp>
#include <tenzir/detail/padded_buffer.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/try_simdjson.hpp>

#include <tenzir2/table_slice.hpp>
#include <tenzir2/type_system/array/builder.hpp>

#include <simdjson.h>

namespace tenzir::plugins::read_json2 {

namespace {

// simdjson recommends starting with a generous batch size and growing it when a
// single document exceeds the current batch. These mirror the constants used by
// the classic JSON parser.
constexpr auto initial_batch_size = size_t{10 * 1024 * 1024};
constexpr auto max_batch_size = size_t{2ull * 1024 * 1024 * 1024};

/// Recursively writes a single simdjson value into an erased `array_<data>`
/// builder slot. Emits warnings on malformed values and falls back to `null`.
auto parse_value(auto&& val, tenzir2::array_builder_<tenzir2::data>& out,
                 diagnostic_handler& dh, size_t depth = 0) -> void {
  auto type = val.type();
  if (type.error()) {
    diagnostic::warning("failed to parse a JSON value").emit(dh);
    out.null();
    return;
  }
  switch (type.value_unsafe()) {
    case simdjson::ondemand::json_type::null: {
      out.null();
      return;
    }
    case simdjson::ondemand::json_type::boolean: {
      auto result = val.get_bool();
      if (result.error()) {
        diagnostic::warning("failed to parse a JSON boolean").emit(dh);
        out.null();
        return;
      }
      out.data(result.value_unsafe());
      return;
    }
    case simdjson::ondemand::json_type::number: {
      auto kind = val.get_number_type();
      if (kind.error()) {
        diagnostic::warning("failed to parse a JSON number").emit(dh);
        out.null();
        return;
      }
      switch (kind.value_unsafe()) {
        case simdjson::ondemand::number_type::floating_point_number: {
          out.data(val.get_double().value_unsafe());
          return;
        }
        case simdjson::ondemand::number_type::signed_integer: {
          out.data(val.get_int64().value_unsafe());
          return;
        }
        case simdjson::ondemand::number_type::unsigned_integer: {
          out.data(val.get_uint64().value_unsafe());
          return;
        }
        case simdjson::ondemand::number_type::big_integer: {
          // Does not fit into 64 bits; store the raw token as a string.
          out.data(std::string{val.raw_json_token()});
          return;
        }
      }
      TENZIR_UNREACHABLE();
    }
    case simdjson::ondemand::json_type::string: {
      auto str = val.get_string();
      if (str.error()) {
        diagnostic::warning("failed to parse a JSON string").emit(dh);
        out.null();
        return;
      }
      out.data(std::string{str.value_unsafe()});
      return;
    }
    case simdjson::ondemand::json_type::array: {
      auto arr = val.get_array();
      if (arr.error()) {
        diagnostic::warning("failed to parse a JSON array").emit(dh);
        out.null();
        return;
      }
      auto row = out.list();
      // Append elements through the underlying value builder, since the erased
      // `array_builder_<data>` has no `emplace_back`.
      auto& elements = row.value_builder();
      for (auto element : arr.value_unsafe()) {
        if (element.error()) {
          diagnostic::warning("failed to parse a JSON array element").emit(dh);
          elements.null();
          continue;
        }
        parse_value(element.value_unsafe(), elements, dh, depth + 1);
      }
      return;
    }
    case simdjson::ondemand::json_type::object: {
      auto obj = val.get_object();
      if (obj.error()) {
        diagnostic::warning("failed to parse a JSON object").emit(dh);
        out.null();
        return;
      }
      auto row = out.record();
      for (auto pair : obj.value_unsafe()) {
        if (pair.error()) {
          diagnostic::warning("failed to parse a JSON key-value pair").emit(dh);
          continue;
        }
        auto key = pair.unescaped_key();
        if (key.error()) {
          diagnostic::warning("failed to parse a JSON key").emit(dh);
          continue;
        }
        auto value = pair.value();
        if (value.error()) {
          diagnostic::warning("failed to parse a JSON object value").emit(dh);
          continue;
        }
        parse_value(value.value_unsafe(), row.field(key.value_unsafe()), dh,
                    depth + 1);
      }
      return;
    }
    case simdjson::ondemand::json_type::unknown: {
      diagnostic::warning("failed to parse a JSON value").emit(dh);
      out.null();
      return;
    }
  }
  TENZIR_UNREACHABLE();
}

/// Arguments for `read_json2`. The operator currently takes none.
struct ReadJson2Args {};

/// Runtime operator: parses `chunk_ptr` bytes into `tenzir2::TableSlice`.
class ReadJson2 final : public Operator<chunk_ptr, tenzir2::TableSlice> {
public:
  ReadJson2() = default;

  explicit ReadJson2(ReadJson2Args) {
  }

  auto process(chunk_ptr input, Push<tenzir2::TableSlice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input or input->size() == 0) {
      co_return;
    }
    auto& dh = ctx.dh();
    buffer_.append(
      {reinterpret_cast<const char*>(input->data()), input->size()});
    auto builder = tenzir2::array_builder_<tenzir2::record>{};
    auto rows = parse_buffer(builder, dh);
    if (rows > 0) {
      co_await push(make_slice(builder.finish()));
    }
  }

  auto finalize(Push<tenzir2::TableSlice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    auto& dh = ctx.dh();
    if (not buffer_.view().empty()) {
      diagnostic::error("read_json2: input ended with incomplete JSON").emit(dh);
    }
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // Checkpointing is out of scope for this experimental operator.
    TENZIR_TODO();
  }

private:
  auto make_slice(tenzir2::array_<tenzir2::record> data) const
    -> tenzir2::TableSlice {
    return tenzir2::TableSlice{
      std::string{"tenzir.json"},
      tenzir2::clock::now(),
      tenzir2::ProvenanceToken{},
      std::move(data),
    };
  }

  /// Parses all complete documents currently in `buffer_` into `builder`,
  /// retaining any truncated trailing bytes for the next chunk. Returns the
  /// number of rows appended.
  auto parse_buffer(tenzir2::array_builder_<tenzir2::record>& builder,
                    diagnostic_handler& dh) -> size_t {
    auto rows = size_t{0};
    auto retry = false;
    auto completed = size_t{0};
    do {
      retry = false;
      auto view = buffer_.view();
      auto stream = simdjson::ondemand::document_stream{};
      auto err = parser_.iterate_many(view.data(), view.length(), batch_size_)
                   .get(stream);
      if (err) {
        buffer_.reset();
        diagnostic::warning("read_json2: {}", simdjson::error_message(err))
          .note("failed to parse")
          .emit(dh);
        return rows;
      }
      auto current = size_t{0};
      for (auto doc_it = stream.begin(); doc_it != stream.end(); ++doc_it) {
        // Skip documents already processed on a previous (smaller) batch.
        if (current < completed) {
          ++current;
          continue;
        }
        ++current;
        auto doc = (*doc_it).get_value();
        if (auto derr = doc.error()) {
          if (derr == simdjson::CAPACITY) {
            batch_size_ *= 2;
            retry = batch_size_ < max_batch_size;
            if (retry) {
              break;
            }
          }
          diagnostic::error("read_json2: {}", simdjson::error_message(derr))
            .note("found invalid JSON")
            .emit(dh);
          buffer_.reset();
          return rows;
        }
        ++completed;
        auto type = doc.value_unsafe().type();
        if (type.error()
            or type.value_unsafe() != simdjson::ondemand::json_type::object) {
          diagnostic::error("read_json2: expected a JSON object").emit(dh);
          continue;
        }
        auto row = builder.record();
        for (auto pair : doc.value_unsafe().get_object().value_unsafe()) {
          if (pair.error()) {
            diagnostic::warning("read_json2: failed to parse a key-value pair")
              .emit(dh);
            continue;
          }
          auto key = pair.unescaped_key();
          auto value = pair.value();
          if (key.error() or value.error()) {
            diagnostic::warning("read_json2: failed to parse an object entry")
              .emit(dh);
            continue;
          }
          parse_value(value.value_unsafe(), row.field(key.value_unsafe()), dh);
        }
        ++rows;
      }
      if (not retry) {
        handle_truncated(stream);
      }
    } while (retry);
    return rows;
  }

  auto handle_truncated(simdjson::ondemand::document_stream& stream) -> void {
    auto truncated = stream.truncated_bytes();
    auto view = buffer_.view();
    if (truncated > view.size()) {
      buffer_.reset();
      return;
    }
    auto partial_utf8 = detail::count_trailing_partial_utf8(view);
    auto keep = std::min(truncated + partial_utf8, view.size());
    if (keep == 0) {
      buffer_.reset();
      return;
    }
    buffer_.truncate(keep);
  }

  detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'> buffer_;
  simdjson::ondemand::parser parser_;
  size_t batch_size_ = initial_batch_size;
};

/// Registers `read_json2` through the standard `describe()` path. The
/// `Describer<ReadJson2Args, ReadJson2>` constructor deduces the operator's
/// `Operator<chunk_ptr, tenzir2::TableSlice>` element types and wires up the
/// spawn; compilation, substitution, and serialization are handled by the
/// generic `GenericIr` bridge, so no dedicated IR operator is required.
class read_json2_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_json2";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadJson2Args, ReadJson2>{};
    // A source-like parser: act as an optimization barrier and take no filter.
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::read_json2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::read_json2::read_json2_plugin)
