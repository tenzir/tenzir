//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>
#include <arrow/util/compression.h>

namespace tenzir::plugins::compress_decompress {

namespace {

/// An adaptable input byte buffer that only copies from chunks when necessary,
/// and has a fallback buffer for lazily merging chunks if necessary.
struct input_buffer {
public:
  auto data() const -> const uint8_t* {
    auto f = detail::overload{
      [](std::monostate) -> const uint8_t* {
        return nullptr;
      },
      [](const std::vector<uint8_t>& buffer) {
        return buffer.data();
      },
      [](const chunk_ptr& buffer) {
        TENZIR_ASSERT(buffer);
        return reinterpret_cast<const uint8_t*>(buffer->data());
      },
    };
    return std::visit(f, buffer_);
  }

  auto size() const -> int64_t {
    auto f = detail::overload{
      [](std::monostate) {
        return size_t{0};
      },
      [](const std::vector<uint8_t>& buffer) {
        return buffer.size();
      },
      [](const chunk_ptr& buffer) {
        TENZIR_ASSERT(buffer);
        return buffer->size();
      },
    };
    return detail::narrow_cast<int64_t>(std::visit(f, buffer_));
  }

  auto consume(chunk_ptr chunk) -> void {
    auto f = detail::overload{
      [&](std::monostate) {
        buffer_ = std::move(chunk);
      },
      [&](std::vector<uint8_t>& buffer) {
        buffer.reserve(buffer.size() + chunk->size());
        std::copy_n(reinterpret_cast<const uint8_t*>(chunk->data()),
                    chunk->size(), std::back_inserter(buffer));
      },
      [&](const chunk_ptr& buffer) {
        TENZIR_ASSERT(buffer);
        auto& new_buffer = buffer_.emplace<std::vector<uint8_t>>();
        new_buffer.reserve(buffer->size() + chunk->size());
        std::copy_n(reinterpret_cast<const uint8_t*>(buffer->data()),
                    buffer->size(), std::back_inserter(new_buffer));
        std::copy_n(reinterpret_cast<const uint8_t*>(chunk->data()),
                    chunk->size(), std::back_inserter(new_buffer));
      },
    };
    std::visit(f, buffer_);
  }

  auto drop_front_n(int64_t size) -> void {
    auto f = detail::overload{
      [&](std::monostate) {
        TENZIR_ASSERT(size == 0);
      },
      [&](std::vector<uint8_t>& buffer) {
        TENZIR_ASSERT(size <= detail::narrow_cast<int64_t>(buffer.size()));
        if (size == detail::narrow_cast<int64_t>(buffer.size())) {
          buffer_ = {};
          return;
        }
        std::memmove(buffer.data(), buffer.data() + size, buffer.size() - size);
        buffer.resize(buffer.size() - size);
      },
      [&](chunk_ptr& buffer) {
        TENZIR_ASSERT(buffer);
        TENZIR_ASSERT(size <= detail::narrow_cast<int64_t>(buffer->size()));
        if (size == detail::narrow_cast<int64_t>(buffer->size())) {
          buffer_ = {};
          return;
        }
        buffer = buffer->slice(size);
      },
    };
    return std::visit(f, buffer_);
  }

private:
  std::variant<std::monostate, std::vector<uint8_t>, chunk_ptr> buffer_ = {};
};

struct operator_args {
  located<std::string> type = {};
  std::optional<located<int>> level = {};
  // TODO: gzip has some further options, which we should also cover.

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("type", x.type), f.field("level", x.level));
  }
};

auto codec_from_args(const operator_args& args)
  -> arrow::Result<std::shared_ptr<arrow::util::Codec>> {
  auto compression_type
    = arrow::util::Codec::GetCompressionType(args.type.inner);
  // Arrow straight up crashes if we use a codec created from the string
  // "uncompressed", so we just don't do that. Last checked with Arrow 12.0.
  if (not compression_type.ok()
      or *compression_type == arrow::Compression::UNCOMPRESSED) {
    return arrow::Status::Invalid(
      fmt::format("failed to get compression type `{}`; must be one of "
                  "`brotli`, `bz2`, `gzip`, `lz4`, `zstd`",
                  args.type.inner));
  }
  const auto compression_level
    = args.level ? args.level->inner : arrow::util::kUseDefaultCompressionLevel;
  auto codec = arrow::util::Codec::Create(*compression_type, compression_level);
  return codec;
}

class compress_operator final : public crtp_operator<compress_operator> {
public:
  compress_operator() = default;

  explicit compress_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto codec = codec_from_args(args_);
    if (not codec.ok()) {
      diagnostic::error("failed to create codec for compression type `{}`: {}",
                        args_.type.inner, codec.status().ToString())
        .primary(args_.type.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto compressor = codec.ValueUnsafe()->MakeCompressor();
    if (not compressor.ok()) {
      diagnostic::error("failed to create compressor: {}",
                        compressor.status().ToString())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto out_buffer = std::vector<uint8_t>{};
    out_buffer.resize(1 << 20);
    auto in_buffer = input_buffer{};
    for (auto&& bytes : input) {
      if (not bytes) {
        co_yield {};
        continue;
      }
      in_buffer.consume(std::move(bytes));
      while (in_buffer.size() > 0) {
        auto result = compressor.ValueUnsafe()->Compress(
          in_buffer.size(), in_buffer.data(),
          detail::narrow_cast<int64_t>(out_buffer.size()), out_buffer.data());
        if (not result.ok()) {
          diagnostic::error("failed to compress: {}",
                            result.status().ToString())
            .emit(ctrl.diagnostics());
          co_return;
        }
        // Some compressors signal that they need a larger output buffer to
        // write into. We already start with a rather large one, but in case
        // that isn't enough we just double the size until the compressor
        // stops complaining.
        if (result->bytes_read == 0) [[unlikely]] {
          if (out_buffer.size() == out_buffer.max_size()) [[unlikely]] {
            diagnostic::error("failed to resize buffer")
              .emit(ctrl.diagnostics());
            co_return;
          }
          out_buffer.resize(
            std::min(out_buffer.max_size(), out_buffer.size() * 2));
        } else {
          in_buffer.drop_front_n(result->bytes_read);
        }
        if (result->bytes_written > 0) {
          TENZIR_ASSERT(result->bytes_written
                        <= detail::narrow_cast<int64_t>(out_buffer.size()));
          co_yield chunk::copy(
            as_bytes(out_buffer).subspan(0, result->bytes_written));
        } else {
          break;
        }
      }
    }
    // In case the input contains multiple concatenated compressed streams,
    // we gracefully reset the compressor.
    while (true) {
      auto result = compressor.ValueUnsafe()->End(
        detail::narrow_cast<int64_t>(out_buffer.size()), out_buffer.data());
      if (result->should_retry) {
        TENZIR_ASSERT(result->bytes_written == 0);
        if (out_buffer.size() == out_buffer.max_size()) [[unlikely]] {
          diagnostic::error("failed to resize buffer").emit(ctrl.diagnostics());
          co_return;
        }
        out_buffer.resize(
          std::min(out_buffer.max_size(), out_buffer.size() * 2));
        continue;
      }
      if (result->bytes_written > 0) {
        out_buffer.resize(result->bytes_written);
        co_yield chunk::make(std::move(out_buffer));
      }
      break;
    }
  }

  auto name() const -> std::string override {
    return "compress";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, compress_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_ = {};
};

class decompress_operator final : public crtp_operator<decompress_operator> {
public:
  decompress_operator() = default;

  explicit decompress_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto codec = codec_from_args(args_);
    if (not codec.ok()) {
      diagnostic::error("failed to create codec for compression type `{}`: {}",
                        args_.type.inner, codec.status().ToString())
        .primary(args_.type.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto decompressor = codec.ValueUnsafe()->MakeDecompressor();
    if (not decompressor.ok()) {
      diagnostic::error("failed to create decompressor: {}",
                        decompressor.status().ToString())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto out_buffer = std::vector<uint8_t>{};
    out_buffer.resize(1 << 20);
    auto in_buffer = input_buffer{};
    for (auto&& bytes : input) {
      if (not bytes) {
        co_yield {};
        continue;
      }
      in_buffer.consume(std::move(bytes));
      while (in_buffer.size() > 0) {
        auto result = decompressor.ValueUnsafe()->Decompress(
          in_buffer.size(), in_buffer.data(),
          detail::narrow_cast<int64_t>(out_buffer.size()), out_buffer.data());
        if (not result.ok()) {
          diagnostic::error("failed to decompress: {}",
                            result.status().ToString())
            .emit(ctrl.diagnostics());
          co_return;
        }
        in_buffer.drop_front_n(result->bytes_read);
        // Some decompressors signal that they need a larger output buffer to
        // write into. We already start with a rather large one, but in case
        // that isn't enough we just double the size until the decompressor
        // stops complaining.
        if (result->need_more_output) [[unlikely]] {
          if (out_buffer.size() == out_buffer.max_size()) [[unlikely]] {
            diagnostic::error("failed to resize buffer")
              .emit(ctrl.diagnostics());
            co_return;
          }
          out_buffer.resize(
            std::min(out_buffer.max_size(), out_buffer.size() * 2));
        }
        if (result->bytes_written > 0) {
          TENZIR_ASSERT(result->bytes_written
                        <= detail::narrow_cast<int64_t>(out_buffer.size()));
          co_yield chunk::copy(
            as_bytes(out_buffer).subspan(0, result->bytes_written));
        } else {
          break;
        }
        // In case the input contains multiple concatenated compressed streams,
        // we gracefully reset the decompressor.
        if (decompressor.ValueUnsafe()->IsFinished()) [[unlikely]] {
          const auto reset_status = decompressor.ValueUnsafe()->Reset();
          if (not reset_status.ok()) {
            diagnostic::error("failed to reset decompressor: {}",
                              reset_status.ToString())
              .emit(ctrl.diagnostics());
            co_return;
          }
        }
      }
    }
    if (not decompressor.ValueUnsafe()->IsFinished()) {
      TENZIR_VERBOSE(
        "decompressor is not finished, but end of input is reached");
    }
  }

  auto name() const -> std::string override {
    return "decompress";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, decompress_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  operator_args args_ = {};
};

class compress_plugin final : public virtual operator_plugin<compress_operator>,
                              public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"compress",
                                  "https://docs.tenzir.com/operators/compress"};
    auto args = operator_args{};
    parser.add(args.type, "<type>");
    parser.add("--level", args.level, "<level>");
    parser.parse(p);
    return std::make_unique<compress_operator>(std::move(args));
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = operator_args{};
    auto level = std::optional<located<int64_t>>{};
    TRY(argument_parser2::operator_(name())
          .add(args.type, "<type>")
          .add("level", level)
          .parse(inv, ctx));
    // TODO: Where is `try_narrow`?
    using T = decltype(args.level->inner);
    if (level) {
      if (std::numeric_limits<T>::lowest() <= level->inner
          && level->inner <= std::numeric_limits<T>::max()) {
        args.level->inner = detail::narrow<T>(level->inner);
      } else {
        diagnostic::error("invalid compression level: `{}`", level->inner)
          .primary(*level)
          .emit(ctx);
      }
    }
    return std::make_unique<compress_operator>(std::move(args));
  }
};

class decompress_plugin final
  : public virtual operator_plugin<decompress_operator>,
    public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"decompress", "https://docs.tenzir.com/"
                                                "operators/decompress"};
    auto args = operator_args{};
    parser.add(args.type, "<type>");
    parser.parse(p);
    return std::make_unique<decompress_operator>(std::move(args));
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = operator_args{};
    TRY(argument_parser2::operator_(name())
          .add(args.type, "<type>")
          .parse(inv, ctx));
    return std::make_unique<decompress_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::compress_decompress

TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::compress_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::decompress_plugin)
