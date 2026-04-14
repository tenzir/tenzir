//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/narrow.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>

#include <arrow/util/compression.h>

#include <utility>

namespace tenzir::plugins::compress_decompress2 {

namespace {

auto isize(auto&& container) -> int64_t {
  return detail::narrow<int64_t>(container.size());
}

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
    return detail::narrow<int64_t>(std::visit(f, buffer_));
  }

  auto consume(chunk_ptr chunk) -> void {
    TENZIR_ASSERT(chunk);
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
        auto new_buffer = std::vector<uint8_t>();
        new_buffer.reserve(buffer->size() + chunk->size());
        std::copy_n(reinterpret_cast<const uint8_t*>(buffer->data()),
                    buffer->size(), std::back_inserter(new_buffer));
        std::copy_n(reinterpret_cast<const uint8_t*>(chunk->data()),
                    chunk->size(), std::back_inserter(new_buffer));
        buffer_ = std::move(new_buffer);
      },
    };
    std::visit(f, buffer_);
  }

  auto drop_front_n(int64_t size) -> void {
    auto f = detail::overload{
      [&](std::monostate) {
        TENZIR_ASSERT_EQ(size, 0);
      },
      [&](std::vector<uint8_t>& buffer) {
        TENZIR_ASSERT_LEQ(size, isize(buffer));
        if (size == isize(buffer)) {
          buffer_ = {};
          return;
        }
        std::memmove(buffer.data(), buffer.data() + size, buffer.size() - size);
        buffer.resize(buffer.size() - size);
      },
      [&](chunk_ptr& buffer) {
        TENZIR_ASSERT(buffer);
        TENZIR_ASSERT_LEQ(size, detail::narrow<int64_t>(buffer->size()));
        if (size == detail::narrow<int64_t>(buffer->size())) {
          buffer_ = {};
          return;
        }
        buffer = buffer->slice(size);
      },
    };
    std::visit(f, buffer_);
  }

private:
  std::variant<std::monostate, std::vector<uint8_t>, chunk_ptr> buffer_;
};

struct CompressArgs {
  arrow::Compression::type codec = arrow::Compression::UNCOMPRESSED;
  Option<int64_t> level;
  Option<std::string> gzip_format;
  Option<uint64_t> window_bits;
  location operator_location = location::unknown;
};

struct DecompressArgs {
  arrow::Compression::type codec = arrow::Compression::UNCOMPRESSED;
  Option<std::string> gzip_format;
  location operator_location = location::unknown;
};

/// Doubles the buffer size, capping at max_size. Returns false if already at
/// max_size.
auto try_grow(std::vector<uint8_t>& buffer) -> bool {
  if (buffer.size() == buffer.max_size()) [[unlikely]] {
    return false;
  }
  if (buffer.size() < buffer.max_size() / 2) {
    buffer.resize(buffer.size() * 2);
  } else {
    buffer.resize(buffer.max_size());
  }
  return true;
}

/// Creates an Arrow codec from CompressArgs, applying codec-specific options
/// for gzip and brotli (format, window_bits).
auto codec_from_compress_args(const CompressArgs& args)
  -> arrow::Result<std::shared_ptr<arrow::util::Codec>> {
  const auto compression_level = args.level
                                   ? detail::narrow<int>(*args.level)
                                   : arrow::util::kUseDefaultCompressionLevel;
  if (args.codec == arrow::Compression::GZIP) {
    auto opts = arrow::util::GZipCodecOptions{};
    opts.compression_level = compression_level;
    // Convert gzip_format string to enum, defaulting to GZIP.
    if (args.gzip_format) {
      if (*args.gzip_format == "zlib") {
        opts.gzip_format = arrow::util::GZipFormat::ZLIB;
      } else if (*args.gzip_format == "deflate") {
        opts.gzip_format = arrow::util::GZipFormat::DEFLATE;
      } else {
        opts.gzip_format = arrow::util::GZipFormat::GZIP;
      }
    } else {
      opts.gzip_format = arrow::util::GZipFormat::GZIP;
    }
    opts.window_bits = args.window_bits
                         ? std::optional{detail::narrow<int>(*args.window_bits)}
                         : std::nullopt;
    return arrow::util::Codec::Create(args.codec, opts);
  }
  if (args.codec == arrow::Compression::BROTLI) {
    auto opts = arrow::util::BrotliCodecOptions{};
    opts.compression_level = compression_level;
    opts.window_bits = args.window_bits
                         ? std::optional{detail::narrow<int>(*args.window_bits)}
                         : std::nullopt;
    return arrow::util::Codec::Create(args.codec, opts);
  }
  return arrow::util::Codec::Create(args.codec, compression_level);
}

/// Creates an Arrow codec from DecompressArgs, applying gzip format options
/// when applicable.
auto codec_from_decompress_args(const DecompressArgs& args)
  -> arrow::Result<std::shared_ptr<arrow::util::Codec>> {
  if (args.codec == arrow::Compression::GZIP) {
    auto opts = arrow::util::GZipCodecOptions{};
    if (args.gzip_format) {
      if (*args.gzip_format == "zlib") {
        opts.gzip_format = arrow::util::GZipFormat::ZLIB;
      } else if (*args.gzip_format == "deflate") {
        opts.gzip_format = arrow::util::GZipFormat::DEFLATE;
      } else {
        opts.gzip_format = arrow::util::GZipFormat::GZIP;
      }
    } else {
      opts.gzip_format = arrow::util::GZipFormat::GZIP;
    }
    return arrow::util::Codec::Create(args.codec, opts);
  }
  return arrow::util::Codec::Create(args.codec);
}

class Compress final : public Operator<chunk_ptr, chunk_ptr> {
public:
  explicit Compress(CompressArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx&) -> Task<void> override {
    auto codec = codec_from_compress_args(args_);
    TENZIR_ASSERT_ALWAYS(codec.ok(), codec.status().ToString());
    auto compressor = codec.ValueUnsafe()->MakeCompressor();
    TENZIR_ASSERT_ALWAYS(compressor.ok(), compressor.status().ToString());
    compressor_ = std::move(compressor).ValueUnsafe();
    out_buffer_.resize(1 << 20);
    co_return;
  }

  auto process(chunk_ptr input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input or input->size() == 0) {
      // Forward empty chunks as boundary markers.
      co_await push(std::move(input));
      co_return;
    }
    in_buffer_.consume(std::move(input));
    // Feed all buffered input into the compressor.
    while (in_buffer_.size() > 0) {
      auto result
        = compressor_->Compress(in_buffer_.size(), in_buffer_.data(),
                                isize(out_buffer_), out_buffer_.data());
      if (not result.ok()) {
        diagnostic::error("compression failure: {}", result.status().ToString())
          .primary(args_.operator_location)
          .emit(ctx);
        co_return;
      }
      if (result->bytes_written > 0) {
        co_await push(chunk::copy(std::span{
          out_buffer_.data(), detail::narrow<size_t>(result->bytes_written)}));
      }
      in_buffer_.drop_front_n(result->bytes_read);
      // If no input was consumed, the output buffer is too small.
      if (result->bytes_read == 0) {
        if (not try_grow(out_buffer_)) {
          diagnostic::error("failed to grow compression output buffer")
            .primary(args_.operator_location)
            .emit(ctx);
          co_return;
        }
      }
    }
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    // End the compressor stream, flushing all remaining data.
    auto should_retry = true;
    while (should_retry) {
      auto result = compressor_->End(isize(out_buffer_), out_buffer_.data());
      if (not result.ok()) {
        diagnostic::error("compression finalization failure: {}",
                          result.status().ToString())
          .primary(args_.operator_location)
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      if (result->bytes_written > 0) {
        co_await push(chunk::copy(std::span{
          out_buffer_.data(), detail::narrow<size_t>(result->bytes_written)}));
      }
      should_retry = result->should_retry;
      if (should_retry) {
        if (not try_grow(out_buffer_)) {
          diagnostic::error("failed to grow compression output buffer")
            .primary(args_.operator_location)
            .emit(ctx);
          co_return FinalizeBehavior::done;
        }
      }
    }
    co_return FinalizeBehavior::done;
  }

private:
  CompressArgs args_;
  std::shared_ptr<arrow::util::Compressor> compressor_;
  std::vector<uint8_t> out_buffer_;
  input_buffer in_buffer_;
};

class Decompress final : public Operator<chunk_ptr, chunk_ptr> {
public:
  explicit Decompress(DecompressArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx&) -> Task<void> override {
    auto codec = codec_from_decompress_args(args_);
    TENZIR_ASSERT_ALWAYS(codec.ok(), codec.status().ToString());
    auto decompressor = codec.ValueUnsafe()->MakeDecompressor();
    TENZIR_ASSERT_ALWAYS(decompressor.ok(), decompressor.status().ToString());
    decompressor_ = std::move(decompressor).ValueUnsafe();
    out_buffer_.resize(1 << 20);
    co_return;
  }

  auto process(chunk_ptr input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input or input->size() == 0) {
      // Forward empty chunks as boundary markers.
      co_await push(std::move(input));
      co_return;
    }
    received_input_ = true;
    in_buffer_.consume(std::move(input));
    // If the previous process() call ended with a finished decompressor
    // (member boundary aligned with chunk boundary), reset for the next
    // concatenated member.
    if (decompressor_->IsFinished()) {
      auto status = decompressor_->Reset();
      if (not status.ok()) {
        diagnostic::error("failed to reset decompressor: {}", status.ToString())
          .primary(args_.operator_location)
          .emit(ctx);
        co_return;
      }
    }
    // Feed all buffered input into the decompressor, then drain any remaining
    // internal output (the decompressor may buffer output even after all input
    // is consumed, signaled via need_more_output).
    auto need_more_output = false;
    while (in_buffer_.size() > 0 or need_more_output) {
      auto result
        = decompressor_->Decompress(in_buffer_.size(), in_buffer_.data(),
                                    isize(out_buffer_), out_buffer_.data());
      if (not result.ok()) {
        diagnostic::error("decompression failure: {}",
                          result.status().ToString())
          .primary(args_.operator_location)
          .emit(ctx);
        co_return;
      }
      if (result->bytes_written > 0) {
        auto span = std::span{
          out_buffer_.data(),
          detail::narrow<size_t>(result->bytes_written),
        };
        co_await push(chunk::copy(span));
      }
      in_buffer_.drop_front_n(result->bytes_read);
      need_more_output = result->need_more_output;
      if (need_more_output) {
        // The decompressor needs a larger output buffer.
        if (result->bytes_written == 0) {
          if (not try_grow(out_buffer_)) {
            diagnostic::error("failed to grow decompression output buffer")
              .primary(args_.operator_location)
              .emit(ctx);
            co_return;
          }
        }
        continue;
      }
      // Support concatenated compressed streams by resetting the decompressor.
      // Only reset if there is remaining input; otherwise, preserve the
      // finished state so that finalize() can distinguish a complete stream
      // from a truncated one.
      if (decompressor_->IsFinished()) {
        if (in_buffer_.size() == 0) {
          break;
        }
        auto status = decompressor_->Reset();
        if (not status.ok()) {
          diagnostic::error("failed to reset decompressor: {}",
                            status.ToString())
            .primary(args_.operator_location)
            .emit(ctx);
          co_return;
        }
      }
      // If neither input was consumed nor output was produced, we're stuck.
      if (result->bytes_read == 0 and result->bytes_written == 0) {
        diagnostic::error("decompression made no progress")
          .primary(args_.operator_location)
          .emit(ctx);
        co_return;
      }
    }
  }

  auto finalize(Push<chunk_ptr>&, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (in_buffer_.size() > 0) {
      diagnostic::warning("decompression ended with unconsumed input")
        .primary(args_.operator_location)
        .emit(ctx);
    } else if (received_input_ and not decompressor_->IsFinished()) {
      // IsFinished() is a heuristic: true guarantees the stream ended, but
      // false may mean the codec cannot report it.
      TENZIR_DEBUG("decompressor is not finished at end of input");
    }
    co_return FinalizeBehavior::done;
  }

private:
  DecompressArgs args_;
  std::shared_ptr<arrow::util::Decompressor> decompressor_;
  std::vector<uint8_t> out_buffer_;
  input_buffer in_buffer_;
  bool received_input_ = false;
};

class compress_plugin2 final : public virtual OperatorPlugin {
public:
  compress_plugin2() = default;

  explicit compress_plugin2(std::string method_name)
    : method_name_{std::move(method_name)} {
    auto result = arrow::util::Codec::GetCompressionType(method_name_);
    TENZIR_ASSERT_ALWAYS(result.ok(), result.status().ToString());
    compression_type_ = *result;
  }

  auto name() const -> std::string override {
    TENZIR_ASSERT_ALWAYS(not method_name_.empty());
    return "tql2.compress_" + method_name_;
  }

  auto describe() const -> Description override {
    TENZIR_ASSERT_ALWAYS(not method_name_.empty());
    auto d = Describer<CompressArgs, Compress>{CompressArgs{
      .codec = compression_type_,
      .level = {},
      .gzip_format = {},
      .window_bits = {},
    }};
    d.operator_location(&CompressArgs::operator_location);
    auto level = d.named("level", &CompressArgs::level);
    auto format = std::optional<Argument<CompressArgs, std::string>>{};
    auto window_bits = std::optional<Argument<CompressArgs, uint64_t>>{};
    if (method_name_ == "gzip") {
      format = d.named("format", &CompressArgs::gzip_format);
    }
    if (method_name_ == "gzip" or method_name_ == "brotli") {
      window_bits = d.named("window_bits", &CompressArgs::window_bits);
    }
    d.validate([level, format, window_bits,
                codec = compression_type_](DescribeCtx& ctx) -> Empty {
      if (auto val = ctx.get(level)) {
        if (*val < std::numeric_limits<int>::lowest()
            or *val > std::numeric_limits<int>::max()) {
          diagnostic::error("invalid compression level: `{}`", *val)
            .primary(ctx.get_location(level).value())
            .emit(ctx);
          return {};
        }
      }
      if (window_bits) {
        if (auto val = ctx.get(*window_bits)) {
          if (*val > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            diagnostic::error("invalid window_bits: `{}`", *val)
              .primary(ctx.get_location(*window_bits).value())
              .emit(ctx);
            return {};
          }
        }
      }
      if (format) {
        if (auto val = ctx.get(*format)) {
          if (*val != "zlib" and *val != "deflate" and *val != "gzip") {
            diagnostic::error(
              "`format` must be one of `zlib`, `deflate` or `gzip`")
              .primary(ctx.get_location(*format).value())
              .emit(ctx);
            return {};
          }
        }
      }
      // Try creating the codec to catch any remaining errors early.
      auto args = CompressArgs{
        .codec = codec,
        .level = ctx.get(level),
        .gzip_format = format ? ctx.get(*format) : std::nullopt,
        .window_bits = window_bits ? ctx.get(*window_bits) : std::nullopt,
      };
      auto result = codec_from_compress_args(args);
      if (not result.ok()) {
        diagnostic::error("failed to create codec: {}",
                          result.status().ToString())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

private:
  std::string method_name_;
  arrow::Compression::type compression_type_ = arrow::Compression::UNCOMPRESSED;
};

class decompress_plugin2 final : public virtual OperatorPlugin {
public:
  decompress_plugin2() = default;

  explicit decompress_plugin2(std::string method_name)
    : method_name_{std::move(method_name)} {
    auto result = arrow::util::Codec::GetCompressionType(method_name_);
    TENZIR_ASSERT_ALWAYS(result.ok(), result.status().ToString());
    compression_type_ = *result;
  }

  auto name() const -> std::string override {
    TENZIR_ASSERT_ALWAYS(not method_name_.empty());
    return "tql2.decompress_" + method_name_;
  }

  auto describe() const -> Description override {
    TENZIR_ASSERT_ALWAYS(not method_name_.empty());
    auto d = Describer<DecompressArgs, Decompress>{DecompressArgs{
      .codec = compression_type_,
      .gzip_format = {},
    }};
    d.operator_location(&DecompressArgs::operator_location);
    auto format = std::optional<Argument<DecompressArgs, std::string>>{};
    if (method_name_ == "gzip") {
      format = d.named("format", &DecompressArgs::gzip_format);
    }
    d.validate([format, codec = compression_type_](DescribeCtx& ctx) -> Empty {
      if (format) {
        if (auto val = ctx.get(*format)) {
          if (*val != "zlib" and *val != "deflate" and *val != "gzip") {
            diagnostic::error(
              "`format` must be one of `zlib`, `deflate` or `gzip`")
              .primary(ctx.get_location(*format).value())
              .emit(ctx);
            return {};
          }
        }
      }
      auto args = DecompressArgs{
        .codec = codec,
        .gzip_format = format ? ctx.get(*format) : std::nullopt,
      };
      auto result = codec_from_decompress_args(args);
      if (not result.ok()) {
        diagnostic::error("failed to create codec: {}",
                          result.status().ToString())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

private:
  std::string method_name_;
  arrow::Compression::type compression_type_ = arrow::Compression::UNCOMPRESSED;
};

} // namespace

} // namespace tenzir::plugins::compress_decompress2

using namespace tenzir::plugins::compress_decompress2;

TENZIR_REGISTER_PLUGIN(compress_plugin2{"brotli"})
TENZIR_REGISTER_PLUGIN(compress_plugin2{"bz2"})
TENZIR_REGISTER_PLUGIN(compress_plugin2{"gzip"})
TENZIR_REGISTER_PLUGIN(compress_plugin2{"lz4"})
TENZIR_REGISTER_PLUGIN(compress_plugin2{"zstd"})
TENZIR_REGISTER_PLUGIN(decompress_plugin2{"brotli"})
TENZIR_REGISTER_PLUGIN(decompress_plugin2{"bz2"})
TENZIR_REGISTER_PLUGIN(decompress_plugin2{"gzip"})
TENZIR_REGISTER_PLUGIN(decompress_plugin2{"lz4"})
TENZIR_REGISTER_PLUGIN(decompress_plugin2{"zstd"})
