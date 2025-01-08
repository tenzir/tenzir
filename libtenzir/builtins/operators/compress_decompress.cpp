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

namespace arrow::util {
[[maybe_unused]] auto inspect(auto& f, GZipFormat& x) -> bool {
  static_assert(static_cast<int>(GZipFormat::ZLIB) == 0);
  static_assert(static_cast<int>(GZipFormat::DEFLATE) == 1);
  static_assert(static_cast<int>(GZipFormat::GZIP) == 2);
  return ::tenzir::detail::inspect_enum_str(f, x, {"zlib", "deflate", "gzip"});
}
} // namespace arrow::util

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
        TENZIR_ASSERT(size == 0);
      },
      [&](std::vector<uint8_t>& buffer) {
        TENZIR_ASSERT(size <= detail::narrow<int64_t>(buffer.size()));
        if (size == detail::narrow<int64_t>(buffer.size())) {
          buffer_ = {};
          return;
        }
        std::memmove(buffer.data(), buffer.data() + size, buffer.size() - size);
        buffer.resize(buffer.size() - size);
      },
      [&](chunk_ptr& buffer) {
        TENZIR_ASSERT(buffer);
        TENZIR_ASSERT(size <= detail::narrow<int64_t>(buffer->size()));
        if (size == detail::narrow<int64_t>(buffer->size())) {
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
  // used by gzip compress
  located<arrow::util::GZipFormat> gzip_format
    = located{arrow::util::GZipFormat::GZIP, location::unknown};
  // used by gzip & brotli compress
  std::optional<located<int>> window_bits = {};

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("type", x.type), f.field("level", x.level),
              f.field("gzip_format", x.gzip_format),
              f.field("window_bits", x.window_bits));
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
  if (args.type.inner == "gzip") {
    auto opts = arrow::util::GZipCodecOptions{};
    opts.compression_level = compression_level;
    opts.gzip_format = args.gzip_format.inner;
    opts.window_bits = args.window_bits ? std::optional{args.window_bits->inner}
                                        : std::nullopt;
    return arrow::util::Codec::Create(*compression_type, opts);
  }
  if (args.type.inner == "brotli") {
    auto opts = arrow::util::BrotliCodecOptions{};
    opts.compression_level = compression_level;
    opts.window_bits = args.window_bits ? std::optional{args.window_bits->inner}
                                        : std::nullopt;
    return arrow::util::Codec::Create(*compression_type, opts);
  }
  return arrow::util::Codec::Create(*compression_type, compression_level);
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
          detail::narrow<int64_t>(out_buffer.size()), out_buffer.data());
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
          if (out_buffer.size() < out_buffer.max_size() / 2) {
            out_buffer.resize(out_buffer.size() * 2);
          } else {
            out_buffer.resize(out_buffer.max_size());
          }
        } else {
          in_buffer.drop_front_n(result->bytes_read);
        }
        if (result->bytes_written > 0) {
          TENZIR_ASSERT(result->bytes_written
                        <= detail::narrow<int64_t>(out_buffer.size()));
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
        detail::narrow<int64_t>(out_buffer.size()), out_buffer.data());
      if (result->should_retry) {
        TENZIR_ASSERT(result->bytes_written == 0);
        if (out_buffer.size() == out_buffer.max_size()) [[unlikely]] {
          diagnostic::error("failed to resize buffer").emit(ctrl.diagnostics());
          co_return;
        }
        if (out_buffer.size() < out_buffer.max_size() / 2) {
          out_buffer.resize(out_buffer.size() * 2);
        } else {
          out_buffer.resize(out_buffer.max_size());
        }
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
          detail::narrow<int64_t>(out_buffer.size()), out_buffer.data());
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
          if (out_buffer.size() < out_buffer.max_size() / 2) {
            out_buffer.resize(out_buffer.size() * 2);
          } else {
            out_buffer.resize(out_buffer.max_size());
          }
        }
        if (result->bytes_written > 0) {
          TENZIR_ASSERT(result->bytes_written
                        <= detail::narrow<int64_t>(out_buffer.size()));
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

auto get_extensions(std::string_view method_name) -> std::vector<std::string> {
  if (method_name == "brotli") {
    return {"br", "brotli"};
  } else if (method_name == "bz2") {
    return {"bz2"};
  } else if (method_name == "gzip") {
    return {"gz", "gzip"};
  } else if (method_name == "lz4") {
    return {"lz4"};
  } else if (method_name == "zstd") {
    return {"zst", "zstd"};
  }
  return {};
}

class compress_plugin final : public virtual operator_plugin<compress_operator>,
                              public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto name() const -> std::string override {
    return method_name_.empty() ? "compress" : "compress_" + method_name_;
  }

  compress_plugin() = default;
  compress_plugin(std::string method_name)
    : method_name_{std::move(method_name)} {
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
    auto args = operator_args{
      .type = {method_name_, location::unknown},
    };
    auto level = std::optional<located<int64_t>>{};
    auto parser = argument_parser2::operator_(name());
    if (method_name_.empty()) {
      parser.positional("type", args.type);
    }
    parser.named("level", level);
    auto gzip_format_string = std::optional<located<std::string>>{};
    if (method_name_ == "gzip") {
      parser.named("format", gzip_format_string);
    }
    auto window_bits = std::optional<located<uint64_t>>{};
    if (method_name_ == "gzip" or method_name_ == "brotli") {
      parser.named("window_bits", window_bits);
    }
    TRY(parser.parse(inv, ctx));
    if (method_name_.empty()) {
      diagnostic::warning(R"(`{} "{}"` is deprecated)", name(), args.type.inner)
        .hint("use `{}_{}` instead", name(), args.type.inner)
        .primary(inv.self)
        .emit(ctx);
    }
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
    if (window_bits) {
      args.window_bits.emplace(static_cast<int>(window_bits->inner),
                               window_bits->source);
    }
    if (gzip_format_string) {
      if (gzip_format_string->inner == "zlib") {
        args.gzip_format
          = located{arrow::util::GZipFormat::ZLIB, gzip_format_string->source};
      } else if (gzip_format_string->inner == "deflate") {
        args.gzip_format = located{arrow::util::GZipFormat::DEFLATE,
                                   gzip_format_string->source};
      } else if (gzip_format_string->inner == "gzip") {
        args.gzip_format
          = located{arrow::util::GZipFormat::GZIP, gzip_format_string->source};
      } else {
        diagnostic::error("`format` must be one of `zlib`, `deflate` or `gzip`")
          .primary(gzip_format_string->source)
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<compress_operator>(std::move(args));
  }

  auto compress_properties() const -> compress_properties_t override {
    return {.extensions = get_extensions(method_name_)};
  }

private:
  std::string method_name_;
};

class decompress_plugin final
  : public virtual operator_plugin<decompress_operator>,
    public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  decompress_plugin() = default;

  decompress_plugin(std::string method_name)
    : method_name_{std::move(method_name)} {
  }

  auto name() const -> std::string override {
    return method_name_.empty() ? "decompress" : "decompress_" + method_name_;
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
    auto args = operator_args{
      .type = {method_name_, location::unknown},
    };
    auto parser = argument_parser2::operator_(name());
    if (method_name_.empty()) {
      parser.positional("type", args.type);
    }
    TRY(parser.parse(inv, ctx));
    if (method_name_.empty()) {
      diagnostic::warning(R"(`{} "{}"` is deprecated)", name(), args.type.inner)
        .hint("use `{}_{}` instead", name(), args.type.inner)
        .primary(inv.self)
        .emit(ctx);
    }
    return std::make_unique<decompress_operator>(std::move(args));
  }

  auto decompress_properties() const -> decompress_properties_t override {
    return {.extensions = get_extensions(method_name_)};
  }

private:
  std::string method_name_;
};

} // namespace

} // namespace tenzir::plugins::compress_decompress

TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::compress_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::compress_plugin{
  "brotli"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::compress_plugin{
  "bz2"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::compress_plugin{
  "gzip"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::compress_plugin{
  "lz4"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::compress_plugin{
  "zstd"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::decompress_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::decompress_plugin{
  "brotli"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::decompress_plugin{
  "bz2"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::decompress_plugin{
  "gzip"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::decompress_plugin{
  "lz4"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::compress_decompress::decompress_plugin{
  "zstd"})
