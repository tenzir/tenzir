//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/util/utf8.h>

#include <filesystem>

namespace tenzir::plugins::file_contents {

namespace {

struct file_contents final : public function_plugin {
  auto name() const -> std::string override {
    return "file_contents";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto path = located<std::string>{};
    auto binary = std::optional<location>{};
    TRY(argument_parser2::function(name())
          .positional("path", path)
          .named("binary", binary)
          .parse(inv, ctx));
    if (path.inner.empty()) {
      diagnostic::error("`path` must not be empty").primary(path).emit(ctx);
      return failure::promise();
    }
    const auto fpath = std::filesystem::path(path.inner);
    if (fpath.is_relative()) {
      diagnostic::error("`path` must be an absolute path")
        .primary(path)
        .emit(ctx);
      return failure::promise();
    }
    auto fs = arrow::fs::LocalFileSystem{};
    const auto info = fs.GetFileInfo(path.inner);
    if (not info.ok()) {
      diagnostic::error("could not get file info for `{}`: {}", path.inner,
                        info.status().message())
        .primary(path)
        .emit(ctx);
      return failure::promise();
    }
    const auto size = info->size();
    if (size < 0) {
      diagnostic::error("could not get size of file `{}`", path.inner)
        .primary(path)
        .hint("check if the file exists")
        .emit(ctx);
      return failure::promise();
    }
    if (size == 0) {
      diagnostic::error("cannot read file `{}` of size 0", path.inner)
        .primary(path)
        .emit(ctx);
      return failure::promise();
    }
    if (size > 10'000'000) {
      diagnostic::error("file `{}` is bigger than 10MB", path.inner)
        .primary(path)
        .note("`file_contents()` does not allow reading big files as a safety "
              "check")
        .emit(ctx);
      return failure::promise();
    }
    auto ifs = fs.OpenInputStream(path.inner);
    if (not ifs.ok()) {
      diagnostic::error("could not open input file stream for `{}`: {}",
                        path.inner, ifs.status().message())
        .primary(path)
        .emit(ctx);
      return failure::promise();
    }
    auto content = blob{};
    content.resize(size);
    const auto result
      = ifs.ValueUnsafe()->Read(size, reinterpret_cast<void*>(content.data()));
    if (not result.ok()) {
      diagnostic::error("could not read input file stream for `{}`: {}",
                        path.inner, result.status().message())
        .primary(path)
        .emit(ctx);
      return failure::promise();
    }
    const auto valid_utf8 = arrow::util::ValidateUTF8(
      reinterpret_cast<const uint8_t*>(content.data()), size);
    if (not binary and not valid_utf8) {
      diagnostic::error("file '{}' holds invalid UTF-8", path.inner)
        .primary(path)
        .hint("use `binary=true` to read contents as a `blob`")
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([content = std::move(content),
                               binary](evaluator eval, session) -> series {
      if (binary) {
        auto b = series_builder{type{blob_type{}}};
        for (auto i = int64_t{0}; i < eval.length(); ++i) {
          b.data(content);
        }
        return b.finish_assert_one_array();
      }
      auto b = series_builder{type{string_type{}}};
      const auto view = std::string_view{
        reinterpret_cast<const char*>(content.data()), content.size()};
      for (auto i = int64_t{0}; i < eval.length(); ++i) {
        b.data(view);
      }
      return b.finish_assert_one_array();
    });
  }
};

} // namespace

} // namespace tenzir::plugins::file_contents

TENZIR_REGISTER_PLUGIN(tenzir::plugins::file_contents::file_contents)
