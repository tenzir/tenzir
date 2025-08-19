//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/tql2/plugin.hpp>

namespace tenzir {

auto invocation_for_plugin(const plugin& plugin, location location
                                                 = location::unknown)
  -> ast::invocation;

struct compression_and_format {
  compression_and_format(
    const operator_factory_plugin* compression,
    std::reference_wrapper<const operator_factory_plugin> format)
    : compression{compression}, format{format} {
  }

  /// Compression is optional.
  const operator_factory_plugin* compression;
  /// Format is required.
  std::reference_wrapper<const operator_factory_plugin> format;
};

template <bool is_loading>
auto get_compression_and_format(located<std::string_view> url,
                                const operator_factory_plugin* default_format,
                                std::string_view docs, diagnostic_handler& dh)
  -> failure_or<compression_and_format>;

template <bool is_loading>
auto create_pipeline_from_uri(std::string path,
                              operator_factory_plugin::invocation inv,
                              session ctx, const char* docs)
  -> failure_or<operator_ptr>;
} // namespace tenzir
