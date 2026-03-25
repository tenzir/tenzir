//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

#include "operator.hpp"

namespace tenzir::plugins::s3 {

namespace {

class registrar final : public plugin {
public:
  ~registrar() noexcept override {
    const auto finalized = arrow::fs::FinalizeS3();
    TENZIR_ASSERT(finalized.ok(), finalized.ToString().c_str());
  }

  auto initialize(const record&, const record&) -> caf::error override {
    auto initialized = arrow::fs::EnsureS3Initialized();
    if (not initialized.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to initialize Arrow S3 "
                                         "functionality: {}",
                                         initialized.ToString()));
    }
    return {};
  }

  auto name() const -> std::string override {
    return "s3";
  }
};

} // namespace
} // namespace tenzir::plugins::s3
TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::registrar)
