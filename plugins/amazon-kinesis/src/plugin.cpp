//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

#include <aws/core/Aws.h>

namespace tenzir::plugins::amazon_kinesis {

namespace {

class registrar final : public plugin {
public:
  registrar() {
    Aws::InitAPI(options_);
  }

  ~registrar() override {
    Aws::ShutdownAPI(options_);
  }

  auto name() const -> std::string override {
    return "amazon-kinesis";
  }

private:
  Aws::SDKOptions options_;
};

} // namespace

} // namespace tenzir::plugins::amazon_kinesis

TENZIR_REGISTER_PLUGIN(tenzir::plugins::amazon_kinesis::registrar)
