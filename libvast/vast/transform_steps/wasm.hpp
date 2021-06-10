//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transform.hpp"

namespace vast {

class wasm_step : public generic_transform_step {
public:
  wasm_step(chunk_ptr wasm_program);

  caf::expected<table_slice> operator()(table_slice&& slice) const override;

private:
  struct wasm_step_impl;
  std::unique_ptr<wasm_step_impl> pimpl_;
};

} // namespace vast
