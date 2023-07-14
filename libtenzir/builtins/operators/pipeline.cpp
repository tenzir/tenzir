//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::pipeline {

namespace {

class plugin final
  : public virtual operator_inspection_plugin<tenzir::pipeline> {};

} // namespace

} // namespace tenzir::plugins::pipeline

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pipeline::plugin)
