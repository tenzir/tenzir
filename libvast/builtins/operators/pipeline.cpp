//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>

#include <arrow/type.h>

namespace vast::plugins::pipeline {

namespace {

class plugin final : public virtual operator_inspection_plugin<vast::pipeline> {
};

} // namespace

} // namespace vast::plugins::pipeline

VAST_REGISTER_PLUGIN(vast::plugins::pipeline::plugin)
