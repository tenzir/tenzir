//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/fwd.hpp>
#include <vast/http_api.hpp>
#include <vast/plugin.hpp>
#include <vast/system/actors.hpp>

#include <memory>

namespace vast::plugins::rest {

class restinio_response;

}

// FIXME
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(vast::plugins::rest::restinio_response);
