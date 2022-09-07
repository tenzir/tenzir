//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/http_api.hpp>
#include <vast/plugin.hpp>

#include <restinio/compiler_features.hpp>
#include <restinio/message_builders.hpp>
#include <restinio/request_handler.hpp>

namespace vast::plugins::rest {

// TODO: Does it make sense to make use of the "extra data" to store a
// caf::intrusive_ptr ?
using request_handle_t
  = restinio::generic_request_handle_t<restinio::no_extra_data_factory_t::data_t>;
using response_t
  = restinio::response_builder_t<restinio::user_controlled_output_t>;

class restinio_response final : public vast::http_response {
public:
  restinio_response(request_handle_t&& handle,
                    const rest_endpoint_plugin::api_endpoint&);
  ~restinio_response() override;

  restinio_response(const restinio_response&) = delete;
  restinio_response& operator=(const restinio_response&) = delete;

  void append(std::string_view body) override;

  void abort(uint16_t error_code, std::string_view message) override;

private:
  request_handle_t request_;
  response_t response_; // it looks like we need to keep this alive artificially
  size_t body_size_ = {};
};

} // namespace vast::plugins::rest
