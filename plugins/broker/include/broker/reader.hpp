//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/format/multi_layout_reader.hpp>
#include <vast/fwd.hpp>
#include <vast/legacy_type.hpp>
#include <vast/schema.hpp>

#include <broker/data.hh>
#include <broker/endpoint.hh>
#include <broker/status_subscriber.hh>
#include <broker/subscriber.hh>
#include <broker/topic.hh>
#include <caf/error.hpp>

#include <iosfwd>
#include <memory>
#include <string_view>

namespace vast::plugins::broker {

/// A reader for Zeek Broker.
class reader final : public format::multi_layout_reader {
public:
  using super = multi_layout_reader;

  reader(const caf::settings& options, caf::actor_system& sys);
  reader(const reader&) = delete;
  reader& operator=(const reader&) = delete;
  reader(reader&&) noexcept = delete;
  reader& operator=(reader&&) noexcept = delete;
  ~reader() override = default;

  void reset(std::unique_ptr<std::istream> in) override;

  caf::error schema(class schema schema) override;
  class schema schema() const override;
  const char* name() const override;

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override;

private:
  caf::error
  dispatch(const ::broker::data& msg, size_t max_slice_size, consumer& f);

  // TODO: do not ignore.
  class schema schema_ = {};

  std::unique_ptr<::broker::endpoint> endpoint_;
  std::unique_ptr<::broker::status_subscriber> status_subscriber_;
  std::unique_ptr<::broker::subscriber> subscriber_;

  /// Flag that indicates whether we are processing Zeek events. This is the
  /// only "mode" we currently support, because it gives us a predictable
  /// framing of messages on top of the Broker data model.
  bool zeek_mode_ = true;

  /// Maps stream IDs from Zeek messages to (builder) layouts.
  std::unordered_map<std::string, table_slice_builder_ptr> log_layouts_;
};

} // namespace vast::plugins::broker
