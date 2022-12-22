//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/send.hpp>
#include <fmt/format.h>
#include <spdlog/sinks/base_sink.h>

#include <mutex>

namespace vast::plugins::tui {

// A spdlog sink that sends log messages to an actor.
template <class Mutex>
class actor_sink : public spdlog::sinks::base_sink<Mutex> {
public:
  explicit actor_sink(const caf::actor& receiver)
    : receiver_{caf::actor_cast<caf::weak_actor_ptr>(receiver)} {
  }

protected:
  void sink_it_(const spdlog::details::log_msg& msg) override {
    spdlog::memory_buf_t formatted;
    spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
    auto line = fmt::to_string(formatted);
    buffer_.push_back(std::move(line));
    flush_();
  }

  void flush_() override {
    if (buffer_.empty())
      return;
    auto actor = caf::actor_cast<caf::actor>(receiver_.lock());
    if (!actor)
      return;
    for (auto& line : buffer_)
      caf::anon_send(actor, std::move(line));
    buffer_ = {};
  }

private:
  caf::weak_actor_ptr receiver_;
  std::vector<std::string> buffer_;
};

using actor_sink_mt = actor_sink<std::mutex>;

} // namespace vast::plugins::tui
