#pragma once
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/argument_parser2.hpp>
#include <tenzir/location.hpp>

namespace tenzir::plugins::google_cloud_pubsub {

struct args {
  located<std::string> project_id_;
  located<std::string> topic_id_;

  auto add_to(argument_parser& parser) -> void {
    parser.add(project_id_, "<project-id>");
    parser.add(topic_id_, "<topic-id>");
  }

  auto add_to(argument_parser2& parser) -> void {
    parser.add("project_id", project_id_);
    parser.add("topic_id", topic_id_);
  }

  friend auto inspect(auto& f, args& x) -> bool {
    return f.object(x).fields(f.field("project_id", x.project_id_),
                              f.field("topic_id", x.topic_id_));
  }
};

} // namespace tenzir::plugins::google_cloud_pubsub
