//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::config {

namespace {

template <class Guard>
struct recursive_add {
  explicit recursive_add(Guard& guard) noexcept : guard{guard} {
  }

  auto operator()(const record& x) {
    auto r = guard.push_record();
    for (const auto& [key, value] : x) {
      auto field = r.push_field(key);
      auto add = recursive_add<detail::field_guard>{field};
      caf::visit(
        [&](const auto& value) {
          add(value);
        },
        value);
    }
  }

  auto operator()(const list& x) {
    auto l = guard.push_list();
    auto add = recursive_add<detail::list_guard>{l};
    for (const auto& value : x) {
      caf::visit(
        [&](const auto& value) {
          add(value);
        },
        value);
    }
  }

  auto operator()(caf::none_t) {
  }

  auto operator()(const pattern&) {
    die("unreachable");
  }

  auto operator()(const map&) {
    die("unreachable");
  }

  template <class T>
    requires(not std::is_same_v<T, data>)
  auto operator()(const T& x) {
    // FIXME
    (void)guard.add(x);
  }

  Guard& guard;
};

class plugin final : public virtual aspect_plugin {
public:
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)plugin_config;
    config_ = global_config;
    return {};
  }

  auto name() const -> std::string override {
    return "config";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto show(operator_control_plane&) const -> generator<table_slice> override {
    auto builder = adaptive_table_slice_builder{};
    {
      auto row = builder.push_row();
      for (const auto& [key, value] : config_) {
        if (key == "caf") {
          // We skip CAF's configuration as it adds a lot of noise for very
          // little value.
          continue;
        }
        auto field = row.push_field(key);
        auto add = recursive_add{field};
        caf::visit(
          [&](const auto& value) {
            add(value);
          },
          value);
      }
    }
    co_yield builder.finish("tenzir.config");
  }

private:
  record config_ = {};
};

} // namespace

} // namespace tenzir::plugins::config

TENZIR_REGISTER_PLUGIN(tenzir::plugins::config::plugin)
