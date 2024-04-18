//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/configuration.hpp>
#include <tenzir/detail/load_contents.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql/parser.hpp>

namespace tenzir::plugins::apply {

namespace {

class plugin final : public virtual operator_parser_plugin {
public:
  auto name() const -> std::string override {
    return "apply";
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)plugin_config;
    // Note: `remote apply` will not work.
    paths_ = config_dirs(global_config);
    for (auto& path : paths_) {
      path /= "apply";
    }
    return {};
  }

  auto signature() const -> operator_signature override {
    return {.source = true, .transformation = true, .sink = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto file = located<std::string>{};
    auto parser = argument_parser{name(), fmt::format("https://docs.tenzir.com/"
                                                      "operators/{}",
                                                      name())};
    parser.add(file, "<file>");
    parser.parse(p);
    auto completed_path = std::invoke([&] {
      auto path = std::filesystem::path{file.inner};
      if (not path.has_extension()) {
        path += ".tql";
      }
      auto err = std::error_code{};
      if (std::filesystem::exists(path, err)) {
        return std::filesystem::absolute(path);
      }
      if (not path.is_absolute()) {
        for (auto& prefix : paths_) {
          auto combined = prefix / path;
          if (std::filesystem::exists(combined, err)) {
            return combined;
          }
        }
      }
      // The call to `std::filesystem::path::string` is necessary to prevent
      // quotes in older versions of `fmt`.
      diagnostic::error("could not find `{}`", path.string())
        .primary(file.source)
        .throw_();
    });
    // This is TOCTOU, but `detail::load_contents` does not really lead to a
    // nice error message.
    auto content = detail::load_contents(completed_path);
    if (not content) {
      diagnostic::error("failed to read from file `{}`: {}",
                        completed_path.string(), content.error())
        .primary(file.source)
        .throw_();
    }
    auto [pipe, diags] = tql::parse_internal_with_diags(*content);
    if (not pipe) {
      // This is somewhat hacky. Should be fixed with revamp.
      if (diags.size() == 1 && diags[0].severity == severity::error) {
        std::move(diags[0])
          .modify()
          .primary(file.source)
          .note("while parsing `{}`", completed_path.string())
          .throw_();
      } else {
        diagnostic::error("failed to parse `{}`: {::?}", file.inner, diags)
          .primary(file.source)
          .throw_();
      }
    }
    return std::make_unique<pipeline>(std::move(*pipe));
  }

private:
  std::vector<std::filesystem::path> paths_;
};

} // namespace

} // namespace tenzir::plugins::apply

TENZIR_REGISTER_PLUGIN(tenzir::plugins::apply::plugin)
