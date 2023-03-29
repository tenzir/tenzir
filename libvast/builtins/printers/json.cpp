//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/plugin.hpp>

#include <arrow/record_batch.h>

namespace vast::plugins::json_printer {

class plugin : public virtual printer_plugin {
public:
  [[nodiscard]] auto
  make_printer(const record&, type input_schema, operator_control_plane&) const
    -> caf::expected<printer> override {
    auto input_type = caf::get<record_type>(input_schema);
    return [input_type](table_slice slice) -> generator<chunk_ptr> {
      // JSON printer should output NDJSON, see:
      // https://github.com/ndjson/ndjson-spec
      auto printer = vast::json_printer{{.oneline = true}};
      // TODO: Since this printer is per-schema we can write an optimized
      // version of it that gets the schema ahead of time and only expects data
      // corresponding to exactly that schema.
      auto buffer = std::vector<char>{};
      auto resolved_slice = resolve_enumerations(slice);
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      auto out_iter = std::back_inserter(buffer);
      for (const auto& row : values(input_type, *array)) {
        VAST_ASSERT_CHEAP(row);
        const auto ok = printer.print(out_iter, *row);
        VAST_ASSERT_CHEAP(ok);
        out_iter = fmt::format_to(out_iter, "\n");
        auto chunk = chunk::make(std::exchange(buffer, {}));
        co_yield std::move(chunk);
      }
      co_return;
    };
  }

  [[nodiscard]] auto make_default_dumper() const
    -> const dumper_plugin* override {
    return vast::plugins::find<vast::dumper_plugin>("stdout");
  }

  [[nodiscard]] auto printer_allows_joining() const -> bool override {
    return true;
  };

  [[nodiscard]] auto name() const -> std::string override {
    return "json";
  }

  [[nodiscard]] auto initialize([[maybe_unused]] const record& plugin_config,
                                [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return caf::none;
  }
};

} // namespace vast::plugins::json_printer

VAST_REGISTER_PLUGIN(vast::plugins::json_printer::plugin)
