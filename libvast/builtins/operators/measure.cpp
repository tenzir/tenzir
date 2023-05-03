//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>

#include <arrow/type.h>

namespace vast::plugins::measure {

namespace {

class measure_operator final : public crtp_operator<measure_operator> {
public:
  measure_operator(uint64_t batch_size, bool real_time)
    : batch_size_{batch_size}, real_time_{real_time} {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    static const auto schema = type{
      "vast.metrics.events",
      record_type{
        {"timestamp", time_type{}},
        {"events", uint64_type{}},
        {"schema", string_type{}},
        {"schema_id", string_type{}},
      },
    };
    auto builder = table_slice_builder{schema};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        if (builder.rows() == 0) {
          co_yield {};
          continue;
        }
        co_yield builder.finish();
        continue;
      }
      const auto ok
        = builder.add(time{std::chrono::system_clock::now()}, slice.rows(),
                      slice.schema().name(), slice.schema().make_fingerprint());
      VAST_ASSERT(ok);
      if (real_time_ || builder.rows() == batch_size_) {
        co_yield builder.finish();
        continue;
      }
      co_yield {};
    }
    if (builder.rows() > 0)
      co_yield builder.finish();
  }

  auto operator()(generator<chunk_ptr> input) const -> generator<table_slice> {
    static const auto schema = type{
      "vast.metrics.bytes",
      record_type{
        {"timestamp", time_type{}},
        {"bytes", uint64_type{}},
      },
    };
    auto builder = table_slice_builder{schema};
    for (auto&& chunk : input) {
      if (!chunk || chunk->size() == 0) {
        if (builder.rows() == 0) {
          co_yield {};
          continue;
        }
        co_yield builder.finish();
        continue;
      }
      const auto ok = builder.add(std::chrono::time_point_cast<time::duration>(
                                    std::chrono::system_clock::now()),
                                  chunk->size());
      VAST_ASSERT(ok);
      if (real_time_ || builder.rows() == batch_size_) {
        co_yield builder.finish();
        continue;
      }
      co_yield {};
    }
    if (builder.rows() > 0)
      co_yield builder.finish();
  }

  auto to_string() const -> std::string override {
    return real_time_ ? "measure --real-time" : "measure";
  }

private:
  uint64_t batch_size_ = {};
  bool real_time_ = {};
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  auto initialize([[maybe_unused]] const record& plugin_config,
                  const record& global_config) -> caf::error override {
    batch_size_ = get_or(global_config, "vast.import.batch-size",
                         defaults::import::table_slice_size);
    return {};
  }

  auto name() const -> std::string override {
    return "measure";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto real_time_parser
      = (-string_parser{"--real-time"}).then([](std::string in) {
          return not in.empty();
        });
    const auto p = optional_ws_or_comment >> real_time_parser
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    bool real_time = false;
    if (!p(f, l, real_time)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "measure operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<measure_operator>(batch_size_, real_time),
    };
  }

private:
  uint64_t batch_size_ = {};
};

} // namespace

} // namespace vast::plugins::measure

VAST_REGISTER_PLUGIN(vast::plugins::measure::plugin)
