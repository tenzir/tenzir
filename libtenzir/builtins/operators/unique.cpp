//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

namespace tenzir::plugins::unique {

namespace {

class unique_operator final : public crtp_operator<unique_operator> {
public:
  unique_operator() = default;

  // Note: The following implementation does a point-wise comparison of
  // consecutive rows. To this end, we use `table_slice::at`. This could be
  // optimized in the future.
  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    // We keep track of the last non-empty slice to compare the first event of
    // the next slice against its last event.
    auto previous = table_slice{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      };
      // The first row could be equal to the last row of the previous batch.
      auto begin = size_t{0};
      if (previous.rows() > 0
          && slice.schema().prune() == previous.schema().prune()) {
        if (is_duplicate(slice, 0, previous, previous.rows() - 1)) {
          begin += 1;
        }
      }
      // We want to yield a subslice when we encounter a duplicate, and when the
      // table slice ends. The loop below unifies both scenarios by including a
      // row at `row == slice.rows()` that is always considered to be a duplicate.
      for (auto row = size_t{1}; row < slice.rows() + 1; ++row) {
        if (row == slice.rows() || is_duplicate(slice, row - 1, slice, row)) {
          co_yield subslice(slice, begin, row);
          begin = row + 1;
        }
      }
      TENZIR_ASSERT(begin == slice.rows() + 1);
      previous = std::move(slice);
    }
  }

  auto to_string() const -> std::string override {
    return "unique";
  }

  auto name() const -> std::string override {
    return "unique";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    // TODO: We compare the *pruned* schemas above. Hence, returning
    // `event_order::schema` here might be slightly incorrect.
    (void)order;
    return optimize_result{filter, event_order::schema, copy()};
  }

  friend auto inspect(auto&, unique_operator&) -> bool {
    return true;
  }

private:
  /// @pre `a.schema() == b.schema()`
  static auto is_duplicate(const table_slice& a, size_t a_row,
                           const table_slice& b, size_t b_row) -> bool {
    TENZIR_ASSERT_EXPENSIVE(a.schema().prune() == b.schema().prune());
    for (auto col = size_t{0}; col < a.columns(); ++col) {
      if (a.at(a_row, col) != b.at(b_row, col)) {
        return false;
      }
    }
    return true;
  }
};

class plugin final : public virtual operator_plugin<unique_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"unique", "https://docs.tenzir.com/next/"
                                            "operators/transformations/unique"};
    parser.parse(p);
    return std::make_unique<unique_operator>();
  }
};

} // namespace

} // namespace tenzir::plugins::unique

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unique::plugin)
