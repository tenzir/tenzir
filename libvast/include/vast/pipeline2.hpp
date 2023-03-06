//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/logical_operator.hpp"

#include <caf/error.hpp>

namespace vast {
class pipeline2 final : public runtime_logical_operator {
public:
  static auto make(std::vector<logical_operator_ptr> ops)
    -> caf::expected<pipeline2> {
    if (ops.empty())
      return caf::make_error(ec::logic_error, "encountered empty pipeline");
    auto mismatch
      = std::adjacent_find(ops.begin(), ops.end(), [](auto& a, auto& b) {
          return a->output_element_type() != b->input_element_type();
        });
    if (mismatch != ops.end()) {
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("type mismatch: {} != {}",
                    (*mismatch)->output_element_type().name,
                    (*(mismatch + 1))->input_element_type().name));
    }
    auto flattened = std::vector<logical_operator_ptr>{};
    flattened.reserve(ops.size());
    for (auto& op : ops) {
      if (auto p = dynamic_cast<pipeline2*>(op.get())) {
        flattened.insert(flattened.end(),
                         std::make_move_iterator(p->ops_.begin()),
                         std::make_move_iterator(p->ops_.end()));
      } else {
        flattened.push_back(std::move(op));
      }
    }
    return pipeline2{std::move(flattened)};
  }

  [[nodiscard]] auto input_element_type() const noexcept
    -> runtime_element_type final {
    return ops_.front()->input_element_type();
  }

  [[nodiscard]] auto output_element_type() const noexcept
    -> runtime_element_type final {
    return ops_.back()->output_element_type();
  }

  [[nodiscard]] auto
  runtime_instantiate([[maybe_unused]] const type& input_schema) noexcept
    -> caf::expected<runtime_physical_operator> final {
    return caf::make_error(ec::logic_error, "instantiated pipeline");
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string final {
    return fmt::to_string(fmt::join(ops_, " | "));
  }

  auto definition() const -> std::span<const logical_operator_ptr> {
    return ops_;
  }

private:
  explicit pipeline2(std::vector<logical_operator_ptr> ops)
    : ops_(std::move(ops)) {
  }

  std::vector<logical_operator_ptr> ops_;
};
} // namespace vast

template <>
struct fmt::formatter<vast::pipeline2> : fmt::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const vast::pipeline2& value, FormatContext& ctx) const {
    return fmt::formatter<std::string_view>::format(value.to_string(), ctx);
  }
};
