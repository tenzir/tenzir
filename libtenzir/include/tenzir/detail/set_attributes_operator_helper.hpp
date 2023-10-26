//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/type.hpp>

namespace tenzir::detail {

class set_attributes_operator_helper {
public:
  struct configuration {
    configuration() = default;

    explicit configuration(
      std::vector<std::pair<std::string, std::string>>&& attrs)
      : attributes_(std::move(attrs)) {
    }

    auto get_attributes() const -> generator<type::attribute_view>;
    auto count_attributes() const -> size_t {
      return attributes_.size();
    }

    auto to_string() const -> std::string;

    friend auto inspect(auto& f, configuration& cfg) -> bool {
      return f.apply(cfg.attributes_);
    }

  private:
    friend class set_attributes_operator_helper;

    std::vector<std::pair<std::string, std::string>> attributes_{};
  };

  set_attributes_operator_helper() = default;

  /// Allows supplying an initial `configuration`, which can be added to with
  /// `parse`.
  set_attributes_operator_helper(configuration&& cfg) : cfg_(std::move(cfg)) {
  }

  using parse_verify = caf::error (*)(const configuration&);

  /// Parses `pipeline`, and stores the results in the `configuration` object
  /// contained in `*this`. Verifies the results with the callback `verify`.
  auto
  parse(std::string_view pipeline, parse_verify verify = default_parse_verify)
    -> std::pair<std::string_view, caf::error>;

  using process_verify = caf::error (*)(const type&, const configuration&);

  /// Wraps `slice` in a new schema called `schema_name`, with the attributes
  /// specified in the `configuration` object contained in `*this`.
  /// Verifies the received schema with `verify`.
  auto process(table_slice&& slice, process_verify verify
                                    = default_process_verify) const
    -> std::pair<table_slice, caf::error>;

  auto get_config() -> configuration& {
    return cfg_;
  }
  auto get_config() const -> const configuration& {
    return cfg_;
  }

private:
  static auto default_parse_verify(const configuration&) -> caf::error {
    return {};
  }
  static auto default_process_verify(const type&, const configuration&)
    -> caf::error {
    return {};
  }

  configuration cfg_{};
};

} // namespace tenzir::detail
