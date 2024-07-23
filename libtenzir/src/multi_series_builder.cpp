//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/record_builder.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/type.hpp"
#include "tenzir/diagnostics.hpp"

#include <tenzir/multi_series_builder.hpp>

#include <caf/none.hpp>
#include <caf/sum_type.hpp>
#include <fmt/core.h>

#include <http_parser.h>
#include <optional>
#include <string_view>
#include <utility>
#include <variant>

namespace tenzir {
namespace {
using signature_type = detail::multi_series_builder::signature_type;
void append_name_to_signature(std::string_view x, signature_type& out) {
  auto name_bytes = as_bytes(x);
  out.insert(out.end(), name_bytes.begin(), name_bytes.end());
}
} // namespace

namespace detail::multi_series_builder {

auto record_generator::field(std::string_view name) -> field_generator {
  const auto visitor = detail::overload{
    [&](series_builder_element& b) {
      return field_generator{b.ref.field(name), b.parser};
    },
    [&](raw_pointer raw) {
      return field_generator{raw->field(name)};
    },
  };
  return std::visit(visitor, var_);
}

auto record_generator::unflattend_field(
  std::string_view key, std::string_view unflatten) -> field_generator {
  if (unflatten.empty()) {
    return field(key);
  }
  auto i = key.find(unflatten);
  if (i == key.npos) {
    return field(key);
  }
  auto pre = key.substr(0, i);
  auto post = key.substr(i + unflatten.size());

  return field(pre).record().unflattend_field(post, unflatten);
}

auto field_generator::record() -> record_generator {
  const auto visitor = detail::overload{
    [&](series_builder_element& b) {
      return record_generator{b.ref.record(), b.parser};
    },
    [&](raw_pointer raw) {
      return record_generator{raw->record()};
    },
  };
  return std::visit(visitor, var_);
}

auto field_generator::list() -> list_generator {
  const auto visitor = detail::overload{
    [&](series_builder_element& b) {
      return list_generator{b.ref.list(), b.parser};
    },
    [&](raw_pointer raw) {
      return list_generator{raw->list()};
    },
  };
  return std::visit(visitor, var_);
}

void field_generator::null() {
  return this->data(caf::none);
}

void list_generator::null() {
  return this->data(caf::none);
}

auto list_generator::record() -> record_generator {
  const auto visitor = detail::overload{
    [&](series_builder_element& b) {
      return record_generator{b.ref.record(), b.parser};
    },
    [&](raw_pointer raw) {
      return record_generator{raw->record()};
    },
  };
  return std::visit(visitor, var_);
}

auto list_generator::list() -> list_generator {
  const auto visitor = detail::overload{
    [&](series_builder_element& b) {
      return list_generator{b.ref.list(), b.parser};
    },
    [&](raw_pointer raw) {
      return list_generator{raw->list()};
    },
  };
  return std::visit(visitor, var_);
}
} // namespace detail::multi_series_builder

auto series_to_table_slice(series array,
                           std::string_view fallback_name) -> table_slice {
  TENZIR_ASSERT(caf::holds_alternative<record_type>(array.type));
  TENZIR_ASSERT(array.length() > 0);
  if (array.type.name().empty()) {
    array.type = tenzir::type{fallback_name, array.type};
  }
  auto* cast = dynamic_cast<arrow::StructArray*>(array.array.get());
  TENZIR_ASSERT(cast);
  auto arrow_schema = array.type.to_arrow_schema();
  auto batch = arrow::RecordBatch::Make(std::move(arrow_schema), cast->length(),
                                        cast->fields());
  TENZIR_ASSERT(batch);
  TENZIR_ASSERT_EXPENSIVE(batch->Validate().ok());
  return table_slice{std::move(batch), std::move(array.type)};
}
auto series_to_table_slice(std::vector<series> data,
                           std::string_view fallback_name)
  -> std::vector<table_slice> {
  auto result = std::vector<table_slice>{};
  result.resize(data.size());
  std::ranges::transform(
    std::move(data), result.begin(), [fallback_name](auto& s) {
      return series_to_table_slice(std::move(s), fallback_name);
    });
  return result;
}

auto multi_series_builder::yield_ready() -> std::vector<series> {
  const auto now = std::chrono::steady_clock::now();
  if (now - last_yield_time_ < settings_.timeout) {
    return {};
  }
  last_yield_time_ = now;
  if (get_policy<policy_merge>()) {
    return merging_builder_.finish();
  }
  make_events_available_where(
    [now, timeout = settings_.timeout,
     target_size = settings_.desired_batch_size](const entry_data& e) {
      return e.builder.length()
               >= static_cast<int64_t>(target_size) // batch size hit
             or now - e.flushed >= timeout;         // timeout hit
    });
  garbage_collect_where(
    [now, timeout = settings_.timeout](const entry_data& e) {
      return now - e.flushed >= 10 * timeout;
    });
  return std::exchange(ready_events_, {});
}

auto multi_series_builder::yield_ready_as_table_slice()
  -> std::vector<table_slice> {
  return series_to_table_slice(yield_ready(), settings_.default_name);
}

auto multi_series_builder::last_errors() -> std::vector<tenzir::diagnostic> {
  return std::exchange(errors_, {});
}

auto multi_series_builder::record() -> record_generator {
  if (get_policy<policy_merge>()) {
    return record_generator{merging_builder_.record(), &parser_};
  } else {
    complete_last_event();
    return record_generator{builder_raw_.record()};
  }
}

void multi_series_builder::remove_last() {
  if (get_policy<policy_merge>()) {
    merging_builder_.remove_last();
    return;
  }
  if (not builder_raw_.has_elements()) {
    builder_raw_.clear();
    return;
  }
  if (active_index_ < entries_.size()) {
    entries_[active_index_].builder.remove_last();
  }
}

auto multi_series_builder::finalize() -> std::vector<series> {
  if (get_policy<policy_merge>()) {
    return merging_builder_.finish();
  }
  make_events_available_where([](const auto&) {
    return true;
  });
  return std::exchange(ready_events_, {});
}

auto multi_series_builder::finalize_as_table_slice()
  -> std::vector<table_slice> {
  return series_to_table_slice(finalize(), settings_.default_name);
}

void multi_series_builder::complete_last_event() {
  if (get_policy<policy_merge>()) {
    return; // merging mode just writes directly into a series builder
  }
  if (not builder_raw_.has_elements()) {
    return; // an empty raw field does not need to be written back
  }
  std::string schema_name;
  if (auto p = get_policy<policy_selector>()) {
    auto* selected_schema = builder_raw_.find_field_raw(p->field_name);
    if (selected_schema) {
      const auto visitor = detail::overload{
        [p]<detail::record_builder::non_structured_data_type T>(
          const T& v) -> std::string {
          if (p->naming_prefix) {
            return fmt::format("{}.{}", *(p->naming_prefix), v);
          } else {
            return fmt::format("{}", v);
          }
        },
        [](const caf::none_t&) -> std::string {
          return "null"; // TODO this is a magic constant.
        },
        [&err_vec = this->errors_](const blob&) -> std::string {
          err_vec.emplace_back(diagnostic::warning("parser: a field of type `blob` cannot be used as a selector").done());
          return {};
        },
        [](const auto&) -> std::string {
          return {};
        },
      };
      schema_name = std::visit(visitor, selected_schema->data_);
      //FIXME raise warning for schema issues
    } else {
      // TODO should this raise some warning?
    }
  } else if (auto p = get_policy<policy_precise>()) {
    if (p->seed_schema) {
      schema_name = *(p->seed_schema);
    }
  }
  if (schema_name.empty()) {
    schema_name = settings_.default_name;
  }
  signature_raw_.clear();
  append_name_to_signature(schema_name, signature_raw_);
  const auto schema_type = type_for_schema(schema_name);
  auto e = builder_raw_.append_signature_to(signature_raw_, parser_,
                                            schema_type, settings_.schema_only);
  if (e) {
    errors_.push_back(std::move(*e));
    // TODO re-consider what to do with an errored event
  }
  auto free_index = next_free_index();
  auto [it, inserted] = signature_map_.try_emplace(
    std::move(signature_raw_), free_index.value_or(entries_.size()));
  if (inserted) { // the signature wasn't in the map yet
    if (not free_index) {
      entries_.emplace_back(std::move(schema_name), schema_type);
    } else {
      entries_[it->second].unused = false;
    }
  }
  const auto new_index = it->second;
  if (settings_.ordered and new_index != active_index_) {
    // Because it's the ordered mode, we know that that only this single
    // series builder can be active and hold elements. Since the active
    // builder changed, we flush the previous one.
    append_ready_events(entries_[active_index_].flush());
  }
  active_index_ = new_index;
  auto& entry = entries_[new_index];
  builder_raw_.commit_to(entry.builder, true);
}

void multi_series_builder::clear_raw_event() {
  builder_raw_.clear();
  signature_raw_.clear();
}

std::optional<size_t> multi_series_builder::next_free_index() const {
  for (size_t i = 0; i < entries_.size(); ++i) {
    if (entries_[i].unused) {
      return i;
    }
  }
  return std::nullopt;
}

auto multi_series_builder::type_for_schema(std::string_view name)
  -> const tenzir::type* {
  const auto it = schemas_.find(name);
  if (it == std::ranges::end(schemas_)) {
    return nullptr;
  } else {
    return std::addressof(it->second);
  }
}

void multi_series_builder::make_events_available_where(
  std::predicate<const entry_data&> auto pred) {
  complete_last_event();
  for (auto& entry : entries_) {
    if (pred(entry)) {
      append_ready_events(entry.flush());
    }
  }
}

void multi_series_builder::append_ready_events(
  std::vector<series>&& new_events) {
  ready_events_.reserve(ready_events_.size() + new_events.size());
  ready_events_.insert(ready_events_.end(),
                       std::make_move_iterator(new_events.begin()),
                       std::make_move_iterator(new_events.end()));
  new_events.clear();
}

void multi_series_builder::garbage_collect_where(
  std::predicate<const entry_data&> auto pred) {
  if (get_policy<policy_merge>()) {
    return;
  }
  for (auto it = signature_map_.begin(); it != signature_map_.end(); ++it) {
    auto& entry = entries_[it.value()];
    if (pred(entry)) {
      TENZIR_ASSERT(entry.builder.length() == 0,
                    "The predicate for garbage collection should be strictly "
                    "wider than the predicate for yielding in call cases. GC "
                    "should never remove collect builders that still have "
                    "events in them.");
      entry.unused = true;
      it = signature_map_.erase(it);
    }
  }
}
} // namespace tenzir
