//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/data_builder.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/type.hpp"

#include <tenzir/multi_series_builder.hpp>

#include <caf/none.hpp>
#include <caf/sum_type.hpp>
#include <fmt/core.h>

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

auto record_generator::exact_field(std::string_view name) -> object_generator {
  if (not msb_) {
    return object_generator{};
  }
  const auto visitor = detail::overload{
    [&](tenzir::record_ref b) {
      auto f = b.field(name);
      if (msb_->settings_.schema_only and not f.is_protected()) {
        return object_generator{};
      }
      return object_generator{msb_, std::move(f)};
    },
    [&](raw_pointer raw) {
      return object_generator{msb_, raw->field(name)};
    },
  };
  return std::visit(visitor, var_);
}

auto record_generator::field(std::string_view name) -> object_generator {
  if (not msb_) {
    return object_generator{};
  }
  return exact_field(name);
}

auto record_generator::unflattened_field(
  std::string_view key, std::string_view unflatten) -> object_generator {
  if (not msb_) {
    return object_generator{};
  }
  if (unflatten.empty()) {
    return exact_field(key);
  }
  auto i = key.find(unflatten);
  if (i == key.npos) {
    return exact_field(key);
  }
  const auto pre = key.substr(0, i);
  const auto post = key.substr(i + unflatten.size());

  return exact_field(pre).record().unflattened_field(post, unflatten);
}

auto record_generator::unflattened_field(std::string_view key)
  -> object_generator {
  if (not msb_) {
    return object_generator{};
  }
  return unflattened_field(key, msb_->settings_.unnest_separator);
}

auto record_generator::writable() -> bool {
  return msb_;
}

auto object_generator::data(const tenzir::data& d) -> void {
  if (not msb_) {
    return;
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return;
      }
      b.data(d);
    },
    [&](raw_pointer raw) {
      raw->data(d);
    },
  };
  return std::visit(visitor, var_);
}

auto object_generator::data_unparsed(std::string_view s) -> void {
  if (not msb_) {
    return;
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return;
      }
      auto res = msb_->builder_raw_.parser_(s, nullptr);
      auto& [value, diag] = res;
      if (value) {
        b.data(std::move(*value));
      } else {
        b.data(s);
      }
    },
    [&](raw_pointer raw) {
      raw->data_unparsed(std::string{s});
    },
  };
  return std::visit(visitor, var_);
}

auto object_generator::data_unparsed(std::string s) -> void {
  if (not msb_) {
    return;
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return;
      }
      auto res = msb_->builder_raw_.parser_(s, nullptr);
      auto& [value, diag] = res;
      if (value) {
        b.data(std::move(*value));
      } else {
        b.data(s);
      }
    },
    [&](raw_pointer raw) {
      raw->data_unparsed(std::move(s));
    },
  };
  return std::visit(visitor, var_);
}

auto object_generator::record() -> record_generator {
  if (not msb_) {
    return record_generator{};
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return record_generator{};
      }
      return record_generator{msb_, b.record()};
    },
    [&](raw_pointer raw) {
      return record_generator{msb_, raw->record()};
    },
  };
  return std::visit(visitor, var_);
}

auto object_generator::list() -> list_generator {
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return list_generator{};
      }
      return list_generator{msb_, b.list()};
    },
    [&](raw_pointer raw) {
      return list_generator{msb_, raw->list()};
    },
  };
  return std::visit(visitor, var_);
}

auto object_generator::null() -> void {
  if (not msb_) {
    return;
  }
  return this->data(caf::none);
}

auto object_generator::writable() -> bool {
  if (not msb_) {
    return false;
  }
  if (msb_->settings_.schema_only) {
    if (auto* b = std::get_if<builder_ref>(&var_)) {
      return b->is_protected();
    }
    return true;
  }
  return true;
}

void list_generator::null() {
  if (not msb_) {
    return;
  }
  return this->data(caf::none);
}

auto list_generator::data(const tenzir::data& d) -> void {
  if (not msb_) {
    return;
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return;
      }
      b.data(d);
    },
    [&](raw_pointer raw) {
      raw->data(d);
    },
  };
  return std::visit(visitor, var_);
}

auto list_generator::data_unparsed(std::string_view s) -> void {
  if (not msb_) {
    return;
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return;
      }
      auto res = msb_->builder_raw_.parser_(s, nullptr);
      auto& [value, diag] = res;
      if (value) {
        b.data(std::move(*value));
      } else {
        b.data(s);
      }
    },
    [&](raw_pointer raw) {
      raw->data_unparsed(std::string{s});
    },
  };
  return std::visit(visitor, var_);
}

auto list_generator::data_unparsed(std::string s) -> void {
  if (not msb_) {
    return;
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return;
      }
      auto res = msb_->builder_raw_.parser_(s, nullptr);
      auto& [value, diag] = res;
      if (value) {
        b.data(std::move(*value));
      } else {
        b.data(s);
      }
    },
    [&](raw_pointer raw) {
      raw->data_unparsed(std::move(s));
    },
  };
  return std::visit(visitor, var_);
}

auto list_generator::record() -> record_generator {
  if (not msb_) {
    return record_generator{};
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return record_generator{};
      }
      return record_generator{msb_, b.record()};
    },
    [&](raw_pointer raw) {
      return record_generator{msb_, raw->record()};
    },
  };
  return std::visit(visitor, var_);
}

auto list_generator::list() -> list_generator {
  if (not msb_) {
    return list_generator{};
  }
  const auto visitor = detail::overload{
    [&](tenzir::builder_ref b) {
      if (not writable()) {
        return list_generator{};
      }
      return list_generator{msb_, b.list()};
    },
    [&](raw_pointer raw) {
      return list_generator{msb_, raw->list()};
    },
  };
  return std::visit(visitor, var_);
}

auto list_generator::writable() -> bool {
  return msb_;
}

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
} // namespace detail::multi_series_builder

multi_series_builder::multi_series_builder(
  policy_type policy, settings_type settings, diagnostic_handler& dh,
  std::vector<type> schemas, data_builder::data_parsing_function parser)
  : policy_{std::move(policy)},
    settings_{std::move(settings)},
    dh_{dh},
    builder_raw_{std::move(parser), &dh, settings_.schema_only, settings_.raw} {
  schemas_.reserve(schemas.size());
  for (auto t : schemas) {
    const auto [it, success] = schemas_.try_emplace(t.name(), std::move(t));
    TENZIR_ASSERT(success, "Repeated schema name");
  }
  if (get_policy<policy_default>()) {
    // if we merge all events, they are necessarily ordered
    settings_.ordered |= settings_.merge;
  } else if (auto p = get_policy<policy_schema>()) {
    auto seed = type_for_schema(p->seed_schema);
    TENZIR_ASSERT(settings_.schema_only ? seed != nullptr : true);
    // If we are in schema_only mode, that means we can also be merging into a
    // single builder.
    if (seed and settings_.schema_only) {
      settings_.merge = true;
    }
    if (settings_.merge) {
      // if we merge all events, they are necessarily ordered
      settings_.ordered = true;
      if (seed) {
        merging_builder_ = seed;
      } else {
        merging_builder_ = tenzir::type{p->seed_schema, null_type{}};
      }
    } else {
      if (seed) {
        naming_sentinel_ = *seed;
        needs_signature_ = not settings_.schema_only;
        builder_schema_ = &naming_sentinel_;
        parsing_signature_schema_ = &naming_sentinel_;
      } else {
        naming_sentinel_ = tenzir::type{p->seed_schema, null_type{}};
        builder_schema_ = &naming_sentinel_;
        parsing_signature_schema_ = nullptr;
      }
    }
  }
  // The selector mode has not special ctor setup, as it all depends on runtime
  // inputs
}

multi_series_builder::multi_series_builder(multi_series_builder&& other) noexcept
  : policy_{std::move(other.policy_)},
    settings_{std::move(other.settings_)},
    dh_{other.dh_},
    schemas_{std::move(other.schemas_)},
    merging_builder_{std::move(other.merging_builder_)},
    builder_raw_{std::move(other.builder_raw_)},
    needs_signature_{other.needs_signature_},
    naming_sentinel_{std::move(other.naming_sentinel_)},
    builder_schema_{other.builder_schema_ == &other.naming_sentinel_
                      ? &naming_sentinel_
                      : nullptr},
    parsing_signature_schema_{other.parsing_signature_schema_
                                  == &other.naming_sentinel_
                                ? &naming_sentinel_
                                : nullptr},
    signature_raw_{std::move(other.signature_raw_)},
    signature_map_{std::move(other.signature_map_)},
    entries_{std::move(other.entries_)},
    ready_events_{std::move(other.ready_events_)},
    last_yield_time_{std::move(other.last_yield_time_)},
    active_index_{other.active_index_} {
}

auto multi_series_builder::yield_ready() -> std::vector<series> {
  const auto now = std::chrono::steady_clock::now();
  if (now - last_yield_time_ < settings_.timeout) {
    return {};
  }
  last_yield_time_ = now;
  if (settings_.merge and not get_policy<policy_selector>()) {
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
  return detail::multi_series_builder::series_to_table_slice(
    yield_ready(), settings_.default_schema_name);
}

auto multi_series_builder::record() -> record_generator {
  if (uses_merging_builder()) {
    return record_generator{this, merging_builder_.record()};
  } else {
    complete_last_event();
    return record_generator{this, builder_raw_.record()};
  }
}

auto multi_series_builder::list() -> list_generator {
  if (uses_merging_builder()) {
    return list_generator{this, merging_builder_.list()};
  } else {
    complete_last_event();
    return list_generator{this, builder_raw_.list()};
  }
}

void multi_series_builder::remove_last() {
  if (uses_merging_builder()) {
    merging_builder_.remove_last();
    return;
  }
  if (builder_raw_.has_elements()) {
    builder_raw_.clear();
    return;
  }
  if (active_index_ < entries_.size()) {
    entries_[active_index_].builder.remove_last();
  }
}

auto multi_series_builder::finalize() -> std::vector<series> {
  if (uses_merging_builder()) {
    return merging_builder_.finish();
  }
  make_events_available_where([](const auto&) {
    return true;
  });
  return std::exchange(ready_events_, {});
}

auto multi_series_builder::finalize_as_table_slice()
  -> std::vector<table_slice> {
  return detail::multi_series_builder::series_to_table_slice(
    finalize(), settings_.default_schema_name);
}

void multi_series_builder::complete_last_event() {
  if (uses_merging_builder()) {
    return; // merging mode just writes directly into a series builder
  }
  if (not builder_raw_.has_elements()) {
    return; // an empty raw field does not need to be written back
  }
  signature_raw_.clear();
  if (auto p = get_policy<policy_selector>()) {
    auto* selected_schema = builder_raw_.find_field_raw(p->field_name);
    if (not selected_schema) {
      diagnostic::warning("event did not contain selector field")
        .note("selector field `{}` was not found", p->field_name)
        .emit(dh_);
      needs_signature_ = true;
      builder_schema_ = nullptr;
      parsing_signature_schema_ = nullptr;
    } else {
      bool selector_was_string = false;
      const auto visitor = detail::overload{
        [&](const std::string& v) {
          selector_was_string = true;
          if (p->naming_prefix) {
            return fmt::format("{}.{}", *(p->naming_prefix), v);
          } else {
            return v;
          }
        },
        [p]<detail::data_builder::non_structured_data_type T>(
          const T& v) -> std::string {
          if (p->naming_prefix) {
            return fmt::format("{}.{}", *(p->naming_prefix), v);
          } else {
            return fmt::format("{}", v);
          }
        },
        [](const caf::none_t&) -> std::string {
          return {};
        },
        [this](const blob&) -> std::string {
          diagnostic::warning("selector field contains `blob` data, "
                              "which cannot be used as a selector")
            .emit(dh_);
          return {};
        },
        [this](const auto&) -> std::string {
          diagnostic::warning("selector field contains structural "
                              "type, which cannot be used as a selector")
            .emit(dh_);
          return {};
        },
      };
      const auto schema_name = std::visit(visitor, selected_schema->data_);
      builder_schema_ = type_for_schema(schema_name);
      parsing_signature_schema_ = builder_schema_;
      // we may need to compute the signature in selector mode
      needs_signature_ = true;
      // if the user promised that the selector is unique, we can rely on the
      // selectors name
      if (settings_.merge) {
        needs_signature_ = schema_name.empty();
      }
      // if we only want to output a schema, cam also just rely on its name
      if (builder_schema_ and settings_.schema_only) {
        needs_signature_ = false;
      }
      if (not builder_schema_) { // if the selector didnt refer to a known schema
        // TODO re-consider this warning
        //  * it can get quite noisy
        //  * Do we even want to disable it for unique_selector?
        if (selector_was_string and not settings_.merge) {
          diagnostic::warning("selected schema not found")
            .note("`{}` does not refer to a known schema", schema_name)
            .emit(dh_);
        }
        naming_sentinel_ = tenzir::type{schema_name, null_type{}};
        builder_schema_ = &naming_sentinel_;
      }
      append_name_to_signature(schema_name, signature_raw_);
    }
  } else if (auto p = get_policy<policy_schema>()) {
    if (not p->seed_schema.empty()) {
      // technically there is no need to repeat these steps every iteration
      // But we would need special handling for writing the schema name into the
      // signature every event
      append_name_to_signature(p->seed_schema, signature_raw_);
    }
  }
  if (needs_signature_) {
    builder_raw_.append_signature_to(signature_raw_, parsing_signature_schema_);
  }
  auto free_index = next_free_index();
  auto [it, inserted] = signature_map_.try_emplace(
    std::move(signature_raw_), free_index.value_or(entries_.size()));
  if (inserted) { // the signature wasn't in the map yet
    if (not free_index) {
      entries_.emplace_back(builder_schema_);
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
  builder_raw_.commit_to(entry.builder, true, parsing_signature_schema_);
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
  if (uses_merging_builder()) {
    return;
  }
  for (auto it = signature_map_.begin(); it != signature_map_.end(); ++it) {
    auto& entry = entries_[it.value()];
    if (pred(entry)) {
      TENZIR_ASSERT(entry.builder.length() == 0,
                    "The predicate for garbage collection should be strictly "
                    "wider than the predicate for yielding in call cases. GC "
                    "should never trigger on builders that still have "
                    "events in them.");
      entry.unused = true;
      it = signature_map_.erase(it);
    }
  }
}
} // namespace tenzir
