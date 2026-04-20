//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval_impl2.hpp"

#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir2/printer.hpp"
#include "tenzir2/type_system/array/builder.hpp"
#include "tenzir2/type_system/array/fundamental.hpp"
#include "tenzir2/type_system/array/list.hpp"
#include "tenzir2/type_system/array/record.hpp"
#include "tenzir2/type_system/array/string.hpp"
#include "tenzir2/type_system/array/subnet.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <ranges>

namespace tenzir2 {

namespace {

class row_value_builder {
public:
  auto append(array_row_view_<data> row) -> void {
    rows_.push_back(row);
  }

  auto size() const -> std::ptrdiff_t {
    return static_cast<std::ptrdiff_t>(rows_.size());
  }

  auto finish() -> array_<data> {
    return finish_impl(rows_);
  }

private:
  template <class Container>
  static auto finish_impl(Container const& rows) -> array_<data> {
    if (rows.empty()) {
      auto builder = array_builder_<null>{memory::default_resource()};
      return builder.finish();
    }
    auto first_kind = row_kind(rows.front());
    auto single_kind = std::ranges::all_of(rows, [&](auto row) {
      return row_kind(row) == first_kind;
    });
    if (single_kind) {
      return match(rows.front(), [&](auto row) -> array_<data> {
        using Row = std::remove_cvref_t<decltype(row)>;
        using type = typename row_type<Row>::type;
        return array_<data>{finish_typed<type>(rows)};
      });
    }
    using key_t = detail::union_array::key;
    auto groups = std::vector<std::vector<array_row_view_<data>>>{};
    auto group_kinds = std::vector<size_t>{};
    auto keys
      = memory::shared_owner<key_t[]>::builder{memory::default_resource()};
    keys.reserve(static_cast<std::ptrdiff_t>(rows.size()));
    for (auto row : rows) {
      auto kind = row_kind(row);
      auto it = std::ranges::find(group_kinds, kind);
      auto group_index
        = static_cast<size_t>(std::distance(group_kinds.begin(), it));
      if (it == group_kinds.end()) {
        group_index = groups.size();
        group_kinds.push_back(kind);
        groups.emplace_back();
      }
      auto value_index = static_cast<int64_t>(groups[group_index].size());
      groups[group_index].push_back(row);
      keys.emplace_back(group_index, value_index);
    }
    auto arrays = std::vector<detail::erased_array>{};
    arrays.reserve(groups.size());
    for (auto const& group : groups) {
      auto array = match(group.front(), [&](auto row) -> detail::erased_array {
        using Row = std::remove_cvref_t<decltype(row)>;
        using type = typename row_type<Row>::type;
        return detail::erased_array{finish_typed<type>(group)};
      });
      arrays.push_back(std::move(array));
    }
    return detail::union_array{
      memory::detail::abuffer<key_t>{keys.finish(),
                                     memory::detail::slice_info::none},
      std::move(arrays),
    };
  }

  template <class Row>
  struct row_type;

  template <data_type T>
  struct row_type<array_row_view_<T>> {
    using type = T;
  };

  static auto row_kind(array_row_view_<data> row) -> size_t {
    return match(row, []<data_type T>(array_row_view_<T>) -> size_t {
      return data_type_list::unique_index_of<T>;
    });
  }

  template <class Container>
  static auto finish_typed_null(Container const& rows) -> array_<null> {
    auto builder = array_builder_<null>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<null>>(&row);
      builder.null(view.state());
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_bool(Container const& rows) -> array_<bool> {
    auto builder = array_builder_<bool>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<bool>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_int64(Container const& rows) -> array_<int64_t> {
    auto builder = array_builder_<int64_t>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<int64_t>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_uint64(Container const& rows) -> array_<uint64_t> {
    auto builder = array_builder_<uint64_t>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<uint64_t>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_double(Container const& rows) -> array_<double> {
    auto builder = array_builder_<double>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<double>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_string(Container const& rows)
    -> array_<std::string> {
    auto builder = array_builder_<std::string>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<std::string>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_ip(Container const& rows) -> array_<ip> {
    auto builder = array_builder_<ip>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<ip>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_subnet(Container const& rows) -> array_<subnet> {
    auto builder = array_builder_<subnet>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<subnet>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_time(Container const& rows) -> array_<time> {
    auto builder = array_builder_<time>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<time>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_duration(Container const& rows) -> array_<duration> {
    auto builder = array_builder_<duration>{memory::default_resource()};
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<duration>>(&row);
      if (not view.valid()) {
        builder.null(view.state());
        continue;
      }
      builder.data(*view);
    }
    return builder.finish();
  }

  template <class Container>
  static auto finish_typed_list(Container const& rows) -> array_<list> {
    auto values = row_value_builder{};
    auto bounds = memory::shared_owner<std::ptrdiff_t[]>::builder{
      memory::default_resource()};
    auto state = memory::shared_owner<memory::element_state[]>::builder{
      memory::default_resource()};
    auto count = std::ptrdiff_t{0};
    bounds.reserve(static_cast<std::ptrdiff_t>(rows.size()));
    state.reserve(static_cast<std::ptrdiff_t>(rows.size()));
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<list>>(&row);
      if (not view.valid()) {
        state.emplace_back(view.state());
        bounds.emplace_back(count);
        continue;
      }
      state.emplace_back(memory::element_state::valid);
      for (auto value : *view) {
        values.append(value);
        ++count;
      }
      bounds.emplace_back(count);
    }
    return array_<list>{
      memory::list_array<array_<data>>{
        values.finish(),
        memory::detail::position_buffer{
          bounds.finish(),
          memory::detail::slice_info::none,
        },
        memory::detail::state_buffer{
          state.finish(),
          memory::detail::slice_info::none,
        },
        nullptr,
      },
    };
  }

  template <class Container>
  static auto finish_typed_record(Container const& rows) -> array_<record> {
    using key_t = array_<record>::key_t;
    auto names = std::vector<std::string>{};
    auto fields = std::vector<row_value_builder>{};
    auto state = memory::shared_owner<memory::element_state[]>::builder{
      memory::default_resource()};
    auto keys_builder = memory::list_array_builder<memory::array_builder<key_t>,
                                                   memory::array<key_t>>{
      memory::default_resource(),
    };
    state.reserve(static_cast<std::ptrdiff_t>(rows.size()));
    for (auto row : rows) {
      auto view = *try_as<array_row_view_<record>>(&row);
      state.emplace_back(view.state());
      auto keys = keys_builder.list();
      if (not view.valid()) {
        continue;
      }
      for (auto [name, value] : *view) {
        auto it = std::ranges::find(names, name);
        auto field_index
          = static_cast<size_t>(std::distance(names.begin(), it));
        if (it == names.end()) {
          field_index = names.size();
          names.emplace_back(name);
          fields.emplace_back();
        }
        keys.emplace_back(field_index, fields[field_index].size());
        fields[field_index].append(value);
      }
    }
    auto arrays = std::vector<array_<data>>{};
    arrays.reserve(fields.size());
    for (auto& field : fields) {
      arrays.push_back(field.finish());
    }
    return array_<record>{
      std::move(names),
      std::move(arrays),
      memory::detail::state_buffer{
        state.finish(),
        memory::detail::slice_info::none,
      },
      keys_builder.finish(),
    };
  }

  template <data_type T, class Container>
  static auto finish_typed(Container const& rows) -> array_<T> {
    if constexpr (std::same_as<T, null>) {
      return finish_typed_null(rows);
    } else if constexpr (std::same_as<T, bool>) {
      return finish_typed_bool(rows);
    } else if constexpr (std::same_as<T, int64_t>) {
      return finish_typed_int64(rows);
    } else if constexpr (std::same_as<T, uint64_t>) {
      return finish_typed_uint64(rows);
    } else if constexpr (std::same_as<T, double>) {
      return finish_typed_double(rows);
    } else if constexpr (std::same_as<T, std::string>) {
      return finish_typed_string(rows);
    } else if constexpr (std::same_as<T, ip>) {
      return finish_typed_ip(rows);
    } else if constexpr (std::same_as<T, subnet>) {
      return finish_typed_subnet(rows);
    } else if constexpr (std::same_as<T, time>) {
      return finish_typed_time(rows);
    } else if constexpr (std::same_as<T, duration>) {
      return finish_typed_duration(rows);
    } else if constexpr (std::same_as<T, list>) {
      return finish_typed_list(rows);
    } else if constexpr (std::same_as<T, record>) {
      return finish_typed_record(rows);
    } else {
      static_assert(sizeof(T) == 0);
    }
  }

  std::vector<array_row_view_<data>> rows_;
};

auto null_row(memory::element_state state = memory::element_state::null)
  -> array_row_view_<data> {
  return array_row_view_<data>{array_row_view_<null>{state}};
}

auto field_index_of(array_row_view_<record> row, std::string_view name)
  -> std::optional<size_t> {
  if (not row.valid()) {
    return std::nullopt;
  }
  auto index = size_t{0};
  for (auto const& [field_name, value] : *row) {
    (void)value;
    if (field_name == name) {
      return index;
    }
    ++index;
  }
  return std::nullopt;
}

auto contains_ip(subnet const& network, ip const& address) -> bool {
  auto bits = network.length;
  if (bits > 128) {
    return false;
  }
  auto lhs = network.ip.data();
  auto rhs = address.data();
  auto bytes = bits / 8;
  auto tail = static_cast<uint8_t>(bits % 8);
  if (not std::equal(lhs.begin(), lhs.begin() + bytes, rhs.begin())) {
    return false;
  }
  if (tail == 0) {
    return true;
  }
  auto mask = static_cast<uint8_t>(0xFFu << (8u - tail));
  return (std::to_integer<uint8_t>(lhs[bytes]) & mask)
         == (std::to_integer<uint8_t>(rhs[bytes]) & mask);
}

auto make_null_array(std::ptrdiff_t length, memory::element_state state
                                            = memory::element_state::null)
  -> array_<data> {
  auto builder = array_builder_<data>{};
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    builder.null(state);
  }
  return builder.finish();
}

template <fundamental_type T>
  requires(not std::same_as<T, null>)
auto make_repeated_array(T const& value, std::ptrdiff_t length)
  -> array_<data> {
  auto builder = array_builder_<T>{memory::default_resource()};
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    builder.data(value);
  }
  return builder.finish();
}

auto convert_ip(tenzir::ip const& x) -> ip {
  auto storage = ip::storage{};
  auto bytes = as_bytes(x);
  std::copy(bytes.begin(), bytes.end(), storage.begin());
  return ip{storage};
}

auto convert_duration(tenzir::duration const& x) -> duration {
  return duration{
    std::chrono::duration_cast<duration::base>(x),
  };
}

auto convert_time(tenzir::time const& x) -> time {
  return time{
    time::base{
      std::chrono::duration_cast<time::base::duration>(x.time_since_epoch()),
    },
  };
}

auto convert_subnet(tenzir::subnet const& x) -> subnet {
  return {
    .ip = convert_ip(x.network()),
    .length = x.length(),
  };
}

auto append_legacy_data(array_builder_<data>& builder,
                        tenzir::data const& value) -> bool {
  return tenzir::match(
    value,
    [&](caf::none_t) -> bool {
      builder.null();
      return true;
    },
    [&](bool x) -> bool {
      builder.data(x);
      return true;
    },
    [&](int64_t x) -> bool {
      builder.data(x);
      return true;
    },
    [&](uint64_t x) -> bool {
      builder.data(x);
      return true;
    },
    [&](double x) -> bool {
      builder.data(x);
      return true;
    },
    [&](tenzir::duration const& x) -> bool {
      builder.data(convert_duration(x));
      return true;
    },
    [&](tenzir::time const& x) -> bool {
      builder.data(convert_time(x));
      return true;
    },
    [&](std::string const& x) -> bool {
      builder.data(x);
      return true;
    },
    [&](tenzir::pattern const&) -> bool {
      return false;
    },
    [&](tenzir::ip const& x) -> bool {
      builder.data(convert_ip(x));
      return true;
    },
    [&](tenzir::subnet const& x) -> bool {
      builder.data(convert_subnet(x));
      return true;
    },
    [&](tenzir::enumeration) -> bool {
      return false;
    },
    [&](tenzir::list const& x) -> bool {
      if (not x.empty()) {
        return false;
      }
      auto row = builder.list();
      (void)row;
      return true;
    },
    [&](tenzir::map const&) -> bool {
      return false;
    },
    [&](tenzir::record const& x) -> bool {
      auto row = builder.record();
      for (auto const& [name, field] : x) {
        if (not append_legacy_data(row.field(name), field)) {
          return false;
        }
      }
      return true;
    },
    [&](tenzir::blob const&) -> bool {
      return false;
    },
    [&](tenzir::secret const&) -> bool {
      return false;
    });
}

auto make_legacy_constant_array(tenzir::data const& value,
                                std::ptrdiff_t length)
  -> std::optional<array_<data>> {
  auto builder = array_builder_<data>{};
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    if (not append_legacy_data(builder, value)) {
      return std::nullopt;
    }
  }
  return builder.finish();
}

auto supports_direct_field_projection(array_<record> const& arr) -> bool {
  if (not arr.is_uniform()) {
    return false;
  }
  for (auto row = std::ptrdiff_t{0}; row < arr.length(); ++row) {
    if (arr.state(row) != memory::element_state::valid) {
      return false;
    }
  }
  return true;
}

auto find_field_index(array_<record> const& arr, std::string_view name)
  -> std::optional<size_t> {
  if (not supports_direct_field_projection(arr) or arr.length() == 0) {
    return std::nullopt;
  }
  auto row = arr.get(0);
  TENZIR_ASSERT(row.valid());
  auto field_index = size_t{0};
  for (auto const& [field_name, value] : *row) {
    (void)value;
    if (field_name == name) {
      return field_index;
    }
    ++field_index;
  }
  return std::nullopt;
}

auto field_count(array_<record> const& arr) -> size_t {
  if (not supports_direct_field_projection(arr) or arr.length() == 0) {
    return 0;
  }
  auto row = arr.get(0);
  TENZIR_ASSERT(row.valid());
  auto count = size_t{0};
  for (auto const& [field_name, value] : *row) {
    (void)field_name;
    (void)value;
    ++count;
  }
  return count;
}

auto suggest_field_name(std::string_view requested_field,
                        array_<record> const& rec)
  -> std::optional<std::string_view> {
  if (not supports_direct_field_projection(rec) or rec.length() == 0) {
    return std::nullopt;
  }
  auto best_field = std::string_view{};
  auto best_similarity = int64_t{std::numeric_limits<int64_t>::min()};
  auto row = rec.get(0);
  TENZIR_ASSERT(row.valid());
  for (auto const& [field_name, value] : *row) {
    (void)value;
    if (field_name.empty()) {
      continue;
    }
    auto similarity
      = tenzir::detail::calculate_similarity(requested_field, field_name);
    if (similarity > best_similarity) {
      best_similarity = similarity;
      best_field = field_name;
    }
  }
  if (best_field.empty() or best_similarity <= -3) {
    return std::nullopt;
  }
  return best_field;
}

auto data_kind_name(array_row_view_<data> row) -> std::string_view {
  return match(row, []<data_type T>(array_row_view_<T>) -> std::string_view {
    if constexpr (std::same_as<T, null>) {
      return "null";
    } else if constexpr (std::same_as<T, bool>) {
      return "bool";
    } else if constexpr (std::same_as<T, std::int64_t>) {
      return "int64";
    } else if constexpr (std::same_as<T, std::uint64_t>) {
      return "uint64";
    } else if constexpr (std::same_as<T, double>) {
      return "double";
    } else if constexpr (std::same_as<T, std::string>) {
      return "string";
    } else if constexpr (std::same_as<T, ip>) {
      return "ip";
    } else if constexpr (std::same_as<T, subnet>) {
      return "subnet";
    } else if constexpr (std::same_as<T, time>) {
      return "time";
    } else if constexpr (std::same_as<T, duration>) {
      return "duration";
    } else if constexpr (std::same_as<T, list>) {
      return "list";
    } else if constexpr (std::same_as<T, record>) {
      return "record";
    }
  });
}

auto make_empty_record_array(std::ptrdiff_t length) -> array_<data> {
  auto builder = array_builder_<record>{memory::default_resource()};
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    auto record = builder.record();
    (void)record;
  }
  return builder.finish();
}

auto make_empty_list_array(std::ptrdiff_t length) -> array_<data> {
  auto builder = array_builder_<data>{};
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    auto list = builder.list();
    (void)list;
  }
  return builder.finish();
}

} // namespace

auto evaluator::eval(tenzir::ast::record const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  if (x.items.empty()) {
    return make_empty_record_array(length_);
  }
  using key_t = array_<record>::key_t;
  auto item_values = std::vector<array_<data>>{};
  item_values.reserve(x.items.size());
  for (auto const& item : x.items) {
    item_values.push_back(item.match(
      [&](tenzir::ast::record::field const& field) {
        return eval(field.expr, active);
      },
      [&](tenzir::ast::spread const& spread) {
        return eval(spread.expr, active);
      }));
  }
  auto names = std::vector<std::string>{};
  auto fields = std::vector<row_value_builder>{};
  auto state = memory::shared_owner<memory::element_state[]>::builder{
    memory::default_resource()};
  auto keys_builder = memory::list_array_builder<memory::array_builder<key_t>,
                                                 memory::array<key_t>>{
    memory::default_resource(),
  };
  auto warned_spread = false;
  for (auto row = std::ptrdiff_t{0}; row < length_; ++row) {
    auto row_state = active.is_active(static_cast<int64_t>(row))
                       ? memory::element_state::valid
                       : memory::element_state::dead;
    state.emplace_back(row_state);
    auto keys = keys_builder.list();
    if (row_state != memory::element_state::valid) {
      continue;
    }
    auto row_fields = std::vector<std::optional<array_row_view_<data>>>{};
    row_fields.resize(names.size());
    auto set_field = [&](std::string_view name, array_row_view_<data> value) {
      auto it = std::ranges::find(names, name);
      auto field_index = static_cast<size_t>(std::distance(names.begin(), it));
      if (it == names.end()) {
        field_index = names.size();
        names.emplace_back(name);
        fields.emplace_back();
        row_fields.resize(names.size());
      }
      row_fields[field_index] = value;
    };
    for (auto [item, values] : std::views::zip(x.items, item_values)) {
      auto value = values.get(row);
      item.match(
        [&](tenzir::ast::record::field const& field) {
          set_field(field.name.name, value);
        },
        [&](tenzir::ast::spread const& spread) {
          if (auto* record = try_as<array_row_view_<tenzir2::record>>(&value)) {
            if (not record->valid()) {
              return;
            }
            for (auto [name, field_value] : **record) {
              set_field(name, field_value);
            }
            return;
          }
          if (not warned_spread) {
            warned_spread = true;
            tenzir::diagnostic::warning("expected record for record spread")
              .primary(spread.expr)
              .emit(ctx_);
          }
        });
    }
    for (auto field_index = size_t{0}; field_index < row_fields.size();
         ++field_index) {
      if (not row_fields[field_index]) {
        continue;
      }
      keys.emplace_back(field_index, fields[field_index].size());
      fields[field_index].append(*row_fields[field_index]);
    }
  }
  auto arrays = std::vector<array_<data>>{};
  arrays.reserve(fields.size());
  for (auto& field : fields) {
    arrays.push_back(field.finish());
  }
  return array_<record>{
    std::move(names),
    std::move(arrays),
    memory::detail::state_buffer{
      state.finish(),
      memory::detail::slice_info::none,
    },
    keys_builder.finish(),
  };
}

auto evaluator::eval(tenzir::ast::list const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  if (x.items.empty()) {
    return make_empty_list_array(length_);
  }
  auto item_values = std::vector<array_<data>>{};
  item_values.reserve(x.items.size());
  for (auto const& item : x.items) {
    item_values.push_back(item.match(
      [&](tenzir::ast::expression const& expr) {
        return eval(expr, active);
      },
      [&](tenzir::ast::spread const& spread) {
        return eval(spread.expr, active);
      }));
  }
  auto values = row_value_builder{};
  auto bounds = memory::shared_owner<std::ptrdiff_t[]>::builder{
    memory::default_resource()};
  auto state = memory::shared_owner<memory::element_state[]>::builder{
    memory::default_resource()};
  auto count = std::ptrdiff_t{0};
  auto warned_spread = false;
  bounds.reserve(length_);
  state.reserve(length_);
  for (auto row = std::ptrdiff_t{0}; row < length_; ++row) {
    auto row_state = active.is_active(static_cast<int64_t>(row))
                       ? memory::element_state::valid
                       : memory::element_state::dead;
    state.emplace_back(row_state);
    if (row_state != memory::element_state::valid) {
      bounds.emplace_back(count);
      continue;
    }
    for (auto [item, item_array] : std::views::zip(x.items, item_values)) {
      auto value = item_array.get(row);
      item.match(
        [&](tenzir::ast::expression const&) {
          values.append(value);
          ++count;
        },
        [&](tenzir::ast::spread const& spread) {
          if (auto* list = try_as<array_row_view_<tenzir2::list>>(&value)) {
            if (not list->valid()) {
              return;
            }
            for (auto element : **list) {
              values.append(element);
              ++count;
            }
            return;
          }
          if (not warned_spread) {
            warned_spread = true;
            tenzir::diagnostic::warning("expected list for list spread")
              .primary(spread.expr)
              .emit(ctx_);
          }
        });
    }
    bounds.emplace_back(count);
  }
  return array_<list>{
    memory::list_array<array_<data>>{
      values.finish(),
      memory::detail::position_buffer{
        bounds.finish(),
        memory::detail::slice_info::none,
      },
      memory::detail::state_buffer{
        state.finish(),
        memory::detail::slice_info::none,
      },
      nullptr,
    },
  };
}

auto evaluator::eval(tenzir::ast::field_access const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  if (active.as_constant() == false) {
    return make_null_array(length_);
  }
  auto left = eval(x.left, active);
  if (auto* records = try_as<array_<record>>(&left)) {
    if (supports_direct_field_projection(*records)) {
      if (auto index = find_field_index(*records, x.name.name)) {
        return array_<data>{records->value_array(*index)};
      }
    }
  }
  auto values = row_value_builder{};
  auto warned_null = false;
  auto warned_non_record = false;
  auto non_record_type = std::optional<std::string_view>{};
  auto saw_missing = false;
  for (auto row = std::ptrdiff_t{0}; row < length_; ++row) {
    auto parent = left.get(row);
    if (auto* record = try_as<array_row_view_<tenzir2::record>>(&parent)) {
      if (not record->valid()) {
        if (not x.suppress_warnings()
            and active.is_active(static_cast<int64_t>(row))) {
          warned_null = true;
        }
        values.append(null_row(record->state()));
        continue;
      }
      auto found = false;
      for (auto [name, value] : **record) {
        if (name != x.name.name) {
          continue;
        }
        values.append(value);
        found = true;
        break;
      }
      if (found) {
        continue;
      }
      saw_missing = true;
      values.append(null_row());
      continue;
    }
    if (active.is_active(static_cast<int64_t>(row))) {
      if (try_as<array_row_view_<tenzir2::null>>(&parent)) {
        warned_null = true;
      } else {
        warned_non_record = true;
        if (not non_record_type) {
          non_record_type = data_kind_name(parent);
        }
      }
    }
    values.append(null_row(parent.state()));
  }
  if (warned_null and not x.suppress_warnings()) {
    tenzir::diagnostic::warning("tried to access field of `null`")
      .primary(x.name)
      .note("field name is `{}`", x.name.name)
      .hint("append `?` to suppress this warning")
      .emit(ctx_);
  }
  if (warned_non_record) {
    tenzir::diagnostic::warning("cannot access field of non-record type")
      .primary(x.name)
      .secondary(x.left, "type `{}`", *non_record_type)
      .note("field name is `{}`", x.name.name)
      .emit(ctx_);
  }
  if (saw_missing and not x.suppress_warnings()) {
    auto diag = tenzir::diagnostic::warning("record does not have this field")
                  .primary(x.name)
                  .note("field name is `{}`", x.name.name);
    if (auto* records = try_as<array_<record>>(&left)) {
      diag = std::move(diag).compose([&](auto&& d) {
        auto suggestion = suggest_field_name(x.name.name, *records);
        return suggestion ? std::move(d).hint("did you mean `{}`?", *suggestion)
                          : d;
      });
    }
    std::move(diag)
      .hint(std::string{"append `?` to suppress this warning"})
      .emit(ctx_);
  }
  return values.finish();
}

auto evaluator::eval(tenzir::ast::function_call const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  return not_implemented(x);
}

auto evaluator::eval(tenzir::ast::this_ const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  return array_<data>{input_or_throw(x).data_};
}

auto evaluator::eval(tenzir::ast::root_field const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  auto const& input = input_or_throw(x);
  if (auto index = find_field_index(input.data_, x.id.name)) {
    return array_<data>{input.data_.value_array(*index)};
  }
  auto values = row_value_builder{};
  auto saw_missing = false;
  for (auto row = std::ptrdiff_t{0}; row < length_; ++row) {
    auto record = input.data_.get(row);
    if (not record.valid()) {
      values.append(null_row(record.state()));
      continue;
    }
    auto found = false;
    for (auto [name, value] : *record) {
      if (name != x.id.name) {
        continue;
      }
      values.append(value);
      found = true;
      break;
    }
    if (found) {
      continue;
    }
    saw_missing = true;
    values.append(null_row());
  }
  if (not saw_missing) {
    return values.finish();
  }
  if (not x.has_question_mark) {
    tenzir::diagnostic::warning("field `{}` not found", x.id.name)
      .primary(x.id)
      .compose([&](auto&& d) {
        auto suggestion = suggest_field_name(x.id.name, input.data_);
        return suggestion ? std::move(d).hint("did you mean `{}`?", *suggestion)
                          : d;
      })
      .hint(std::string{"append `?` to suppress this warning"})
      .emit(ctx_);
  }
  return values.finish();
}

auto evaluator::eval(tenzir::ast::index_expr const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  if (auto const* constant = tenzir::try_as<tenzir::ast::constant>(x.index)) {
    if (auto const* string = tenzir::try_as<std::string>(constant->value)) {
      return eval(
        tenzir::ast::field_access{
          x.expr,
          tenzir::location::unknown,
          x.has_question_mark,
          tenzir::ast::identifier{*string, constant->source},
        },
        active);
    }
  }
  auto value = eval(x.expr, active);
  auto index = eval(x.index, active);
  auto const* record_values = try_as<array_<record>>(&value);
  auto result = row_value_builder{};
  auto warned_null_value = false;
  auto warned_null_list = false;
  auto warned_null_index = false;
  auto warned_bad_string_target = false;
  auto warned_bad_number_target = false;
  auto bad_string_target_type = std::optional<std::string_view>{};
  auto bad_number_target_type = std::optional<std::string_view>{};
  auto warned_bad_index = false;
  auto warned_out_of_bounds = false;
  auto missing_fields = std::vector<std::string>{};
  auto add_suppress_hint = [&](auto diag) {
    return std::move(diag).hint(
      x.rbracket != tenzir::location::unknown
        ? "use `[…]?` to suppress this warning"
        : "provide a fallback value to suppress this warning");
  };
  for (auto row = std::ptrdiff_t{0}; row < length_; ++row) {
    if (not active.is_active(static_cast<int64_t>(row))) {
      result.append(null_row(memory::element_state::dead));
      continue;
    }
    auto value_row = value.get(row);
    auto index_row = index.get(row);
    if (not index_row.valid()) {
      warned_null_index = true;
      result.append(null_row(index_row.state()));
      continue;
    }
    if (auto* field = try_as<array_row_view_<std::string>>(&index_row)) {
      auto* record = try_as<array_row_view_<tenzir2::record>>(&value_row);
      if (record and not record->valid()) {
        warned_null_value = true;
        result.append(null_row(record->state()));
        continue;
      }
      if (not record) {
        if (try_as<array_row_view_<tenzir2::null>>(&value_row)) {
          warned_null_value = true;
        } else {
          warned_bad_string_target = true;
          if (not bad_string_target_type) {
            bad_string_target_type = data_kind_name(value_row);
          }
        }
        result.append(null_row(value_row.state()));
        continue;
      }
      auto found = false;
      for (auto [name, field_value] : **record) {
        if (name != **field) {
          continue;
        }
        result.append(field_value);
        found = true;
        break;
      }
      if (found) {
        continue;
      }
      if (not x.has_question_mark
          and std::ranges::find(missing_fields, std::string{**field})
                == missing_fields.end()) {
        missing_fields.emplace_back(**field);
      }
      result.append(null_row());
      continue;
    }
    if (auto* number = try_as<array_row_view_<int64_t>>(&index_row)) {
      if (auto* record = try_as<array_row_view_<tenzir2::record>>(&value_row)) {
        if (not record->valid()) {
          warned_null_value = true;
          result.append(null_row(record->state()));
          continue;
        }
        auto target = **number;
        if (target < 0) {
          warned_out_of_bounds = true;
          result.append(null_row());
          continue;
        }
        auto fields = **record;
        auto count = std::ptrdiff_t{0};
        auto found = std::optional<array_row_view_<data>>{};
        for (auto [name, field_value] : fields) {
          (void)name;
          if (count == target) {
            found = field_value;
            break;
          }
          ++count;
        }
        if (not found) {
          warned_out_of_bounds = true;
          result.append(null_row());
          continue;
        }
        result.append(*found);
        continue;
      }
      if (auto* list = try_as<array_row_view_<tenzir2::list>>(&value_row)) {
        if (not list->valid()) {
          warned_null_list = true;
          result.append(null_row(list->state()));
          continue;
        }
        auto target = **number;
        auto count = list->length();
        if (target < 0) {
          target += count;
        }
        if (target < 0 or target >= count) {
          warned_out_of_bounds = true;
          result.append(null_row());
          continue;
        }
        result.append(array_row_view_<data>{list->get(target)});
        continue;
      }
      if (try_as<array_row_view_<tenzir2::null>>(&value_row)) {
        warned_null_value = true;
      } else {
        warned_bad_number_target = true;
        if (not bad_number_target_type) {
          bad_number_target_type = data_kind_name(value_row);
        }
      }
      result.append(null_row(value_row.state()));
      continue;
    }
    warned_bad_index = true;
    result.append(null_row());
  }
  if (warned_null_value and not x.has_question_mark) {
    tenzir::diagnostic::warning("tried to access field of `null`")
      .primary(x.expr, "is null")
      .compose(add_suppress_hint)
      .emit(ctx_);
  }
  if (warned_null_list and not x.has_question_mark) {
    tenzir::diagnostic::warning("cannot index into `null`")
      .primary(x.expr, "is null")
      .compose(add_suppress_hint)
      .emit(ctx_);
  }
  if (warned_null_index and not x.has_question_mark) {
    tenzir::diagnostic::warning("cannot use `null` as index")
      .primary(x.index, "is null")
      .compose(add_suppress_hint)
      .emit(ctx_);
  }
  if (warned_bad_string_target) {
    tenzir::diagnostic::warning("cannot access field of non-record type")
      .primary(x.index)
      .secondary(x.expr, "has type `{}`", *bad_string_target_type)
      .emit(ctx_);
  }
  if (warned_bad_number_target) {
    tenzir::diagnostic::warning("expected `record` or `list`")
      .primary(x.expr, "has type `{}`", *bad_number_target_type)
      .compose(add_suppress_hint)
      .emit(ctx_);
  }
  if (warned_bad_index) {
    tenzir::diagnostic::warning("expected `string` or `int64` as index")
      .primary(x.index)
      .emit(ctx_);
  }
  if (warned_out_of_bounds and not x.has_question_mark) {
    tenzir::diagnostic::warning("index out of bounds")
      .primary(x.index, "is out of bounds")
      .compose(add_suppress_hint)
      .emit(ctx_);
  }
  if (not x.has_question_mark) {
    for (auto const& missing : missing_fields) {
      auto diag = tenzir::diagnostic::warning("record does not have field `{}`",
                                              missing)
                    .primary(x.index, "does not exist");
      if (record_values) {
        diag = std::move(diag).compose([&](auto&& d) {
          auto suggestion = suggest_field_name(missing, *record_values);
          return suggestion
                   ? std::move(d).hint("did you mean `{}`?", *suggestion)
                   : d;
        });
      }
      std::move(diag).compose(add_suppress_hint).emit(ctx_);
    }
  }
  return result.finish();
}

auto evaluator::eval(tenzir::ast::meta const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  auto const& input = input_or_throw(x);
  switch (x.kind) {
    case tenzir::ast::meta::name:
      return make_repeated_array<std::string>(input.name_, length_);
    case tenzir::ast::meta::import_time:
      if (input.ingest_time_ == time{}) {
        return make_null_array(length_);
      }
      return make_repeated_array(input.ingest_time_, length_);
    case tenzir::ast::meta::internal:
      return make_repeated_array(
        input.data_.type_info().meta
          and input.data_.type_info().meta->get_attribute("internal").has_value(),
        length_);
  }
  TENZIR_UNREACHABLE();
}

auto evaluator::eval(tenzir::ast::assignment const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  tenzir::diagnostic::warning("unexpected assignment").primary(x).emit(ctx_);
  return null();
}

auto evaluator::eval(tenzir::ast::constant const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  if (auto result = make_legacy_constant_array(x.as_data(), length_)) {
    return std::move(*result);
  }
  tenzir::diagnostic::warning(
    "constant evaluation is only partially implemented in "
    "`tenzir2::evaluator`")
    .primary(x)
    .emit(ctx_);
  return null();
}

auto evaluator::eval(tenzir::ast::format_expr const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  auto cols = std::vector<variant<std::string, array_<data>>>{};
  cols.reserve(x.segments.size());
  for (auto const& segment : x.segments) {
    segment.match(
      [&](std::string const& text) {
        cols.emplace_back(text);
      },
      [&](tenzir::ast::format_expr::replacement const& replacement) {
        cols.emplace_back(eval(replacement.expr, active));
      });
  }
  auto builder = array_builder_<std::string>{memory::default_resource()};
  for (auto row = std::ptrdiff_t{0}; row < length_; ++row) {
    auto result = std::string{};
    for (auto const& col : cols) {
      match(
        col,
        [&](std::string const& text) {
          result += text;
        },
        [&](array_<data> const& values) {
          result += format_tql(values.get(row), {.compact = true});
        });
    }
    builder.data(result);
  }
  return builder.finish();
}

auto evaluator::eval(tenzir::ast::lambda_expr const& x,
                     array_<list> const& input) -> array_<data> {
  TENZIR_UNUSED(input);
  return not_implemented(x);
}

auto evaluator::eval(tenzir::ast::expression const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  return tenzir::trace_panic(x, [&] {
    auto result = x.match([&](auto const& y) {
      return eval(y, active);
    });
    TENZIR_ASSERT_EQ(result.length(), static_cast<std::ptrdiff_t>(length_),
                     "got length {} instead of {} while evaluating {:?}",
                     result.length(), length_, x);
    return result;
  });
}

auto evaluator::eval(tenzir::ast::binary_expr const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  return not_implemented(x);
}

auto evaluator::to_array(data const& x) const -> array_<data> {
  TENZIR_UNUSED(x);
  return make_null_array(length_);
}

} // namespace tenzir2
