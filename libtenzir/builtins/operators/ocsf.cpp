//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ocsf.hpp"

#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/view3.hpp"

namespace tenzir::plugins::ocsf {
namespace {

auto cast(const arrow::Array& array, const type& ty)
  -> std::shared_ptr<arrow::Array>;

template <basic_type Type>
auto cast(const type_to_arrow_array_t<Type>& array, const Type&)
  -> std::shared_ptr<type_to_arrow_array_t<Type>> {
  return std::make_shared<type_to_arrow_array_t<Type>>(array.data());
}

auto cast(const enumeration_type::array_type&, const enumeration_type&)
  -> std::shared_ptr<enumeration_type::array_type> {
  TENZIR_UNREACHABLE();
}

auto cast(const arrow::MapArray&, const map_type&)
  -> std::shared_ptr<arrow::MapArray> {
  TENZIR_UNREACHABLE();
}

auto cast(const arrow::ListArray& array, const list_type& ty)
  -> std::shared_ptr<arrow::ListArray> {
  auto values = cast(*array.values(), ty.value_type());
  // TODO: What about `array.offset()`?
  return check(arrow::ListArray::FromArrays(
    *array.offsets(), *values, arrow::default_memory_pool(),
    array.null_bitmap(), array.data()->null_count));
}

auto cast(const arrow::StructArray& array, const record_type& ty)
  -> std::shared_ptr<arrow::StructArray> {
  auto fields = arrow::FieldVector{};
  auto field_arrays = arrow::ArrayVector{};
  for (auto&& field : ty.fields()) {
    fields.push_back(field.type.to_arrow_field(field.name));
    auto field_array = array.GetFieldByName(std::string{field.name});
    if (not field_array) {
      field_arrays.push_back(check(
        arrow::MakeArrayOfNull(field.type.to_arrow_type(), array.length())));
      continue;
    }
    field_arrays.push_back(cast(*field_array, field.type));
  }
  return make_struct_array(array.length(), array.null_bitmap(), fields,
                           field_arrays);
}

auto cast(const arrow::Array& array, const type& ty)
  -> std::shared_ptr<arrow::Array> {
  if (is<string_type>(ty) and ty.attribute("print_json")) {
    if (is<arrow::StringArray>(array)) {
      // Keep strings as they are (assuming they are already JSON).
      return std::make_shared<arrow::StringArray>(array.data());
    }
    // Convert everything else into JSON.
    auto builder = arrow::StringBuilder{};
    auto printer = json_printer{{.style = no_style(), .oneline = true}};
    auto buffer = std::string{};
    for (auto&& row : values3(array)) {
      auto it = std::back_inserter(buffer);
      auto success = printer.print(it, row);
      TENZIR_ASSERT(success);
      check(builder.Append(buffer));
      buffer.clear();
    }
    return finish(builder);
  }
  return match(
    std::tie(array, ty),
    []<class Type>(const type_to_arrow_array_t<Type>& array,
                   const Type& ty) -> std::shared_ptr<arrow::Array> {
      return cast(array, ty);
    },
    []<class Array, class Type>(const Array& array, const Type& ty)
      requires(not std::same_as<Array, type_to_arrow_array_t<Type>>)
    {
      return check(arrow::MakeArrayOfNull(ty.to_arrow_type(), array.length()));
    });
}

auto cast(const table_slice& slice, const type& ty) -> table_slice {
  auto array = check(to_record_batch(slice)->ToStructArray());
  TENZIR_ASSERT(array);
  auto result = std::dynamic_pointer_cast<arrow::StructArray>(cast(*array, ty));
  TENZIR_ASSERT(result);
  auto columns = check(result->Flatten());
  return table_slice{
    arrow::RecordBatch::Make(ty.to_arrow_schema(), result->length(),
                             std::move(columns)),
    ty,
  };
}

class ocsf_operator final : public crtp_operator<ocsf_operator> {
public:
  ocsf_operator() = default;

  explicit ocsf_operator(struct location self) : self_{self} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto ty = as<record_type>(slice.schema());
      auto class_index = ty.resolve_field("class_uid");
      if (not class_index) {
        diagnostic::warning("dropping events where `class_uid` does not exist")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      auto class_array = try_as<arrow::Int64Array>(
        *to_record_batch(slice)->column(detail::narrow<int>(*class_index)));
      if (not class_array) {
        diagnostic::warning(
          "dropping events where `class_uid` is not an integer")
          .primary(self_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      // Figure out longest slices where the class ID is the constant.
      auto begin = int64_t{0};
      auto end = begin;
      auto id = view_at(*class_array, begin);
      auto process = [&]() -> table_slice {
        if (not id) {
          diagnostic::warning("dropping events where `class_uid` is null")
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto name = ocsf_class_name(*id);
        if (not name) {
          diagnostic::warning("dropping events where `class_uid` is unknown")
            .primary(self_)
            .note("could not find class for ID {}", *id)
            .emit(ctrl.diagnostics());
          return {};
        }
        auto schema = std::string{"ocsf."};
        schema.reserve(schema.size() + name->length());
        for (auto c : *name) {
          if (c == ' ') {
            schema += '_';
          } else {
            schema
              += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          }
        }
        auto it = std::ranges::find_if(modules::schemas(), [&](const type& ty) {
          return ty.name() == schema;
        });
        if (it == modules::schemas().end()) {
          diagnostic::warning(
            "dropping events because schema `{}` is not available", schema)
            .primary(self_)
            .emit(ctrl.diagnostics());
          return {};
        }
        return cast(subslice(slice, begin, end), *it);
      };
      for (; end < class_array->length(); ++end) {
        auto next_id = view_at(*class_array, end);
        if (next_id == id) {
          continue;
        }
        co_yield process();
        begin = end;
        id = next_id;
      }
      co_yield process();
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "ocsf::apply";
  }

  friend auto inspect(auto& f, ocsf_operator& x) -> bool {
    return f.apply(x.self_);
  }

private:
  struct location self_;
};

class ocsf_plugin final : public operator_plugin2<ocsf_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_(name()).parse(inv, ctx).ignore();
    return std::make_unique<ocsf_operator>(inv.self.get_location());
  }
};

} // namespace
} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::ocsf_plugin)
