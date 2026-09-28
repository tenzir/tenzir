//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/nova/array.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/type_system.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array.h>

#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::plugins::internal_memory_size {

namespace {

class byte_size_visitor {
public:
  byte_size_visitor(std::span<int64_t> sizes, std::vector<int64_t> buffer = {},
                    bool buffer_in_use = false)
    : sizes_{sizes}, buffer_{std::move(buffer)}, buffer_in_use_{buffer_in_use} {
  }

  auto operator()(basic_series<record_type> const& input) -> void {
    auto fields = check(input.array->Flatten(arrow_memory_pool()));
    auto index = size_t{0};
    for (auto const& field : input.type.fields()) {
      match(series{field.type, fields[index]}, *this);
      ++index;
    }
  }

  auto operator()(basic_series<string_type> const& input) -> void {
    for (auto i = int64_t{0}; i < input.length(); ++i) {
      if (input.array->IsValid(i)) {
        sizes_[i] += input.array->value_length(i);
      }
    }
  }

  auto operator()(basic_series<blob_type> const& input) -> void {
    for (auto i = int64_t{0}; i < input.length(); ++i) {
      if (input.array->IsValid(i)) {
        sizes_[i] += input.array->value_length(i);
      }
    }
  }

  auto operator()(basic_series<null_type> const& input) -> void {
    for (auto i = int64_t{0}; i < input.length(); ++i) {
      sizes_[i] += 1;
    }
  }

  auto operator()(basic_series<list_type> const& input) -> void {
    auto values = input.list_values();
    auto value_count = static_cast<size_t>(values.length());
    auto was_buffer_in_use = buffer_in_use_;
    auto visitor = byte_size_visitor{
      std::span<int64_t>{},
      was_buffer_in_use ? std::vector<int64_t>{} : std::move(buffer_),
      true,
    };
    visitor.buffer_.assign(value_count, int64_t{0});
    visitor.sizes_ = std::span{visitor.buffer_};
    match(values, visitor);
    auto values_start = input.array->value_offset(0);
    for (auto i = int64_t{0}; i < input.length(); ++i) {
      if (input.array->IsNull(i)) {
        continue;
      }
      auto begin = input.array->value_offset(i) - values_start;
      auto end = begin + input.array->value_length(i);
      for (auto j = begin; j < end; ++j) {
        sizes_[i] += visitor.sizes_[static_cast<size_t>(j)];
      }
    }
    if (not was_buffer_in_use) {
      buffer_ = std::move(visitor.buffer_);
    }
  }

  template <class Type>
  auto operator()(basic_series<Type> const& input) -> void
    requires(std::same_as<Type, bool_type> or numeric_type<Type>
             or std::same_as<Type, duration_type>
             or std::same_as<Type, time_type> or std::same_as<Type, ip_type>
             or std::same_as<Type, subnet_type>
             or std::same_as<Type, enumeration_type>
             or std::same_as<Type, secret_type>)
  {
    for (auto i = int64_t{0}; i < input.length(); ++i) {
      if (input.array->IsValid(i)) {
        sizes_[i] += sizeof(type_to_data_t<Type>);
      }
    }
  }

  auto operator()(auto const&) -> void {
    TENZIR_UNREACHABLE();
  }

  std::span<int64_t> sizes_;
  std::vector<int64_t> buffer_;
  bool buffer_in_use_ = false;
};

struct InternalMemorySizeArgs {
  nova::ValueArgument x;
};

class InternalMemorySizeFunction {
public:
  static auto eval(InternalMemorySizeArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto size_view
      = [](auto const& self, RowView<Data> const& value) -> int64_t {
      return match(value, [&](auto const& value) -> int64_t {
        using Value = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::same_as<Value, RowView<Null>>) {
          return 1;
        } else if constexpr (std::same_as<Value, RowView<List>>) {
          auto result = int64_t{0};
          for (auto element : value) {
            result += self(self, element);
          }
          return result;
        } else if constexpr (std::same_as<Value, RowView<Record>>) {
          auto result = int64_t{0};
          for (auto [_, field] : value) {
            result += self(self, field);
          }
          return result;
        } else if constexpr (std::same_as<Value, RowView<String>>
                             or std::same_as<Value, RowView<Blob>>
                             or std::same_as<Value, RowView<Secret>>) {
          return static_cast<int64_t>((*value).size());
        } else {
          return static_cast<int64_t>(sizeof(*value));
        }
      });
    };
    auto size_at = [&size_view](auto const& self, Array<Data> const& array,
                                storage::Index row) -> int64_t {
      return match(
        array,
        [&]<data_type Tag>(Array<Tag> const& array) -> int64_t {
          if constexpr (std::same_as<Tag, List>) {
            return match(array.storage(), [&](auto const& storage) -> int64_t {
              using Storage = std::remove_cvref_t<decltype(storage)>;
              if constexpr (std::same_as<Storage, storage::ListStorage>) {
                auto const span = storage.spans()[row];
                auto result = int64_t{0};
                for (auto i = span.begin; i < span.end; ++i) {
                  result += self(self, storage.values(), i);
                }
                return result;
              } else {
                auto result = int64_t{0};
                for (auto value : array.get(row)) {
                  result += size_view(size_view, value);
                }
                return result;
              }
            });
          } else if constexpr (std::same_as<Tag, Record>) {
            return match(array.storage(), [&](auto const& storage) -> int64_t {
              using Storage = std::remove_cvref_t<decltype(storage)>;
              if constexpr (std::same_as<Storage, storage::RecordStorage>) {
                auto const& primary = *storage;
                auto const shape = primary.shape_indices.get(row);
                auto result = int64_t{0};
                for (auto field : primary.shape_table.fields(shape)) {
                  auto const& value = primary.arrays[field];
                  if (value.present.get(row)) {
                    result += self(self, value.data, row);
                  } else {
                    result += sizeof(Null);
                  }
                }
                return result;
              } else {
                auto result = int64_t{0};
                for (auto [_, value] : array.get(row)) {
                  result += size_view(size_view, value);
                }
                return result;
              }
            });
          } else if constexpr (std::same_as<Tag, String>
                               or std::same_as<Tag, Blob>
                               or std::same_as<Tag, Secret>) {
            return static_cast<int64_t>(array.get(row).operator*().size());
          } else {
            return match(array.storage(), [](auto const& storage) {
              using Storage = std::remove_cvref_t<decltype(storage)>;
              return static_cast<int64_t>(sizeof(typename Storage::ViewType));
            });
          }
        },
        [&](UnionArray const& array) -> int64_t {
          auto result = int64_t{0};
          auto add = [&]<data_type Tag>() {
            if (auto alternative = array.get_alternative<Tag>();
                alternative and alternative->present.get(row)) {
              result += self(self, alternative->data, row);
            }
          };
          add.template operator()<Null>();
          add.template operator()<Bool>();
          add.template operator()<Int>();
          add.template operator()<UInt>();
          add.template operator()<Float>();
          add.template operator()<String>();
          add.template operator()<Blob>();
          add.template operator()<Secret>();
          add.template operator()<Ip>();
          add.template operator()<Subnet>();
          add.template operator()<Time>();
          add.template operator()<Duration>();
          add.template operator()<List>();
          add.template operator()<Record>();
          return result;
        });
    };
    auto mut = Type<Int>::PrimaryPhysicalStorage::Mutable{args.x.data.length()};
    for (auto row : storage::true_bits(frame.mask())) {
      mut.set(row, size_at(size_at, args.x.data, row));
    }
    return Array<Data>{Array<Int>{std::move(mut).finish()}}.null_where(
      frame.mask().make_inverted());
  }
};

class internal_memory_size final : public virtual function_plugin,
                                   public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "internal_memory_size";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<InternalMemorySizeArgs,
                                     InternalMemorySizeFunction>{};
    d.positional("x", &InternalMemorySizeArgs::x, "any");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        TENZIR_UNUSED(ctx);
        auto input = eval(expr);
        auto sizes = std::vector<int64_t>{};
        sizes.resize(input.length());
        auto buffer = std::vector<int64_t>{};
        auto offset = int64_t{0};
        for (auto const& part : input) {
          auto part_sizes = std::span{sizes}.subspan(
            static_cast<size_t>(offset), static_cast<size_t>(part.length()));
          auto visitor = byte_size_visitor{part_sizes, std::move(buffer)};
          match(part, visitor);
          buffer = std::move(visitor.buffer_);
          offset += part.length();
        }
        TENZIR_ASSERT_EQ(offset, input.length());
        auto builder = arrow::Int64Builder{arrow_memory_pool()};
        check(builder.Reserve(input.length()));
        check(builder.AppendValues(sizes));
        return series{int64_type{}, finish(builder)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::internal_memory_size

TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::internal_memory_size::internal_memory_size)
