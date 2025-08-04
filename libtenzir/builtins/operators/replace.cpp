//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/type_fwd.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

namespace tenzir::plugins::replace {

namespace {

auto comparable(const type& x, const type& y) -> bool {
  return match(std::tie(x, y), []<typename X, typename Y>(const X&, const Y&) {
    return std::same_as<X, Y> or concepts::one_of<null_type, X, Y>
           or (concepts::number<type_to_data_t<X>>
               and concepts::number<type_to_data_t<Y>>);
  });
}

auto equals(const data_view& l, const data& r) -> bool {
  return match(
    std::tie(l, r),
    [](const concepts::integer auto& x, const concepts::integer auto& y) {
      return std::cmp_equal(x, y);
    },
    [](const concepts::number auto& x, const concepts::number auto& y) {
      return x == y;
    },
    [&](const auto&, const auto&) {
      return l == r;
    });
}

auto check_name(const std::vector<ast::field_path>& paths,
                const std::string_view name, const size_t idx) -> bool {
  return std::ranges::any_of(paths, [&](auto& path) {
    return name == path.path()[idx].id.name;
  });
}

auto update_paths(const std::vector<ast::field_path>& paths,
                  const std::string_view name, const size_t idx)
  -> std::vector<ast::field_path> {
  return std::views::filter(paths,
                            [&](auto& path) {
                              return name == path.path()[idx].id.name;
                            })
         | std::ranges::to<std::vector<ast::field_path>>();
}

auto find_splits(const series& in, std::set<int64_t>& indices,
                 const std::vector<ast::field_path>& paths, const size_t idx,
                 const type& what_type, const data& what) -> void {
  if (comparable(in.type, what_type)) {
    auto replace = false;
    for (const auto& [curr, val] :
         detail::enumerate<int64_t>(values(in.type, *in.array))) {
      if (equals(val, what) != replace) {
        indices.insert(curr);
        replace = not replace;
      }
    }
  }
  const auto replace_all
    = paths.empty() or std::ranges::any_of(paths, [&](auto& path) {
        return idx >= path.path().size();
      });
  if (const auto rin = in.as<record_type>()) {
    for (const auto& field : rin->fields()) {
      if (replace_all or check_name(paths, field.name, idx)) {
        const auto& new_paths
          = replace_all ? paths : update_paths(paths, field.name, idx);
        find_splits(field.data, indices, new_paths, idx + 1, what_type, what);
      }
    }
  }
}

auto replace_split_series(series in, const std::vector<ast::field_path>& paths,
                          const std::size_t idx, const type& what_type,
                          const data& what, const data& with) -> series {
  if (comparable(in.type, what_type)) {
    const auto val = value_at(in.type, *in.array, 0);
    if (equals(val, what)) {
      return data_to_series(with, in.length());
    }
  }
  const auto replace_all
    = paths.empty() or std::ranges::any_of(paths, [&](auto& path) {
        return idx >= path.path().size();
      });
  if (const auto rin = in.as<record_type>()) {
    auto fields = collect(rin->fields());
    for (auto& field : fields) {
      if (replace_all) {
        field.data = replace_split_series(field.data, paths, idx + 1, what_type,
                                          what, with);
        continue;
      }
      if (check_name(paths, field.name, idx)) {
        field.data = replace_split_series(field.data,
                                          update_paths(paths, field.name, idx),
                                          idx + 1, what_type, what, with);
      }
    }
    return make_record_series(fields, *rin->array);
  }
  return in;
}

auto replace_series(const basic_series<record_type>& input,
                    const std::vector<ast::field_path>& paths,
                    const type& what_type, const data& what, const data& with)
  -> std::vector<basic_series<record_type>> {
  auto fields = collect(input.fields());
  if (fields.empty()) {
    return {input};
  }
  auto split_indices = std::set<int64_t>{0, input.length()};
  const auto replace_all = paths.empty();
  for (const auto& field : fields) {
    if (replace_all or check_name(paths, field.name, 0)) {
      const auto new_paths = update_paths(paths, field.name, 0);
      find_splits(field.data, split_indices, new_paths, 1, what_type, what);
    }
  }
  auto splits
    = std::vector<std::vector<series_field>>{split_indices.size() - 1};
  for (auto&& field : fields) {
    auto it = split_indices.begin();
    for (auto i = size_t{}; i != splits.size(); ++i) {
      const auto begin = *it;
      const auto end = *(++it);
      auto split = [&] {
        if (replace_all) {
          return replace_split_series(field.data.slice(begin, end), paths, 1,
                                      what_type, what, with);
        }
        if (check_name(paths, field.name, 0)) {
          const auto new_paths = update_paths(paths, field.name, 0);
          return replace_split_series(field.data.slice(begin, end), new_paths,
                                      1, what_type, what, with);
        }
        return field.data.slice(begin, end);
      }();
      splits[i].emplace_back(field.name, std::move(split));
    }
  }
  auto replaced = std::vector<basic_series<record_type>>{};
  auto offset = int64_t{};
  for (auto&& split : splits) {
    auto input_slice = std::static_pointer_cast<arrow::StructArray>(
      input.array->Slice(offset, split[0].data.length()));
    replaced.emplace_back(make_record_series(split, *input_slice));
    offset += split[0].data.length();
  }
  return replaced;
}

auto replace_series_with_null(const basic_series<record_type>& input,
                              const std::vector<ast::field_path>& paths,
                              const size_t idx, const type& what_type,
                              const data& what) -> basic_series<record_type> {
  auto fields = collect(input.fields());
  const auto replace_all
    = paths.empty() or std::ranges::any_of(paths, [&](auto& path) {
        return idx >= path.path().size();
      });
  for (auto&& field : fields) {
    if (not replace_all and not check_name(paths, field.name, idx)) {
      continue;
    }
    if (not comparable(field.data.type, what_type)) {
      if (const auto rin = field.data.as<record_type>()) {
        const auto& new_paths
          = replace_all ? paths : update_paths(paths, field.name, idx);
        auto replaced
          = replace_series_with_null(*rin, new_paths, idx + 1, what_type, what);
        field.data.array = std::move(replaced.array);
      }
      continue;
    }
    auto& array = field.data.array;
    const auto& array_data = array->data();
    const auto offset = array_data->offset;
    const auto total_count = array_data->length + offset;
    auto null_bitmap = array->null_bitmap();
    if (not null_bitmap) {
      null_bitmap = check(arrow::AllocateEmptyBitmap(total_count));
    }
    TENZIR_ASSERT(null_bitmap->size()
                  >= arrow::bit_util::BytesForBits(total_count));
    auto* null_bitmap_data = null_bitmap->mutable_data();
    TENZIR_ASSERT(null_bitmap_data);
    auto null_count = int64_t{};
    for (const auto& [i, val] :
         detail::enumerate<int64_t>(field.data.values())) {
      if (is<caf::none_t>(val) or equals(val, what)) {
        arrow::bit_util::SetBitTo(null_bitmap_data, i + offset, false);
        ++null_count;
      }
    }
    auto buffers = array_data->buffers;
    if (buffers.empty()) {
      buffers.emplace_back(null_bitmap);
    } else {
      buffers.front() = null_bitmap;
    }
    auto new_array_data
      = arrow::ArrayData::Make(array_data->type, array_data->length, buffers,
                               null_count, array_data->offset);
    array = arrow::MakeArray(new_array_data);
  }
  return make_record_series(fields, *input.array);
}

struct replace_args {
  std::vector<ast::field_path> path;
  located<data> what;
  located<data> with;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    const auto what_type = type::infer(what.inner);
    const auto with_type = type::infer(with.inner);
    if (not what_type) {
      diagnostic::error("failed to infer type of `what`").primary(what).emit(dh);
      return failure::promise();
    }
    if (not with_type) {
      diagnostic::error("failed to infer type of `with`").primary(with).emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, replace_args& x) -> bool {
    return f.object(x).fields(f.field("path", x.path), f.field("what", x.what),
                              f.field("with", x.with));
  }
};

class replace_operator final : public crtp_operator<replace_operator> {
public:
  replace_operator() = default;

  replace_operator(replace_args args) : args_{std::move(args)} {
  }

  auto operator()(generator<table_slice> input, operator_control_plane&) const
    -> generator<table_slice> {
    const auto what_type = type::infer(args_.what.inner).value();
    const auto with_type = type::infer(args_.with.inner).value();
    const auto replace_with_null = with_type.kind().is<null_type>();
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto s = basic_series<record_type>{slice};
      if (replace_with_null) {
        auto rs = replace_series_with_null(s, args_.path, 0, what_type,
                                           args_.what.inner);
        co_yield table_slice{
          arrow::RecordBatch::Make(slice.schema().to_arrow_schema(),
                                   rs.length(), rs.array->fields()),
          slice.schema(),
        };
      } else {
        auto rs = replace_series(s, args_.path, what_type, args_.what.inner,
                                 args_.with.inner);
        auto attrs = collect(slice.schema().attributes());
        for (auto& r : rs) {
          auto rty = type{slice.schema().name(), r.type, auto{attrs}};
          co_yield table_slice{
            arrow::RecordBatch::Make(rty.to_arrow_schema(), r.array->length(),
                                     r.array->fields()),
            std::move(rty),
          };
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "tql2.replace";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  };

  friend auto inspect(auto& f, replace_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  replace_args args_;
};

struct replace : public virtual operator_plugin2<replace_operator> {
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = replace_args{};
    auto p = argument_parser2::operator_(name());
    p.named("what", args.what);
    p.named("with", args.with);
    auto partition
      = std::partition(inv.args.begin(), inv.args.end(), [](auto&& x) {
          return not ast::field_path::try_from(x).has_value();
        });
    std::ranges::transform(partition, inv.args.end(),
                           std::back_inserter(args.path), [](auto& x) {
                             return ast::field_path::try_from(x).value();
                           });
    inv.args.erase(partition, inv.args.end());
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<replace_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::replace

TENZIR_REGISTER_PLUGIN(tenzir::plugins::replace::replace)
