#include "tenzir/nova/fundamental_array.hpp"

namespace tenzir::nova {

template <fundamental_type Tag>
auto Array<Tag>::as_unique() const& -> Array {
  return match(storage_, [](auto const& storage) -> Array {
    return Array{storage.as_unique()};
  });
}

template <fundamental_type Tag>
auto Array<Tag>::as_unique() && -> Array {
  return match(storage_, [](auto& storage) -> Array {
    return Array{std::move(storage).as_unique()};
  });
}

template <fundamental_type Tag>
auto Array<Tag>::length() const noexcept -> storage::Index {
  return match(storage_, [](const auto& storage) {
    return storage.length();
  });
}

template <fundamental_type Tag>
auto Array<Tag>::get(storage::Index i) const -> RowView<Tag> {
  return match(storage_, [i](const auto& storage) {
    return RowView<Tag>{static_cast<ViewType>(storage.get(i))};
  });
}

template <fundamental_type Tag>
auto Array<Tag>::storage() const& -> const storage_t& {
  return storage_;
}

template <fundamental_type Tag>
auto Array<Tag>::storage() && -> storage_t&& {
  return std::move(storage_);
}

template class Array<Null>;
template class Array<Bool>;
template class Array<Int>;
template class Array<UInt>;
template class Array<Float>;
template class Array<String>;
template class Array<Blob>;
template class Array<Secret>;
template class Array<Ip>;
template class Array<Subnet>;
template class Array<Time>;
template class Array<Duration>;

} // namespace tenzir::nova
