#ifndef VAST_CONCEPT_HASHABLE_UHASH_HPP
#define VAST_CONCEPT_HASHABLE_UHASH_HPP

#include "vast/concept/hashable/hash_append.hpp"

namespace vast {

/// The universal hash function.
template <class Hasher>
class uhash {
public:
  using result_type = typename Hasher::result_type;

  template <class... Ts>
  uhash(Ts&&... xs) : h_(std::forward<Ts>(xs)...) {
  }

  template <class T>
  result_type operator()(T const& x) noexcept {
    hash_append(h_, x);
    return static_cast<result_type>(h_);
  }

private:
  Hasher h_;
};

} // namespace vast

#endif
