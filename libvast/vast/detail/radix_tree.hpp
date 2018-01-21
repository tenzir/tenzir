/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

/*
C++ flavor of Adaptive Radix Tree
Derived from original C implementation with copyright/license:

https://github.com/armon/libart
Copyright (c) 2012, Armon Dadgar
All rights reserved.

Adapted by Matthias Vallentin (2015).

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the organization nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL ARMON DADGAR BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef VAST_DETAIL_RADIX_TREE_HPP
#define VAST_DETAIL_RADIX_TREE_HPP

#include <emmintrin.h>

#include <cstdint>
#include <cstdlib>

#include <array>
#include <algorithm>
#include <deque>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <utility>
#include <string>

namespace vast::detail {

/// A radix tree data structure that facilitates *O(k)* operations, including
/// finding elements that match a given prefix. Keys are byte-strings and
/// values are given by template parameter T. Elements are stored according to
/// bitwise lexicographic order of the keys. Using ASCII strings as keys will
/// give the correct ordering, but other types may need to be transformed if
/// it's important to perform operations on the tree that depend on it (e.g., if
/// using unsigned integers or IP addresses as keys, they should be provided in
/// "network" order).
/// @tparam T The element type of the radix tree.
template <typename T, std::size_t N = 10>
class radix_tree {
  struct node;

public:
  using key_type = std::string;
  using mapped_type = T;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = size_t;

  class iterator : public std::iterator<std::forward_iterator_tag, value_type> {
    friend class radix_tree;

  public:
    using reference = value_type&;
    using pointer = value_type*;

    iterator() = default;
    iterator(iterator&& other) = default;
    iterator(const iterator& other);
    iterator& operator=(iterator rhs);
    reference operator*() const;
    pointer operator->() const;
    bool operator==(const iterator& other) const;
    bool operator!=(const iterator& other) const;
    const iterator& operator++();
    iterator operator++(int);

    friend void swap(iterator& a, iterator& b) {
      using std::swap;
      swap(a.root, b.root);
      swap(a.node_ptr, b.node_ptr);
      swap(a.ready_to_iterate, b.ready_to_iterate);
      swap(a.visited, b.visited);
    }

  private:
    iterator(node* root, node* starting_point);

    void increment();
    void prepare();

    struct node_visit {
      node* n;
      uint16_t idx;
      node_visit(node* arg_n) : n(arg_n), idx(0) {
      }
      node_visit(node* arg_n, uint16_t arg_i) : n(arg_n), idx(arg_i) {
      }
    };

    node* root;
    node* node_ptr;
    bool ready_to_iterate;
    std::unique_ptr<std::deque<node_visit>> visited;
  };

  /**
   * Default construct an empty container.
   */
  explicit radix_tree() : num_entries(0), root(nullptr) {
  }

  /**
   * Destructor.
   */
  ~radix_tree() {
    recursive_clear(root);
  }

  /**
   * Copy construct.
   */
  radix_tree(const radix_tree& other) : num_entries(0), root(nullptr) {
    // Maybe this could probably be better optimized?
    for (const auto& p : other)
      insert(p);
  }

  /**
   * Move construct.
   */
  radix_tree(radix_tree&& other) : num_entries(0), root(nullptr) {
    swap(*this, other);
  }

  /**
   * List construct.
   */
  radix_tree(std::initializer_list<value_type> l)
    : num_entries(0), root(nullptr) {
    for (const auto& e : l)
      insert(e);
  }

  /**
   * Assignment operator.
   */
  radix_tree& operator=(radix_tree other) {
    swap(*this, other);
    return *this;
  }

  /**
   * @returns the number of entries in the container.
   */
  size_type size() const {
    return num_entries;
  }

  /**
   * @return true if the container has no entries.
   */
  bool empty() const {
    return num_entries == 0;
  }

  /**
   * Remove all entries from the container.
   */
  void clear() {
    recursive_clear(root);
    root = nullptr;
    num_entries = 0;
  }

  /**
   * Locate a key in the container.
   * @param key the key to locate.
   * @returns an iterator to the key-value pair entry if found, else an
   * iterator equal to end().
   */
  iterator find(const key_type& key) const {
    node* n = root;
    int depth = 0;
    while (n) {
      if (n->type == node::tag::leaf) {
        if (reinterpret_cast<leaf*>(n)->key() == key)
          return {root, n};
        return end();
      }
      if (n->partial_len) {
        auto prefix_len = prefix_shared(n, key, depth);
        if (prefix_len != std::min(N, static_cast<size_t>(n->partial_len)))
          // Prefix mismatch.
          return end();
        depth += n->partial_len;
      }
      auto child = find_child(n, key[depth]).first;
      if (child)
        n = *child;
      else
        n = nullptr;
      ++depth;
    }
    return end();
  }

  /**
   * @returns an iterator to the first entry in the container, or an iterator
   * equal to end() if the container is empty.
   */
  iterator begin() const {
    return {root, reinterpret_cast<node*>(minimum(root))};
  }

  /**
   * @returns an iterator that does not point to any entry in the container,
   * or can be thought of as "past-the-end".
   */
  iterator end() const {
    return {root, nullptr};
  }

  /**
   * Insert a key-value pair if there exists no conflicting key already
   * in the container.
   * @param kv the key-value pair to insert.
   * @returns a pair whose first element is an iterator to the newly inserted
   *          key-value pair or the old key-value pair if it existed at the
   *          same key.  The second element of the returned pair is true
   *          if no conflicting key existed.
   */
  std::pair<iterator, bool> insert(value_type kv) {
    auto rval = recursive_insert(root, &root, std::move(kv), 0);
    if (rval.second)
      ++num_entries;
    return rval;
  }

  /**
   * Remove an entry from the container if it exists.
   * @param key the key corresponding to the entry to remove.
   * @returns the number of entries removed (either 1 or 0).
   */
  size_type erase(const key_type& key) {
    auto l = recursive_erase(root, &root, key, 0);
    if (!l)
      return 0;
    --num_entries;
    delete l;
    return 1;
  }

  /**
   * Access a key-value pair via its key.
   * @param lhs the key of the key-value pair to access.
   * @return the key-value pair corresponding to the key which may be
   * newly-created if it did not yet exist in the container.
   */
  mapped_type& operator[](key_type lhs) {
    iterator it = find(lhs);
    if (it != end())
      return it->second;
    return insert(value_type(std::move(lhs), mapped_type{})).first->second;
  }

  /**
   * @returns all entries that have a key prefixed by the argument.
   */
  std::deque<iterator> prefixed_by(const key_type& prefix) const {
    node* n = root;
    int depth = 0;
    std::deque<iterator> rval;
    while (n) {
      if (n->type == node::tag::leaf) {
        if (prefix_matches(reinterpret_cast<leaf*>(n)->key(), prefix))
          rval.push_back({root, n});
        return rval;
      }
      if (static_cast<size_t>(depth) == prefix.size()) {
        auto l = minimum(n);
        if (prefix_matches(l->key(), prefix)) {
          recursive_add_leaves(n, rval);
          return rval;
        }
        return rval;
      }
      if (n->partial_len) {
        auto prefix_len = prefix_mismatch(n, prefix, depth);
        if (!prefix_len)
          return rval;
        if (depth + prefix_len == prefix.size()) {
          recursive_add_leaves(n, rval);
          return rval;
        }
        // There is a full match, go deeper.
        depth += n->partial_len;
      }
      if (auto child = find_child(n, prefix[depth]).first)
        n = *child;
      else
        n = nullptr;
      ++depth;
    }
    return rval;
  }

  /**
   * @returns all entries that have a key that are a prefix of the argument.
   */
  std::deque<iterator> prefix_of(const key_type& data) const {
    node* n = root;
    std::deque<iterator> rval;
    int depth = 0;
    while (n) {
      if (n->type == node::tag::leaf) {
        if (prefix_matches(data, reinterpret_cast<leaf*>(n)->key()))
          rval.push_back({root, n});
        return rval;
      }
      if (n->partial_len) {
        auto prefix_len = prefix_shared(n, data, depth);
        if (prefix_len != std::min(N, static_cast<size_t>(n->partial_len)))
          // Prefix mismatch.
          return rval;
        depth += n->partial_len;
      }
      auto leaf = add_prefix_leaf(n, rval);
      auto child = find_child(n, data[depth]).first;
      if (child)
        n = *child;
      else
        n = nullptr;
      if (n == leaf)
        break;
      ++depth;
    }
    return rval;
  }

  bool operator==(const radix_tree& rhs) const {
    if (num_entries != rhs.num_entries)
      return false;
    // Maybe this could probably be better optimized?
    for (const auto& p : rhs)
      if (find(p.first) == end())
        return false;
    return true;
  }

  bool operator!=(const radix_tree& rhs) const {
    return !operator==(rhs);
  }

  friend void swap(radix_tree& a, radix_tree& b) {
    using std::swap;
    swap(a.root, b.root);
    swap(a.num_entries, b.num_entries);
  }

private:
  /**
   * Included as part of all internal nodes.
   */
  struct node {
    enum class tag : uint8_t {
      leaf,
      node4,
      node16,
      node48,
      node256,
    };

    node(tag t) : type(t) {
    }

    node(tag t, const node& other)
      : type(t),
        num_children(other.num_children),
        partial_len(other.partial_len),
        partial(other.partial) {
    }

    tag type;
    uint8_t num_children = 0;
    uint32_t partial_len = 0;
    std::array<unsigned char, N> partial = {};
  };

  /**
    * A small internal node in the radix tree with only 4 children.
    */
  struct node4 {
    node n = {node::tag::node4};
    std::array<unsigned char, 4> keys = {};
    std::array<node*, 4> children = {};

    node4() = default;
    node4(const node& other) : n(node::tag::node4, other) {
    }

    void add_child(node** ref, unsigned char c, node* child);
    void rem_child(node** ref, node** child);
  };

  /**
    * An internal node in the radix tree with 16 children.
    */
  struct node16 {
    node n = {node::tag::node16};
    std::array<unsigned char, 16> keys = {};
    std::array<node*, 16> children = {};

    node16() = default;
    node16(const node& other) : n(node::tag::node16, other) {
    }

    void add_child(node** ref, unsigned char c, node* child);
    void rem_child(node** ref, node** child);
  };

  /**
    * An internal node in the radix tree with 48 children, but
    * a full 256 byte field.
    */
  struct node48 {
    node n = {node::tag::node48};
    std::array<unsigned char, 256> keys = {};
    std::array<node*, 48> children = {};

    node48() = default;
    node48(const node& other) : n(node::tag::node48, other) {
    }

    void add_child(node** ref, unsigned char c, node* child);
    void rem_child(node** ref, unsigned char c);
  };

  /**
   * A full, internal node in the radix tree with 256 children.
   */
  struct node256 {
    node n = {node::tag::node256};
    std::array<node*, 256> children = {};

    node256() = default;
    node256(const node& other) : n(node::tag::node256, other) {
    }

    void add_child(node** ref, unsigned char c, node* child);
    void rem_child(node** ref, unsigned char c);
  };

  /**
   * A leaf in the radix tree.  Contains the key and associated value.
   */
  struct leaf {
    typename node::tag type;
    value_type kv;

    leaf(value_type arg_kv) : type(node::tag::leaf), kv(std::move(arg_kv)) {
    }

    const key_type& key() const {
      return kv.first;
    }
  };

  static inline const unsigned char* as_key_data(const std::string& key) {
    return reinterpret_cast<const unsigned char*>(key.data());
  }

  static uint32_t longest_common_prefix(const std::string& k1,
                                        const std::string& k2, uint32_t depth) {
    // Null-terminator is part of key.
    auto n = std::min(k1.size() + 1, k2.size() + 1) - depth;
    uint32_t i;
    auto k1_data = as_key_data(k1);
    auto k2_data = as_key_data(k2);
    for (i = 0; i < n; ++i)
      if (k1_data[depth + i] != k2_data[depth + i])
        return i;
    return i;
  }

  static bool prefix_matches(const std::string& key,
                             const std::string& prefix) {
    if (key.size() < prefix.size())
      return false;
    return !key.compare(0, prefix.size(), prefix);
  }

  static leaf* minimum(node* n) {
    if (!n)
      return nullptr;
    switch (n->type) {
      case node::tag::leaf:
        return reinterpret_cast<leaf*>(n);
      case node::tag::node4:
        return minimum(reinterpret_cast<node4*>(n)->children[0]);
      case node::tag::node16:
        return minimum(reinterpret_cast<node16*>(n)->children[0]);
      case node::tag::node48: {
        auto p = reinterpret_cast<node48*>(n);
        int i = 0;
        while (!p->keys[i])
          ++i;
        i = p->keys[i] - 1;
        return minimum(p->children[i]);
      }
      case node::tag::node256: {
        auto p = reinterpret_cast<node256*>(n);
        int i = 0;
        while (!p->children[i])
          ++i;
        return minimum(p->children[i]);
      }
      default:
        abort();
    }
    return nullptr;
  }

  static std::pair<node**, uint16_t> find_child(node* n, unsigned char c) {
    switch (n->type) {
      case node::tag::node4: {
        auto p = reinterpret_cast<node4*>(n);
        for (int i = 0; i < n->num_children; ++i)
          if (p->keys[i] == c)
            return {&p->children[i], i};
      } break;
      case node::tag::node16: {
        auto p = reinterpret_cast<node16*>(n);
        // Compare the key to all 16 stored keys
        __m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c),
                                     _mm_loadu_si128((__m128i*)p->keys.data()));
        // Use a mask to ignore children that don't exist
        int mask = (1 << n->num_children) - 1;
        int bitfield = _mm_movemask_epi8(cmp) & mask;
        if (bitfield) {
          auto i = __builtin_ctz(bitfield);
          return {&p->children[i], i};
        }
      } break;
      case node::tag::node48: {
        auto p = reinterpret_cast<node48*>(n);
        int i = p->keys[c];
        if (i)
          return {&p->children[i - 1], i};
      } break;
      case node::tag::node256: {
        auto p = reinterpret_cast<node256*>(n);
        if (p->children[c])
          return {&p->children[c], c};
      } break;
      default:
        abort();
        break;
    }
    return {nullptr, 0};
  }

  // Returns number of prefix character shared between key and node.
  static size_t prefix_shared(node* n, const key_type& key, int depth) {
    auto key_data = as_key_data(key);
    // Null-terminator is part of the key.
    auto max_cmp = std::min(std::min(N, static_cast<size_t>(n->partial_len)),
                            key.size() + 1 - depth);
    size_t idx;
    for (idx = 0; idx < max_cmp; ++idx)
      if (n->partial[idx] != key_data[depth + idx])
        return idx;
    return idx;
  }

  // Returns calculated index at which a prefix mismatches.
  static size_t prefix_mismatch(node* n, const key_type& key, int depth) {
    auto key_data = as_key_data(key);
    // Null-terminator is part of the key.
    auto max_cmp = std::min(std::min(N, static_cast<size_t>(n->partial_len)),
                            key.size() + 1 - depth);
    size_t idx;
    for (idx = 0; idx < max_cmp; ++idx)
      if (n->partial[idx] != key_data[depth + idx])
        return idx;
    if (n->partial_len > N) {
      // Need to find a leaf to determine.
      const auto l = minimum(n);
      max_cmp = std::min(l->key().size() + 1, key.size() + 1) - depth;
      auto leaf_key_data = as_key_data(l->key());
      for (; idx < max_cmp; ++idx)
        if (leaf_key_data[idx + depth] != key_data[idx + depth])
          return idx;
    }
    return idx;
  }

  static void add_child(node* n, node** ref, unsigned char c, node* child) {
    switch (n->type) {
      case node::tag::node4:
        return reinterpret_cast<node4*>(n)->add_child(ref, c, child);
      case node::tag::node16:
        return reinterpret_cast<node16*>(n)->add_child(ref, c, child);
      case node::tag::node48:
        return reinterpret_cast<node48*>(n)->add_child(ref, c, child);
      case node::tag::node256:
        return reinterpret_cast<node256*>(n)->add_child(ref, c, child);
      default:
        abort();
    }
  }

  static void rem_child(node* n, node** ref, unsigned char c, node** child) {
    switch (n->type) {
      case node::tag::node4:
        return reinterpret_cast<node4*>(n)->rem_child(ref, child);
      case node::tag::node16:
        return reinterpret_cast<node16*>(n)->rem_child(ref, child);
      case node::tag::node48:
        return reinterpret_cast<node48*>(n)->rem_child(ref, c);
      case node::tag::node256:
        return reinterpret_cast<node256*>(n)->rem_child(ref, c);
      default:
        abort();
    }
  }

  std::pair<iterator, bool> recursive_insert(node* n, node** self,
                                             value_type kv, size_t depth) {
    if (!n) {
      *self = reinterpret_cast<node*>(new leaf(std::move(kv)));
      return {{root, *self}, true};
    }
    if (n->type == node::tag::leaf) {
      auto l = reinterpret_cast<leaf*>(n);
      if (l->key() == kv.first) {
        // Value exists, don't change it.
        return {{root, reinterpret_cast<node*>(n)}, false};
      }
      // New value, need a new internal node.
      auto nn = new node4;
      auto l2 = new leaf(std::move(kv));
      auto longest_prefix = longest_common_prefix(l->key(), l2->key(), depth);
      auto m = std::min(N, static_cast<size_t>(longest_prefix));
      nn->n.partial_len = longest_prefix;
      std::copy(l2->key().begin() + depth, l2->key().begin() + depth + m,
                nn->n.partial.begin());
      *self = reinterpret_cast<node*>(nn);
      nn->add_child(self, l->key()[depth + longest_prefix],
                    reinterpret_cast<node*>(l));
      nn->add_child(self, l2->key()[depth + longest_prefix],
                    reinterpret_cast<node*>(l2));
      return {{root, reinterpret_cast<node*>(l2)}, true};
    }
    if (n->partial_len) {
      auto prefix_diff = prefix_mismatch(n, kv.first, depth);
      if (prefix_diff >= n->partial_len)
        depth += n->partial_len;
      else {
        // Need to split the node.
        auto nn = new node4;
        *self = reinterpret_cast<node*>(nn);
        nn->n.partial_len = prefix_diff;
        std::copy(n->partial.begin(),
                  n->partial.begin() + std::min(N, prefix_diff),
                  nn->n.partial.begin());
        // Adjust prefix of the old node.
        if (n->partial_len <= N) {
          nn->add_child(self, n->partial[prefix_diff], n);
          n->partial_len -= prefix_diff + 1;
          auto m = std::min(N, static_cast<size_t>(n->partial_len));
          std::copy(n->partial.begin() + prefix_diff + 1,
                    n->partial.begin() + prefix_diff + 1 + m,
                    n->partial.begin());
        } else {
          n->partial_len -= prefix_diff + 1;
          leaf* l = minimum(n);
          nn->add_child(self, l->key()[depth + prefix_diff], n);
          auto m = std::min(N, static_cast<size_t>(n->partial_len));
          std::copy(l->key().begin() + depth + prefix_diff + 1,
                    l->key().begin() + depth + prefix_diff + 1 + m,
                    n->partial.begin());
        }
        unsigned char c = kv.first[depth + prefix_diff];
        auto nl = reinterpret_cast<node*>(new leaf(std::move(kv)));
        nn->add_child(self, c, nl);
        return {{root, nl}, true};
      }
    }
    node** child = find_child(n, kv.first[depth]).first;
    if (child)
      return recursive_insert(*child, child, std::move(kv), depth + 1);
    unsigned char c = kv.first[depth];
    auto nl = reinterpret_cast<node*>(new leaf(std::move(kv)));
    add_child(n, self, c, nl);
    return {{root, nl}, true};
  }

  static leaf* recursive_erase(node* n, node** self, const key_type& key,
                               size_t depth) {
    if (!n)
      return nullptr;
    if (n->type == node::tag::leaf) {
      auto l = reinterpret_cast<leaf*>(n);
      if (key != l->key())
        return nullptr;
      *self = nullptr;
      return l;
    }
    if (n->partial_len) {
      auto prefix_len = prefix_shared(n, key, depth);
      if (prefix_len != std::min(N, static_cast<size_t>(n->partial_len)))
        // Prefix mismatch.
        return nullptr;
      depth += n->partial_len;
    }
    auto child = find_child(n, key[depth]).first;
    if (!child)
      return nullptr;
    if ((*child)->type != node::tag::leaf)
      return recursive_erase(*child, child, key, depth + 1);
    auto l = reinterpret_cast<leaf*>(*child);
    if (key != l->key())
      return nullptr;
    rem_child(n, self, key[depth], child);
    return l;
  }

  static void recursive_clear(node* n) {
    if (!n)
      return;
    switch (n->type) {
      case node::tag::leaf:
        delete reinterpret_cast<leaf*>(n);
        return;
      case node::tag::node4: {
        auto p = reinterpret_cast<node4*>(n);
        for (int i = 0; i < n->num_children; ++i)
          recursive_clear(p->children[i]);
        delete p;
      }
        return;
      case node::tag::node16: {
        auto p = reinterpret_cast<node16*>(n);
        for (int i = 0; i < n->num_children; ++i)
          recursive_clear(p->children[i]);
        delete p;
      }
        return;
      case node::tag::node48: {
        auto p = reinterpret_cast<node48*>(n);
        for (int i = 0; i < n->num_children; ++i)
          recursive_clear(p->children[i]);
        delete p;
      }
        return;
      case node::tag::node256: {
        auto p = reinterpret_cast<node256*>(n);
        for (int i = 0; i < 256; ++i)
          if (p->children[i])
            recursive_clear(p->children[i]);
        delete p;
      }
        return;
      default:
        abort();
    }
  }

  void recursive_add_leaves(node* n, std::deque<iterator>& leaves) const {
    switch (n->type) {
      case node::tag::leaf:
        leaves.push_back({root, n});
        break;
      case node::tag::node4: {
        auto p = reinterpret_cast<node4*>(n);
        for (int i = 0; i < n->num_children; ++i)
          recursive_add_leaves(p->children[i], leaves);
      } break;
      case node::tag::node16: {
        auto p = reinterpret_cast<node16*>(n);
        for (int i = 0; i < n->num_children; ++i)
          recursive_add_leaves(p->children[i], leaves);
      } break;
      case node::tag::node48: {
        auto p = reinterpret_cast<node48*>(n);
        for (int i = 0; i < 256; ++i) {
          auto idx = p->keys[i];
          if (!idx)
            continue;
          recursive_add_leaves(p->children[idx - 1], leaves);
        }
      } break;
      case node::tag::node256: {
        auto p = reinterpret_cast<node256*>(n);
        for (int i = 0; i < 256; ++i)
          if (p->children[i])
            recursive_add_leaves(p->children[i], leaves);
      } break;
      default:
        abort();
    }
  }

  node* add_prefix_leaf(node* n, std::deque<iterator>& leaves) const {
    switch (n->type) {
      case node::tag::leaf:
        return nullptr;
      case node::tag::node4: {
        auto p = reinterpret_cast<node4*>(n);
        if (n->num_children && p->keys[0] == 0
            && p->children[0]->type == node::tag::leaf) {
          leaves.push_back({root, p->children[0]});
          return p->children[0];
        }
      } break;
      case node::tag::node16: {
        auto p = reinterpret_cast<node16*>(n);
        if (n->num_children && p->keys[0] == 0
            && p->children[0]->type == node::tag::leaf) {
          leaves.push_back({root, p->children[0]});
          return p->children[0];
        }
      } break;
      case node::tag::node48: {
        auto p = reinterpret_cast<node48*>(n);
        if (p->keys[0]
            && p->children[p->keys[0] - 1]->type == node::tag::leaf) {
          leaves.push_back({root, p->children[p->keys[0] - 1]});
          return p->children[p->keys[0] - 1];
        }
      } break;
      case node::tag::node256: {
        auto p = reinterpret_cast<node256*>(n);
        if (p->children[0] && p->children[0]->type == node::tag::leaf) {
          leaves.push_back({root, p->children[0]});
          return p->children[0];
        }
      } break;
      default:
        abort();
    }
    return nullptr;
  }

  size_type num_entries;
  node* root;
};

template <typename T, std::size_t N>
void radix_tree<T, N>::node4::add_child(node** ref, unsigned char c,
                                        node* child) {
  if (n.num_children < 4) {
    int idx;
    for (idx = 0; idx < n.num_children; ++idx)
      if (c < keys[idx])
        break;
    // Shift right.
    std::copy_backward(keys.begin() + idx, keys.begin() + n.num_children,
                       keys.begin() + n.num_children + 1);
    std::copy_backward(children.begin() + idx,
                       children.begin() + n.num_children,
                       children.begin() + n.num_children + 1);
    keys[idx] = c;
    children[idx] = child;
    ++n.num_children;
    return;
  }
  auto nn = new node16(n);
  std::copy(children.begin(), children.begin() + n.num_children,
            nn->children.begin());
  std::copy(keys.begin(), keys.begin() + n.num_children, nn->keys.begin());
  *ref = reinterpret_cast<node*>(nn);
  delete this;
  nn->add_child(ref, c, child);
}

template <typename T, std::size_t N>
void radix_tree<T, N>::node16::add_child(node** ref, unsigned char c,
                                         node* child) {
  if (n.num_children < 16) {
    // Compare the key to all 16 stored keys
    __m128i cmp = _mm_cmplt_epi8(_mm_set1_epi8(c),
                                 _mm_loadu_si128((__m128i*)keys.data()));
    // Use a mask to ignore children that don't exist
    unsigned mask = (1 << n.num_children) - 1;
    unsigned bitfield = _mm_movemask_epi8(cmp) & mask;
    // Check if less than any
    unsigned idx = n.num_children;
    if (bitfield) {
      idx = __builtin_ctz(bitfield);
      // Shift right.
      std::copy_backward(keys.begin() + idx, keys.begin() + n.num_children,
                         keys.begin() + n.num_children + 1);
      std::copy_backward(children.begin() + idx,
                         children.begin() + n.num_children,
                         children.begin() + n.num_children + 1);
    }
    keys[idx] = c;
    children[idx] = child;
    ++n.num_children;
    return;
  }

  auto nn = new node48(n);
  std::copy(children.begin(), children.begin() + n.num_children,
            nn->children.begin());
  for (int i = 0; i < n.num_children; ++i)
    nn->keys[keys[i]] = i + 1;
  *ref = reinterpret_cast<node*>(nn);
  delete this;
  nn->add_child(ref, c, child);
}

template <typename T, std::size_t N>
void radix_tree<T, N>::node48::add_child(node** ref, unsigned char c,
                                         node* child) {
  if (n.num_children < 48) {
    int pos = 0;
    while (children[pos])
      pos++;
    children[pos] = child;
    keys[c] = pos + 1;
    ++n.num_children;
    return;
  }
  auto nn = new node256(n);
  for (int i = 0; i < 256; ++i)
    if (keys[i])
      nn->children[i] = children[keys[i] - 1];
  *ref = reinterpret_cast<node*>(nn);
  delete this;
  nn->add_child(ref, c, child);
}

template <typename T, std::size_t N>
void radix_tree<T, N>::node256::add_child(node**, unsigned char c,
                                          node* child) {
  ++n.num_children;
  children[c] = child;
}

template <typename T, std::size_t N>
void radix_tree<T, N>::node4::rem_child(node** ref, node** child) {
  int pos = child - children.data();
  std::copy(keys.begin() + pos + 1, keys.begin() + n.num_children,
            keys.begin() + pos);
  std::copy(children.begin() + pos + 1, children.begin() + n.num_children,
            children.begin() + pos);
  --n.num_children;
  if (n.num_children != 1)
    return;
  node* last = children[0];
  if (last->type != node::tag::leaf) {
    // Concatenate prefixes.
    size_t prefix = n.partial_len;
    if (prefix < N) {
      n.partial[prefix] = keys[0];
      ++prefix;
    }
    if (prefix < N) {
      int sub_prefix
        = std::min(static_cast<size_t>(last->partial_len), N - prefix);
      std::copy(last->partial.begin(), last->partial.begin() + sub_prefix,
                n.partial.begin() + prefix);
      prefix += sub_prefix;
    }
    // Store prefix in child.
    std::copy(n.partial.begin(), n.partial.begin() + std::min(prefix, N),
              last->partial.begin());
    last->partial_len += n.partial_len + 1;
  }
  *ref = last;
  delete this;
}

template <typename T, std::size_t N>
void radix_tree<T, N>::node16::rem_child(node** ref, node** child) {
  int pos = child - children.data();
  std::copy(keys.begin() + pos + 1, keys.begin() + n.num_children,
            keys.begin() + pos);
  std::copy(children.begin() + pos + 1, children.begin() + n.num_children,
            children.begin() + pos);
  --n.num_children;
  if (n.num_children != 3)
    return;
  auto nn = new node4(n);
  *ref = reinterpret_cast<node*>(nn);
  std::copy(keys.begin(), keys.begin() + 4, nn->keys.begin());
  std::copy(children.begin(), children.begin() + 4, nn->children.begin());
  delete this;
}

template <typename T, std::size_t N>
void radix_tree<T, N>::node48::rem_child(node** ref, unsigned char c) {
  int pos = keys[c];
  keys[c] = 0;
  children[pos - 1] = nullptr;
  --n.num_children;
  if (n.num_children != 12)
    return;
  auto nn = new node16(n);
  *ref = reinterpret_cast<node*>(nn);
  int child = 0;
  for (int i = 0; i < 256; ++i) {
    pos = keys[i];
    if (pos) {
      nn->keys[child] = i;
      nn->children[child] = children[pos - 1];
      ++child;
    }
  }
  delete this;
}

template <typename T, std::size_t N>
void radix_tree<T, N>::node256::rem_child(node** ref, unsigned char c) {
  children[c] = nullptr;
  --n.num_children;
  // Don't immediately resize to prevent thrashing.
  if (n.num_children != 37)
    return;
  auto nn = new node48(n);
  *ref = reinterpret_cast<node*>(nn);
  int pos = 0;
  for (int i = 0; i < 256; ++i)
    if (children[i]) {
      nn->children[pos] = children[i];
      nn->keys[i] = pos + 1;
      ++pos;
    }
  delete this;
}

template <typename T, std::size_t N>
radix_tree<T, N>::iterator::iterator(node* arg_root, node* starting_point)
  : root(arg_root), node_ptr(starting_point), ready_to_iterate(false) {
}

template <typename T, std::size_t N>
typename radix_tree<T, N>::iterator::reference radix_tree<T, N>::iterator::
operator*() const {
  return reinterpret_cast<leaf*>(node_ptr)->kv;
}

template <typename T, std::size_t N>
typename radix_tree<T, N>::iterator::pointer radix_tree<T, N>::iterator::
operator->() const {
  return &reinterpret_cast<leaf*>(node_ptr)->kv;
}

template <typename T, std::size_t N>
bool radix_tree<T, N>::iterator::operator==(const iterator& other) const {
  return node_ptr == other.node_ptr;
}

template <typename T, std::size_t N>
bool radix_tree<T, N>::iterator::operator!=(const iterator& other) const {
  return node_ptr != other.node_ptr;
}

template <typename T, std::size_t N>
const typename radix_tree<T, N>::iterator& radix_tree<T, N>::iterator::
operator++() {
  prepare();
  increment();
  return *this;
}

template <typename T, std::size_t N>
typename radix_tree<T, N>::iterator radix_tree<T, N>::iterator::
operator++(int) {
  prepare();
  auto rval = *this;
  increment();
  return rval;
}

template <typename T, std::size_t N>
void radix_tree<T, N>::iterator::increment() {
  while (!visited->empty()) {
    uint16_t& next_idx = visited->front().idx;
    node* n = visited->front().n;
    switch (n->type) {
      case node::tag::leaf:
        node_ptr = n;
        visited->pop_front();
        return;
      case node::tag::node4: {
        auto p = reinterpret_cast<node4*>(n);
        if (next_idx >= n->num_children)
          visited->pop_front();
        else {
          visited->emplace_front(p->children[next_idx]);
          ++next_idx;
        }
      } break;
      case node::tag::node16: {
        auto p = reinterpret_cast<node16*>(n);
        if (next_idx >= n->num_children)
          visited->pop_front();
        else {
          visited->emplace_front(p->children[next_idx]);
          ++next_idx;
        }
      } break;
      case node::tag::node48: {
        auto p = reinterpret_cast<node48*>(n);
        auto exhausted = true;
        for (; next_idx < 256; ++next_idx) {
          auto idx = p->keys[next_idx];
          if (!idx)
            continue;
          visited->emplace_front(p->children[idx - 1]);
          ++next_idx;
          exhausted = false;
          break;
        }
        if (exhausted)
          visited->pop_front();
      } break;
      case node::tag::node256: {
        auto p = reinterpret_cast<node256*>(n);
        auto exhausted = true;
        for (; next_idx < 256; ++next_idx) {
          if (!p->children[next_idx])
            continue;
          visited->emplace_front(p->children[next_idx]);
          ++next_idx;
          exhausted = false;
          break;
        }
        if (exhausted)
          visited->pop_front();
      } break;
      default:
        abort();
    }
  }
  node_ptr = nullptr;
}

template <typename T, std::size_t N>
void radix_tree<T, N>::iterator::prepare() {
  if (ready_to_iterate)
    return;
  ready_to_iterate = true;
  visited.reset(new std::deque<node_visit>);
  // Find path from root to node_ptr (pretty much same as find(), except build
  // the path within 'visited' and don't need all conditionals).
  node* n = root;
  int depth = 0;
  const key_type& key = reinterpret_cast<leaf*>(node_ptr)->key();
  while (n) {
    if (n->type == node::tag::leaf)
      return;
    depth += n->partial_len;
    auto child = find_child(n, key[depth]);
    if (child.first) {
      visited->emplace_front(n, child.second + 1);
      n = *child.first;
    } else
      n = nullptr;
    ++depth;
  }
}

template <typename T, std::size_t N>
radix_tree<T, N>::iterator::iterator(const iterator& other)
  : root(other.root),
    node_ptr(other.node_ptr),
    ready_to_iterate(other.ready_to_iterate) {
  if (other.visited)
    visited.reset(new std::deque<node_visit>{*other.visited});
}

template <typename T, std::size_t N>
void swap(typename radix_tree<T, N>::iterator& a,
          typename radix_tree<T, N>::iterator& b) {
  using std::swap;
  swap(a.root, b.root);
  swap(a.node_ptr, b.node_ptr);
  swap(a.ready_to_iterate, b.ready_to_iterate);
  swap(a.visited, b.visited);
}

template <typename T, std::size_t N>
typename radix_tree<T, N>::iterator& radix_tree<T, N>::iterator::
operator=(iterator rhs) {
  swap(*this, rhs);
  return *this;
}

} // namespace vast::detail

#endif
