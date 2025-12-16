//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/subnet_tree.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/scope_guard.hpp"

#include <span>

namespace {

// A PATRICIA trie or *tree* (as Knuth calls it) is trie with radix of two.
// Nodes exist to identify the bits that distinguish the keys. Each node has
// exactly two children, like a binary tree, and therefore the number of nodes
// is the number of keys.

/*
 * $Id: patricia.c,v 1.7 2005/12/07 20:46:41 dplonka Exp $
 * Dave Plonka <plonka@doit.wisc.edu>
 *
 * This product includes software developed by the University of Michigan,
 * Merit Network, Inc., and their contributors.
 *
 * This file had been called "radix.c" in the MRT sources.
 *
 * I renamed it to "patricia.c" since it's not an implementation of a general
 * radix trie.  Also I pulled in various requirements from "prefix.c" and
 * "demo.c" so that it could be used as a standalone API.
 */

/*
 * This code originates from Dave Plonka's Net::Security perl module. An
 * adaptation of it in C is kept at
 * https://github.com/CAIDA/cc-common/tree/master/libpatricia. That repository
 * is considered the upstream version for Zeek's fork. We make some custom
 * changes to this upstream:
 * - Replaces void_fn_t with data_fn_t and prefix_data_fn_t
 * - Adds patricia_search_all method
 * - One commented-out portion of an if statement that breaks one of our tests
 *
 * The current version is based on commit
 * fd262ab5ac5bae8b0d4a8b5e2e723115b1846376 from that repo.
 */

/*
 * Johanna Amann <johanna@icir.org>
 *
 * Added patricia_search_all function.
 */

/*
 * Matthias Vallentin <matthias@tenzir.com>
 *
 * - Reformatted to fit the Tenzir code base.
 * - Merged header and implementation.
 * - Removed compile-time IPv6 toggle.
 */

#include <arpa/inet.h>  /* BSD, Linux, Solaris: for inet_addr */
#include <netinet/in.h> /* BSD, Linux: for inet_addr */
#include <sys/socket.h> /* BSD, Linux: for inet_addr */
#include <sys/types.h>  /* BSD: for inet_addr */

#include <cassert> /* assert */
#include <cctype>  /* isdigit */
#include <cerrno>  /* errno */
#include <cmath>   /* sin */
#include <cstdio>  /* snprintf, fprintf, stderr */
#include <cstdlib> /* free, atol, calloc */
#include <cstring> /* memcpy, strchr, strlen */

#define Delete free

static void out_of_memory(const char* where) {
  fprintf(stderr, "out of memory in %s.\n", where);
  abort();
}

// From Zeek for reporting memory exhaustion.
extern void out_of_memory(const char* where);

/* typedef unsigned int u_int; */
using void_fn_t = void (*)();
/* { from defs.h */
#define prefix_touchar(prefix) ((u_char*)&(prefix)->add.sin)
#define MAXLINE 1024
#define BIT_TEST(f, b) ((f) & (b))
/* } */

#include <sys/types.h> /* for u_* definitions (on FreeBSD 5) */

#include <cerrno> /* for EAFNOSUPPORT */
#ifndef EAFNOSUPPORT
#  defined EAFNOSUPPORT WSAEAFNOSUPPORT
#  include <winsock.h>
#else
#  include <netinet/in.h> /* for struct in_addr */
#endif

#include <sys/socket.h> /* for AF_INET */

/* { from mrt.h */

using prefix4_t = struct _prefix4_t {
  u_short family; /* AF_INET | AF_INET6 */
  u_short bitlen; /* same as mask? */
  int ref_count;  /* reference count */
  struct in_addr sin;
};

using prefix_t = struct _prefix_t {
  u_short family; /* AF_INET | AF_INET6 */
  u_short bitlen; /* same as mask? */
  int ref_count;  /* reference count */
  union {
    struct in_addr sin;
    struct in6_addr sin6;
  } add;
};

using data_fn_t = void (*)(void*);
using prefix_data_fn_t = void (*)(prefix_t*, void*);

/* } */

using patricia_node_t = struct _patricia_node_t {
  u_int bit;                       /* flag if this node used */
  prefix_t* prefix;                /* who we are in patricia tree */
  struct _patricia_node_t *l, *r;  /* left and right children */
  struct _patricia_node_t* parent; /* may be used */
  void* data;                      /* pointer to data */
  void* user1; /* pointer to usr data (ex. route flap info) */
};

using patricia_tree_t = struct _patricia_tree_t {
  patricia_node_t* head;
  u_int maxbits;       /* for IP, 32 bit addresses */
  int num_active_node; /* for debug purpose */
};

auto patricia_search_exact(patricia_tree_t* patricia, prefix_t* prefix)
  -> patricia_node_t*;
auto patricia_search_all(patricia_tree_t* patricia, prefix_t* prefix,
                         patricia_node_t*** list, int* n) -> int;
auto patricia_search_best(patricia_tree_t* patricia, prefix_t* prefix)
  -> patricia_node_t*;
auto patricia_search_best2(patricia_tree_t* patricia, prefix_t* prefix,
                           int inclusive) -> patricia_node_t*;
auto patricia_lookup(patricia_tree_t* patricia, prefix_t* prefix)
  -> patricia_node_t*;
auto patricia_remove(patricia_tree_t* patricia, patricia_node_t* node) -> void;
auto New_Patricia(int maxbits) -> patricia_tree_t*;
auto Clear_Patricia(patricia_tree_t* patricia, data_fn_t func) -> void;
auto Destroy_Patricia(patricia_tree_t* patricia, data_fn_t func) -> void;

auto Deref_Prefix(prefix_t* prefix) -> void;

#define PATRICIA_MAXBITS (sizeof(struct in6_addr) * 8)
#define PATRICIA_NBIT(x) (0x80 >> ((x) & 0x7f))
#define PATRICIA_NBYTE(x) ((x) >> 3)

#define PATRICIA_DATA_GET(node, type) (type*)((node)->data)
#define PATRICIA_DATA_SET(node, value) ((node)->data = (void*)(value))

#define PATRICIA_WALK(Xhead, Xnode)                                            \
  do {                                                                         \
    patricia_node_t* Xstack[PATRICIA_MAXBITS + 1];                             \
    patricia_node_t** Xsp = Xstack;                                            \
    patricia_node_t* Xrn = (Xhead);                                            \
    while (((Xnode) = Xrn)) {                                                  \
      if ((Xnode)->prefix)

#define PATRICIA_WALK_ALL(Xhead, Xnode)                                        \
  do {                                                                         \
    patricia_node_t* Xstack[PATRICIA_MAXBITS + 1];                             \
    patricia_node_t** Xsp = Xstack;                                            \
    patricia_node_t* Xrn = (Xhead);                                            \
    while (((Xnode) = Xrn)) {                                                  \
      if (1)

#define PATRICIA_WALK_BREAK                                                    \
  {                                                                            \
    if (Xsp != Xstack) {                                                       \
      Xrn = *(--Xsp);                                                          \
    } else {                                                                   \
      Xrn = (patricia_node_t*)0;                                               \
    }                                                                          \
    continue;                                                                  \
  }

#define PATRICIA_WALK_END                                                      \
  if (Xrn->l) {                                                                \
    if (Xrn->r) {                                                              \
      *Xsp++ = Xrn->r;                                                         \
    }                                                                          \
    Xrn = Xrn->l;                                                              \
  } else if (Xrn->r) {                                                         \
    Xrn = Xrn->r;                                                              \
  } else if (Xsp != Xstack) {                                                  \
    Xrn = *(--Xsp);                                                            \
  } else {                                                                     \
    Xrn = (patricia_node_t*)0;                                                 \
  }                                                                            \
  }                                                                            \
  }                                                                            \
  while (0)

/* { from prefix.c */

/* prefix_tochar
 * convert prefix information to bytes
 */
auto prefix_tochar(prefix_t* prefix) -> u_char* {
  if (prefix == nullptr) {
    return (nullptr);
  }
  return ((u_char*)&prefix->add.sin);
}

auto comp_with_mask(void* addr, void* dest, u_int mask) -> int {
  if (/* mask/8 == 0 || */ memcmp(addr, dest, mask / 8) == 0) {
    int n = mask / 8;
    int m = -(1 << (8 - (mask % 8)));
    if (mask % 8 == 0 || (((u_char*)addr)[n] & m) == (((u_char*)dest)[n] & m)) {
      return (1);
    }
  }
  return (0);
}

#define PATRICIA_MAX_THREADS 16
#define PATRICIA_PREFIX_BUF_LEN 53

auto New_Prefix2(int family, void* dest, int bitlen, prefix_t* prefix)
  -> prefix_t* {
  int dynamic_allocated = 0;
  int default_bitlen = sizeof(struct in_addr) * 8;
  prefix4_t* p4 = nullptr;
  if (prefix == nullptr) {
    prefix = static_cast<prefix_t*>(calloc(1, sizeof(prefix_t)));
    if (prefix == nullptr) {
      out_of_memory("patricia/new_prefix2: unable to allocate memory");
    }
    dynamic_allocated++;
  }
  if (family == AF_INET6) {
    default_bitlen = sizeof(struct in6_addr) * 8;
    memcpy(&prefix->add.sin6, dest, sizeof(struct in6_addr));
  } else if (family == AF_INET) {
    memcpy(&prefix->add.sin, dest, sizeof(struct in_addr));
  } else {
    return (nullptr);
  }
  p4 = (prefix4_t*)prefix;
  p4->bitlen = (bitlen >= 0) ? bitlen : default_bitlen;
  p4->family = family;
  p4->ref_count = 0;
  if (dynamic_allocated) {
    p4->ref_count++;
  }
  return (prefix);
}

auto Ref_Prefix(prefix_t* prefix) -> prefix_t* {
  if (prefix == nullptr) {
    return (nullptr);
  }
  if (prefix->ref_count == 0) {
    /* make a copy in case of a static prefix */
    return (New_Prefix2(prefix->family, &prefix->add, prefix->bitlen, nullptr));
  }
  prefix->ref_count++;
  return (prefix);
}

void Deref_Prefix(prefix_t* prefix) {
  if (prefix == nullptr) {
    return;
  }
  /* for secure programming, raise an assert. no static prefix can call this
   */
  assert(prefix->ref_count > 0);
  prefix->ref_count--;
  assert(prefix->ref_count >= 0);
  if (prefix->ref_count <= 0) {
    Delete(prefix);
    return;
  }
}

/* } */

static int num_active_patricia = 0;

/* these routines support continuous mask only */

auto New_Patricia(int maxbits) -> patricia_tree_t* {
  patricia_tree_t* patricia
    = static_cast<patricia_tree_t*>(calloc(1, sizeof *patricia));
  if (patricia == nullptr) {
    out_of_memory("patricia/new_patricia: unable to allocate memory");
  }
  patricia->maxbits = maxbits;
  patricia->head = nullptr;
  patricia->num_active_node = 0;
  assert(static_cast<size_t>(maxbits) <= PATRICIA_MAXBITS); /* XXX */
  num_active_patricia++;
  return (patricia);
}

/*
 * if func is supplied, it will be called as func(node->data)
 * before deleting the node
 */

void Clear_Patricia(patricia_tree_t* patricia, data_fn_t func) {
  assert(patricia);
  if (patricia->head) {
    patricia_node_t* Xstack[PATRICIA_MAXBITS + 1];
    patricia_node_t** Xsp = Xstack;
    patricia_node_t* Xrn = patricia->head;
    while (Xrn) {
      patricia_node_t* l = Xrn->l;
      patricia_node_t* r = Xrn->r;
      if (Xrn->prefix) {
        Deref_Prefix(Xrn->prefix);
        if (Xrn->data && func) {
          func(Xrn->data);
        }
      } else {
        assert(Xrn->data == nullptr);
      }
      Delete(Xrn);
      patricia->num_active_node--;
      if (l) {
        if (r) {
          *Xsp++ = r;
        }
        Xrn = l;
      } else if (r) {
        Xrn = r;
      } else if (Xsp != Xstack) {
        Xrn = *(--Xsp);
      } else {
        Xrn = nullptr;
      }
    }
  }
  assert(patricia->num_active_node == 0);
  /* Delete (patricia); */
}

auto Destroy_Patricia(patricia_tree_t* patricia, data_fn_t func) -> void {
  Clear_Patricia(patricia, func);
  Delete(patricia);
  num_active_patricia--;
}

auto patricia_search_exact(patricia_tree_t* patricia, prefix_t* prefix)
  -> patricia_node_t* {
  patricia_node_t* node;
  u_char* addr;
  u_int bitlen;
  assert(patricia);
  assert(prefix);
  assert(prefix->bitlen <= patricia->maxbits);
  if (patricia->head == nullptr) {
    return (nullptr);
  }
  node = patricia->head;
  addr = prefix_touchar(prefix);
  bitlen = prefix->bitlen;
  while (node->bit < bitlen) {
    if (BIT_TEST(addr[node->bit >> 3], 0x80 >> (node->bit & 0x07))) {
      node = node->r;
    } else {
      node = node->l;
    }
    if (node == nullptr) {
      return (nullptr);
    }
  }
  if (node->bit > bitlen || node->prefix == nullptr) {
    return (nullptr);
  }
  assert(node->bit == bitlen);
  assert(node->bit == node->prefix->bitlen);
  if (comp_with_mask(prefix_tochar(node->prefix), prefix_tochar(prefix),
                     bitlen)) {
    return (node);
  }
  return (nullptr);
}

auto patricia_search_all(patricia_tree_t* patricia, prefix_t* prefix,
                         patricia_node_t*** list, int* n) -> int {
  patricia_node_t* node;
  patricia_node_t* stack[PATRICIA_MAXBITS + 1];
  u_char* addr;
  u_int bitlen;
  int cnt = 0;
  assert(patricia);
  assert(prefix);
  assert(prefix->bitlen <= patricia->maxbits);
  assert(n);
  assert(list);
  assert(*list == nullptr);
  *n = 0;
  if (patricia->head == nullptr) {
    return 0;
  }
  node = patricia->head;
  addr = prefix_touchar(prefix);
  bitlen = prefix->bitlen;
  while (node->bit < bitlen) {
    if (node->prefix) {
      stack[cnt++] = node;
    }
    if (BIT_TEST(addr[node->bit >> 3], 0x80 >> (node->bit & 0x07))) {
      node = node->r;
    } else {
      node = node->l;
    }
    if (node == nullptr) {
      break;
    }
  }
  if (node && node->prefix) {
    stack[cnt++] = node;
  }
  if (cnt <= 0) {
    return 0;
  }
  // ok, now we have an upper bound of how much we can return. Let's just alloc
  // that...
  patricia_node_t** outlist
    = static_cast<patricia_node_t**>(calloc(cnt, sizeof(patricia_node_t*)));
  if (outlist == nullptr) {
    out_of_memory("patricia/patricia_search_all: unable to allocate memory");
  }
  while (--cnt >= 0) {
    node = stack[cnt];
    if (comp_with_mask(prefix_tochar(node->prefix), prefix_tochar(prefix),
                       node->prefix->bitlen)) {
      outlist[*n] = node;
      (*n)++;
    }
  }
  *list = outlist;
  return *n != 0 ? 1 : 0;
}

/* if inclusive != 0, "best" may be the given prefix itself */
auto patricia_search_best2(patricia_tree_t* patricia, prefix_t* prefix,
                           int inclusive) -> patricia_node_t* {
  patricia_node_t* node;
  patricia_node_t* stack[PATRICIA_MAXBITS + 1];
  u_char* addr;
  u_int bitlen;
  int cnt = 0;
  assert(patricia);
  assert(prefix);
  assert(prefix->bitlen <= patricia->maxbits);
  if (patricia->head == nullptr) {
    return (nullptr);
  }
  node = patricia->head;
  addr = prefix_touchar(prefix);
  bitlen = prefix->bitlen;
  while (node->bit < bitlen) {
    if (node->prefix) {
      stack[cnt++] = node;
    }
    if (BIT_TEST(addr[node->bit >> 3], 0x80 >> (node->bit & 0x07))) {
      node = node->r;
    } else {
      node = node->l;
    }
    if (node == nullptr) {
      break;
    }
  }
  if (inclusive && node && node->prefix) {
    stack[cnt++] = node;
  }
  if (cnt <= 0) {
    return (nullptr);
  }
  while (--cnt >= 0) {
    node = stack[cnt];
    if (comp_with_mask(prefix_tochar(node->prefix), prefix_tochar(prefix),
                       node->prefix->bitlen)
        && node->prefix->bitlen <= bitlen) {
      return (node);
    }
  }
  return (nullptr);
}

auto patricia_search_best(patricia_tree_t* patricia, prefix_t* prefix)
  -> patricia_node_t* {
  return (patricia_search_best2(patricia, prefix, 1));
}

auto patricia_lookup(patricia_tree_t* patricia, prefix_t* prefix)
  -> patricia_node_t* {
  patricia_node_t *node, *new_node, *parent, *glue;
  u_char *addr, *test_addr;
  u_int bitlen, check_bit, differ_bit;
  int i, j, r;
  assert(patricia);
  assert(prefix);
  assert(prefix->bitlen <= patricia->maxbits);
  if (patricia->head == nullptr) {
    node = static_cast<patricia_node_t*>(calloc(1, sizeof *node));
    if (node == nullptr) {
      out_of_memory("patricia/patricia_lookup: unable to allocate memory");
    }
    node->bit = prefix->bitlen;
    node->prefix = Ref_Prefix(prefix);
    node->parent = nullptr;
    node->l = node->r = nullptr;
    node->data = nullptr;
    patricia->head = node;
    patricia->num_active_node++;
    return (node);
  }
  addr = prefix_touchar(prefix);
  bitlen = prefix->bitlen;
  node = patricia->head;
  while (node->bit < bitlen || node->prefix == nullptr) {
    if (node->bit < patricia->maxbits
        && BIT_TEST(addr[node->bit >> 3], 0x80 >> (node->bit & 0x07))) {
      if (node->r == nullptr) {
        break;
      }
      node = node->r;
    } else {
      if (node->l == nullptr) {
        break;
      }
      node = node->l;
    }
    assert(node);
  }
  assert(node->prefix);
  test_addr = prefix_touchar(node->prefix);
  /* find the first bit different */
  check_bit = (node->bit < bitlen) ? node->bit : bitlen;
  differ_bit = 0;
  for (i = 0u; i * 8u < check_bit; i++) {
    if ((r = (addr[i] ^ test_addr[i])) == 0) {
      differ_bit = (i + 1) * 8;
      continue;
    }
    /* I know the better way, but for now */
    for (j = 0; j < 8; j++) {
      if (BIT_TEST(r, (0x80 >> j))) {
        break;
      }
    }
    /* must be found */
    assert(j < 8);
    differ_bit = i * 8 + j;
    break;
  }
  if (differ_bit > check_bit) {
    differ_bit = check_bit;
  }
  parent = node->parent;
  while (parent && parent->bit >= differ_bit) {
    node = parent;
    parent = node->parent;
  }
  if (differ_bit == bitlen && node->bit == bitlen) {
    if (node->prefix) {
      return (node);
    }
    node->prefix = Ref_Prefix(prefix);
    assert(node->data == nullptr);
    return (node);
  }
  new_node = static_cast<patricia_node_t*>(calloc(1, sizeof *new_node));
  if (new_node == nullptr) {
    out_of_memory("patricia/patricia_lookup: unable to allocate memory");
  }
  new_node->bit = prefix->bitlen;
  new_node->prefix = Ref_Prefix(prefix);
  new_node->parent = nullptr;
  new_node->l = new_node->r = nullptr;
  new_node->data = nullptr;
  patricia->num_active_node++;
  if (node->bit == differ_bit) {
    new_node->parent = node;
    if (node->bit < patricia->maxbits
        && BIT_TEST(addr[node->bit >> 3], 0x80 >> (node->bit & 0x07))) {
      assert(node->r == nullptr);
      node->r = new_node;
    } else {
      assert(node->l == nullptr);
      node->l = new_node;
    }
    return (new_node);
  }
  if (bitlen == differ_bit) {
    if (bitlen < patricia->maxbits
        && BIT_TEST(test_addr[bitlen >> 3], 0x80 >> (bitlen & 0x07))) {
      new_node->r = node;
    } else {
      new_node->l = node;
    }
    new_node->parent = node->parent;
    if (node->parent == nullptr) {
      assert(patricia->head == node);
      patricia->head = new_node;
    } else if (node->parent->r == node) {
      node->parent->r = new_node;
    } else {
      node->parent->l = new_node;
    }
    node->parent = new_node;
  } else {
    glue = static_cast<patricia_node_t*>(calloc(1, sizeof *glue));
    if (glue == nullptr) {
      out_of_memory("patricia/patricia_lookup: unable to allocate memory");
    }
    glue->bit = differ_bit;
    glue->prefix = nullptr;
    glue->parent = node->parent;
    glue->data = nullptr;
    patricia->num_active_node++;
    if (differ_bit < patricia->maxbits
        && BIT_TEST(addr[differ_bit >> 3], 0x80 >> (differ_bit & 0x07))) {
      glue->r = new_node;
      glue->l = node;
    } else {
      glue->r = node;
      glue->l = new_node;
    }
    new_node->parent = glue;
    if (node->parent == nullptr) {
      assert(patricia->head == node);
      patricia->head = glue;
    } else if (node->parent->r == node) {
      node->parent->r = glue;
    } else {
      node->parent->l = glue;
    }
    node->parent = glue;
  }
  return (new_node);
}

auto patricia_remove(patricia_tree_t* patricia, patricia_node_t* node) -> void {
  patricia_node_t *parent, *child;
  assert(patricia);
  assert(node);
  if (node->r && node->l) {
    /* this might be a placeholder node -- have to check and make sure
     * there is a prefix aossciated with it ! */
    if (node->prefix != nullptr) {
      Deref_Prefix(node->prefix);
    }
    node->prefix = nullptr;
    /* Also I needed to clear data pointer -- masaki */
    node->data = nullptr;
    return;
  }
  if (node->r == nullptr && node->l == nullptr) {
    parent = node->parent;
    Deref_Prefix(node->prefix);
    Delete(node);
    patricia->num_active_node--;
    if (parent == nullptr) {
      assert(patricia->head == node);
      patricia->head = nullptr;
      return;
    }
    if (parent->r == node) {
      parent->r = nullptr;
      child = parent->l;
    } else {
      assert(parent->l == node);
      parent->l = nullptr;
      child = parent->r;
    }
    if (parent->prefix) {
      return;
    }
    /* we need to remove parent too */
    if (parent->parent == nullptr) {
      assert(patricia->head == parent);
      patricia->head = child;
    } else if (parent->parent->r == parent) {
      parent->parent->r = child;
    } else {
      assert(parent->parent->l == parent);
      parent->parent->l = child;
    }
    child->parent = parent->parent;
    Delete(parent);
    patricia->num_active_node--;
    return;
  }
  if (node->r) {
    child = node->r;
  } else {
    assert(node->l);
    child = node->l;
  }
  parent = node->parent;
  child->parent = parent;
  Deref_Prefix(node->prefix);
  Delete(node);
  patricia->num_active_node--;
  if (parent == nullptr) {
    assert(patricia->head == node);
    patricia->head = child;
    return;
  }
  if (parent->r == node) {
    parent->r = child;
  } else {
    assert(parent->l == node);
    parent->l = child;
  }
}

} // namespace

namespace tenzir::detail {

namespace {

auto make_prefix(const subnet& key) -> prefix_t* {
  auto* prefix = reinterpret_cast<prefix_t*>(std::malloc(sizeof(prefix_t)));
  if (prefix == nullptr) {
    return nullptr;
  }
  std::memcpy(&prefix->add.sin6, as_bytes(key.network()).data(), 16);
  prefix->family = AF_INET6;
  prefix->bitlen = key.length();
  prefix->ref_count = 1;
  return prefix;
}

auto make_subnet(const prefix_t* prefix) -> subnet {
  const auto* ptr = reinterpret_cast<const std::byte*>(&prefix->add.sin6);
  auto bytes = std::span<const std::byte, 16>{ptr, 16};
  return subnet{ip::v6(bytes), narrow_cast<uint8_t>(prefix->bitlen)};
}

auto make_subnet_data_pair(patricia_node_t* node)
  -> std::pair<subnet, std::any*> {
  if (not node) {
    return {{}, nullptr};
  }
  return {make_subnet(node->prefix), reinterpret_cast<std::any*>(node->data)};
}

} // namespace

struct type_erased_subnet_tree::impl {
  static auto delete_data(void* ptr) -> void {
    delete reinterpret_cast<std::any*>(ptr);
  }

  struct patricia_tree_deleter {
    auto operator()(patricia_tree_t* ptr) const noexcept -> void {
      if (ptr) {
        Destroy_Patricia(ptr, delete_data);
      }
    }
  };

  impl() noexcept {
    tree.reset(New_Patricia(128));
  }

  std::unique_ptr<patricia_tree_t, patricia_tree_deleter> tree;
};

type_erased_subnet_tree::type_erased_subnet_tree() noexcept
  : impl_{std::make_unique<impl>()} {
}

type_erased_subnet_tree::type_erased_subnet_tree(
  type_erased_subnet_tree&& other) noexcept
  : impl_{std::move(other.impl_)} {
}

auto type_erased_subnet_tree::operator=(type_erased_subnet_tree&& other) noexcept
  -> type_erased_subnet_tree& {
  impl_ = std::move(other.impl_);
  return *this;
}

type_erased_subnet_tree::~type_erased_subnet_tree() noexcept {
  // Needs to be defined for pimpl.
}

auto type_erased_subnet_tree::lookup(subnet key) const -> const std::any* {
  auto* prefix = make_prefix(key);
  if (prefix == nullptr) {
    return nullptr;
  }
  auto* node = patricia_search_exact(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  return node ? reinterpret_cast<const std::any*>(node->data) : nullptr;
}

auto type_erased_subnet_tree::lookup(subnet key) -> std::any* {
  auto* prefix = make_prefix(key);
  if (prefix == nullptr) {
    return nullptr;
  }
  auto* node = patricia_search_exact(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  return node ? reinterpret_cast<std::any*>(node->data) : nullptr;
}

auto type_erased_subnet_tree::match(ip key) const
  -> std::pair<subnet, const std::any*> {
  return match(subnet{key, 128});
}

auto type_erased_subnet_tree::match(ip key) -> std::pair<subnet, std::any*> {
  return match(subnet{key, 128});
}

auto type_erased_subnet_tree::match(subnet key) const
  -> std::pair<subnet, const std::any*> {
  auto* prefix = make_prefix(key);
  if (prefix == nullptr) {
    return make_subnet_data_pair(nullptr);
  }
  auto* node = patricia_search_best(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  return make_subnet_data_pair(node);
}

auto type_erased_subnet_tree::match(subnet key)
  -> std::pair<subnet, std::any*> {
  auto* prefix = make_prefix(key);
  if (prefix == nullptr) {
    return make_subnet_data_pair(nullptr);
  }
  auto* node = patricia_search_best(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  return make_subnet_data_pair(node);
}

auto type_erased_subnet_tree::search(ip key) const
  -> generator<std::pair<subnet, const std::any*>> {
  return search(subnet{key, 128});
}

auto type_erased_subnet_tree::search(ip key)
  -> generator<std::pair<subnet, std::any*>> {
  return search(subnet{key, 128});
}

auto type_erased_subnet_tree::search(subnet key) const
  -> generator<std::pair<subnet, const std::any*>> {
  auto* prefix = make_prefix(key);
  auto num_elements = 0;
  patricia_node_t** xs = nullptr;
  patricia_search_all(impl_->tree.get(), prefix, &xs, &num_elements);
  auto guard = detail::scope_guard([prefix, xs]() noexcept {
    Deref_Prefix(prefix);
    free(xs);
  });
  for (auto i = 0; i < num_elements; ++i) {
    co_yield make_subnet_data_pair(xs[i]);
  }
}

auto type_erased_subnet_tree::search(subnet key)
  -> generator<std::pair<subnet, std::any*>> {
  auto* prefix = make_prefix(key);
  auto num_elements = 0;
  patricia_node_t** xs = nullptr;
  patricia_search_all(impl_->tree.get(), prefix, &xs, &num_elements);
  auto guard = detail::scope_guard([prefix, xs]() noexcept {
    Deref_Prefix(prefix);
    free(xs);
  });
  for (auto i = 0; i < num_elements; ++i) {
    co_yield make_subnet_data_pair(xs[i]);
  }
}

auto type_erased_subnet_tree::nodes() const
  -> generator<std::pair<subnet, const std::any*>> {
  patricia_node_t* node;
  PATRICIA_WALK(impl_->tree->head, node) {
    co_yield make_subnet_data_pair(node);
  }
  PATRICIA_WALK_END;
}

auto type_erased_subnet_tree::nodes()
  -> generator<std::pair<subnet, std::any*>> {
  patricia_node_t* node;
  PATRICIA_WALK(impl_->tree->head, node) {
    co_yield make_subnet_data_pair(node);
  }
  PATRICIA_WALK_END;
}

auto type_erased_subnet_tree::insert(subnet key, std::any value) -> bool {
  auto* prefix = make_prefix(key);
  if (prefix == nullptr) {
    return false;
  }
  auto* node = patricia_lookup(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  if (node == nullptr) {
    return false;
  }
  auto result = true;
  if (node->data != nullptr) {
    delete reinterpret_cast<const std::any*>(node->data);
    result = false;
  }
  // Deleted in Clear_Patricia() or Destroy_Patricia().
  node->data = new std::any{std::move(value)};
  return result;
}

auto type_erased_subnet_tree::erase(subnet key) -> bool {
  auto* prefix = make_prefix(key);
  TENZIR_ASSERT(prefix != nullptr);
  auto* node = patricia_search_exact(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  if (node == nullptr) {
    return false;
  }
  if (node->data != nullptr) {
    delete reinterpret_cast<const std::any*>(node->data);
  }
  patricia_remove(impl_->tree.get(), node);
  return true;
}

auto type_erased_subnet_tree::clear() -> void {
  Clear_Patricia(impl_->tree.get(), type_erased_subnet_tree::impl::delete_data);
}

} // namespace tenzir::detail
