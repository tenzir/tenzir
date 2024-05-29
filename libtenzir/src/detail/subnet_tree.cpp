//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/subnet_tree.hpp"

#include "tenzir/detail/narrow.hpp"

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

#define addroute make_and_lookup

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

auto patricia_process(patricia_tree_t* patricia, prefix_data_fn_t func) -> void;

auto Deref_Prefix(prefix_t* prefix) -> void;
auto prefix_toa(prefix_t* prefix) -> char*;

/* { from demo.c */

auto ascii2prefix(int family, char* string) -> prefix_t*;

auto make_and_lookup(patricia_tree_t* tree, char* string) -> patricia_node_t*;

/* } */

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

/* this allows imcomplete prefix */
auto my_inet_pton(int af, const char* src, void* dst) -> int {
  if (af == AF_INET) {
    int i, c, val;
    u_char xp[sizeof(struct in_addr)] = {0, 0, 0, 0};
    for (i = 0;; i++) {
      c = *src++;
      if (!isdigit(c)) {
        return (-1);
      }
      val = 0;
      do {
        val = val * 10 + c - '0';
        if (val > 255) {
          return (0);
        }
        c = *src++;
      } while (c && isdigit(c));
      xp[i] = val;
      if (c == '\0') {
        break;
      }
      if (c != '.') {
        return (0);
      }
      if (i >= 3) {
        return (0);
      }
    }
    memcpy(dst, xp, sizeof(struct in_addr));
    return (1);
  } else if (af == AF_INET6) {
    return (inet_pton(af, src, dst));
  } else {
#ifndef NT
    errno = EAFNOSUPPORT;
#endif /* NT */
    return -1;
  }
}

#define PATRICIA_MAX_THREADS 16
#define PATRICIA_PREFIX_BUF_LEN 53

/*
 * convert prefix information to ascii string with length
 * thread safe and (almost) re-entrant implementation
 */
auto prefix_toa2x(prefix_t* prefix, char* buff, int with_len) -> char* {
  if (prefix == nullptr) {
    return ("(Null)");
  }
  assert(prefix->ref_count >= 0);
  if (buff == nullptr) {
    struct buffer {
      char buffs[PATRICIA_MAX_THREADS][PATRICIA_PREFIX_BUF_LEN];
      u_int i;
    }* buffp;
#if 0
	THREAD_SPECIFIC_DATA (struct buffer, buffp, 1);
#else
    { /* for scope only */
      static struct buffer local_buff;
      buffp = &local_buff;
    }
#endif
    if (buffp == nullptr) {
      /* XXX should we report an error? */
      return (nullptr);
    }
    buff = buffp->buffs[buffp->i++ % PATRICIA_MAX_THREADS];
  }
  if (prefix->family == AF_INET) {
    u_char* a;
    assert(prefix->bitlen <= sizeof(struct in_addr) * 8);
    a = prefix_touchar(prefix);
    if (with_len) {
      snprintf(buff, PATRICIA_PREFIX_BUF_LEN, "%d.%d.%d.%d/%d", a[0], a[1],
               a[2], a[3], prefix->bitlen);
    } else {
      snprintf(buff, PATRICIA_PREFIX_BUF_LEN, "%d.%d.%d.%d", a[0], a[1], a[2],
               a[3]);
    }
    return (buff);
  } else if (prefix->family == AF_INET6) {
    char* r;
    r = (char*)inet_ntop(AF_INET6, &prefix->add.sin6, buff,
                         48 /* a guess value */);
    if (r && with_len) {
      assert(prefix->bitlen <= sizeof(struct in6_addr) * 8);
      snprintf(buff + strlen(buff), PATRICIA_PREFIX_BUF_LEN - strlen(buff),
               "/%d", prefix->bitlen);
    }
    return (buff);
  } else {
    return (nullptr);
  }
}

/* prefix_toa2
 * convert prefix information to ascii string
 */
auto prefix_toa2(prefix_t* prefix, char* buff) -> char* {
  return (prefix_toa2x(prefix, buff, 0));
}

/* prefix_toa
 */
auto prefix_toa(prefix_t* prefix) -> char* {
  return (prefix_toa2(prefix, (char*)nullptr));
}

auto New_Prefix2(int family, void* dest, int bitlen, prefix_t* prefix)
  -> prefix_t* {
  int dynamic_allocated = 0;
  int default_bitlen = sizeof(struct in_addr) * 8;
  prefix4_t* p4 = nullptr;
  if (family == AF_INET6) {
    default_bitlen = sizeof(struct in6_addr) * 8;
    if (prefix == nullptr) {
      prefix = static_cast<prefix_t*>(calloc(1, sizeof(prefix_t)));
      if (prefix == nullptr) {
        out_of_memory("patricia/new_prefix2: unable to allocate memory");
      }
      dynamic_allocated++;
    }
    memcpy(&prefix->add.sin6, dest, sizeof(struct in6_addr));
  } else if (family == AF_INET) {
    if (prefix == nullptr) {
#ifndef NT
      prefix = static_cast<prefix_t*>(calloc(1, sizeof(prefix4_t)));
      if (prefix == nullptr) {
        out_of_memory("patricia/new_prefix2: unable to allocate memory");
      }
#else
      // for some reason, compiler is getting
      // prefix4_t size incorrect on NT
      prefix = calloc(1, sizeof(prefix_t));
      if (prefix == nullptr) {
        out_of_memory("patricia/new_prefix2: unable to allocate memory");
      }
#endif /* NT */
      dynamic_allocated++;
    }
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
  /* fprintf(stderr, "[C %s, %d]\n", prefix_toa (prefix), prefix->ref_count);
   */
  return (prefix);
}

auto New_Prefix(int family, void* dest, int bitlen) -> prefix_t* {
  return (New_Prefix2(family, dest, bitlen, nullptr));
}

/* ascii2prefix
 */
auto ascii2prefix(int family, char* string) -> prefix_t* {
  u_long bitlen, maxbitlen = 0;
  char* cp;
  struct in_addr sin;
  struct in6_addr sin6;
  int result;
  char save[MAXLINE];
  if (string == nullptr) {
    return (nullptr);
  }
  /* easy way to handle both families */
  if (family == 0) {
    family = AF_INET;
    if (strchr(string, ':')) {
      family = AF_INET6;
    }
  }
  if (family == AF_INET) {
    maxbitlen = sizeof(struct in_addr) * 8;
  } else if (family == AF_INET6) {
    maxbitlen = sizeof(struct in6_addr) * 8;
  }
  if ((cp = strchr(string, '/')) != nullptr) {
    bitlen = atol(cp + 1);
    /* *cp = '\0'; */
    /* copy the string to save. Avoid destroying the string */
    assert(cp - string < MAXLINE);
    memcpy(save, string, cp - string);
    save[cp - string] = '\0';
    string = save;
    if (/* bitlen < 0 || */ bitlen > maxbitlen) {
      bitlen = maxbitlen;
    }
  } else {
    bitlen = maxbitlen;
  }
  if (family == AF_INET) {
    if ((result = my_inet_pton(AF_INET, string, &sin)) <= 0) {
      return (nullptr);
    }
    return (New_Prefix(AF_INET, &sin, bitlen));
  } else if (family == AF_INET6) {
// Get rid of this with next IPv6 upgrade
#if defined(NT) && !defined(HAVE_INET_NTOP)
    inet6_addr(string, &sin6);
    return (New_Prefix(AF_INET6, &sin6, bitlen));
#else
    if ((result = inet_pton(AF_INET6, string, &sin6)) <= 0) {
      return (nullptr);
    }
#endif /* NT */
    return (New_Prefix(AF_INET6, &sin6, bitlen));
  } else {
    return (nullptr);
  }
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
  /* fprintf(stderr, "[A %s, %d]\n", prefix_toa (prefix), prefix->ref_count);
   */
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

/* #define PATRICIA_DEBUG 1 */

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
  assert(maxbits <= PATRICIA_MAXBITS); /* XXX */
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

/*
 * if func is supplied, it will be called as func(node->prefix, node->data)
 */

void patricia_process(patricia_tree_t* patricia, prefix_data_fn_t func) {
  patricia_node_t* node;
  assert(func);
  PATRICIA_WALK(patricia->head, node) {
    func(node->prefix, node->data);
  }
  PATRICIA_WALK_END;
}

auto patricia_walk_inorder(patricia_node_t* node, prefix_data_fn_t func)
  -> size_t {
  size_t n = 0;
  assert(func);
  if (node->l) {
    n += patricia_walk_inorder(node->l, func);
  }
  if (node->prefix) {
    func(node->prefix, node->data);
    n++;
  }
  if (node->r) {
    n += patricia_walk_inorder(node->r, func);
  }
  return n;
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
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_search_exact: take right %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_search_exact: take right at %u\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->r;
    } else {
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_search_exact: take left %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_search_exact: take left at %u\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->l;
    }
    if (node == nullptr) {
      return (nullptr);
    }
  }
#ifdef PATRICIA_DEBUG
  if (node->prefix) {
    fprintf(stderr, "patricia_search_exact: stop at %s/%d\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
  } else {
    fprintf(stderr, "patricia_search_exact: stop at %u\n", node->bit);
  }
#endif /* PATRICIA_DEBUG */
  if (node->bit > bitlen || node->prefix == nullptr) {
    return (nullptr);
  }
  assert(node->bit == bitlen);
  assert(node->bit == node->prefix->bitlen);
  if (comp_with_mask(prefix_tochar(node->prefix), prefix_tochar(prefix),
                     bitlen)) {
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_search_exact: found %s/%d\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
      fprintf(stderr, "patricia_search_all: push %s/%d\n",
              prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
      stack[cnt++] = node;
    }
    if (BIT_TEST(addr[node->bit >> 3], 0x80 >> (node->bit & 0x07))) {
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_search_all: take right %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_search_all: take right at %d\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->r;
    } else {
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_search_all: take left %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_search_all: take left at %d\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->l;
    }
    if (node == nullptr) {
      break;
    }
  }
  if (node && node->prefix) {
    stack[cnt++] = node;
  }
#ifdef PATRICIA_DEBUG
  if (node == nullptr) {
    fprintf(stderr, "patricia_search_all: stop at null\n");
  } else if (node->prefix) {
    fprintf(stderr, "patricia_search_all: stop at %s/%d\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
  } else {
    fprintf(stderr, "patricia_search_all: stop at %d\n", node->bit);
  }
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_search_all: pop %s/%d\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
    if (comp_with_mask(prefix_tochar(node->prefix), prefix_tochar(prefix),
                       node->prefix->bitlen)) {
#ifdef PATRICIA_DEBUG
      fprintf(stderr, "patricia_search_all: found %s/%d\n",
              prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
      fprintf(stderr, "patricia_search_best: push %s/%d\n",
              prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
      stack[cnt++] = node;
    }
    if (BIT_TEST(addr[node->bit >> 3], 0x80 >> (node->bit & 0x07))) {
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_search_best: take right %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_search_best: take right at %u\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->r;
    } else {
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_search_best: take left %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_search_best: take left at %u\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->l;
    }
    if (node == nullptr) {
      break;
    }
  }
  if (inclusive && node && node->prefix) {
    stack[cnt++] = node;
  }
#ifdef PATRICIA_DEBUG
  if (node == nullptr) {
    fprintf(stderr, "patricia_search_best: stop at null\n");
  } else if (node->prefix) {
    fprintf(stderr, "patricia_search_best: stop at %s/%d\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
  } else {
    fprintf(stderr, "patricia_search_best: stop at %u\n", node->bit);
  }
#endif /* PATRICIA_DEBUG */
  if (cnt <= 0) {
    return (nullptr);
  }
  while (--cnt >= 0) {
    node = stack[cnt];
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_search_best: pop %s/%d\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
    if (comp_with_mask(prefix_tochar(node->prefix), prefix_tochar(prefix),
                       node->prefix->bitlen)
        && node->prefix->bitlen <= bitlen) {
#ifdef PATRICIA_DEBUG
      fprintf(stderr, "patricia_search_best: found %s/%d\n",
              prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_lookup: new_node #0 %s/%d (head)\n",
            prefix_toa(prefix), prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_lookup: take right %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_lookup: take right at %u\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->r;
    } else {
      if (node->l == nullptr) {
        break;
      }
#ifdef PATRICIA_DEBUG
      if (node->prefix) {
        fprintf(stderr, "patricia_lookup: take left %s/%d\n",
                prefix_toa(node->prefix), node->prefix->bitlen);
      } else {
        fprintf(stderr, "patricia_lookup: take left at %u\n", node->bit);
      }
#endif /* PATRICIA_DEBUG */
      node = node->l;
    }
    assert(node);
  }
  assert(node->prefix);
#ifdef PATRICIA_DEBUG
  fprintf(stderr, "patricia_lookup: stop at %s/%d\n", prefix_toa(node->prefix),
          node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
  fprintf(stderr, "patricia_lookup: differ_bit %d\n", differ_bit);
#endif /* PATRICIA_DEBUG */
  parent = node->parent;
  while (parent && parent->bit >= differ_bit) {
    node = parent;
    parent = node->parent;
#ifdef PATRICIA_DEBUG
    if (node->prefix) {
      fprintf(stderr, "patricia_lookup: up to %s/%d\n",
              prefix_toa(node->prefix), node->prefix->bitlen);
    } else {
      fprintf(stderr, "patricia_lookup: up to %u\n", node->bit);
    }
#endif /* PATRICIA_DEBUG */
  }
  if (differ_bit == bitlen && node->bit == bitlen) {
    if (node->prefix) {
#ifdef PATRICIA_DEBUG
      fprintf(stderr, "patricia_lookup: found %s/%d\n",
              prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
      return (node);
    }
    node->prefix = Ref_Prefix(prefix);
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_lookup: new node #1 %s/%d (glue mod)\n",
            prefix_toa(prefix), prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_lookup: new_node #2 %s/%d (child)\n",
            prefix_toa(prefix), prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_lookup: new_node #3 %s/%d (parent)\n",
            prefix_toa(prefix), prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_lookup: new_node #4 %s/%d (glue+node)\n",
            prefix_toa(prefix), prefix->bitlen);
#endif /* PATRICIA_DEBUG */
  }
  return (new_node);
}

auto patricia_remove(patricia_tree_t* patricia, patricia_node_t* node) -> void {
  patricia_node_t *parent, *child;
  assert(patricia);
  assert(node);
  if (node->r && node->l) {
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_remove: #0 %s/%d (r & l)\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
    fprintf(stderr, "patricia_remove: #1 %s/%d (!r & !l)\n",
            prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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
#ifdef PATRICIA_DEBUG
  fprintf(stderr, "patricia_remove: #2 %s/%d (r ^ l)\n",
          prefix_toa(node->prefix), node->prefix->bitlen);
#endif /* PATRICIA_DEBUG */
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

auto make_subnet_data_pair(const patricia_node_t* node)
  -> std::pair<subnet, const data*> {
  return {make_subnet(node->prefix), reinterpret_cast<const data*>(node->data)};
}

} // namespace

struct subnet_tree::impl {
  static auto delete_data(void* ptr) -> void {
    delete reinterpret_cast<data*>(ptr);
  }

  struct patricia_tree_deleter {
    auto operator()(patricia_tree_t* ptr) const noexcept -> void {
      if (ptr) {
        Destroy_Patricia(ptr, delete_data);
      }
    }
  };

  impl() {
    tree.reset(New_Patricia(128));
  }

  std::unique_ptr<patricia_tree_t, patricia_tree_deleter> tree;
};

subnet_tree::subnet_tree() : impl_{std::make_unique<impl>()} {
}

subnet_tree::subnet_tree(subnet_tree&& other) noexcept
  : impl_{std::move(other.impl_)} {
}

auto subnet_tree::operator=(subnet_tree&& other) noexcept -> subnet_tree& {
  impl_ = std::move(other.impl_);
  return *this;
}

subnet_tree::~subnet_tree() {
  // Needs to be defined for pimpl.
}

auto subnet_tree::lookup(subnet key) const -> const data* {
  auto* prefix = make_prefix(key);
  if (prefix == nullptr) {
    return nullptr;
  }
  auto* node = patricia_search_exact(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  return node ? reinterpret_cast<const data*>(node->data) : nullptr;
}

auto subnet_tree::match(ip key) const -> const data* {
  return match(subnet{key, 128});
}

auto subnet_tree::match(subnet key) const -> const data* {
  auto* prefix = make_prefix(key);
  if (prefix == nullptr) {
    return nullptr;
  }
  auto* node = patricia_search_best(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  return node ? reinterpret_cast<const data*>(node->data) : nullptr;
}

auto subnet_tree::search(ip key) const
  -> generator<std::pair<subnet, const data*>> {
  return search(subnet{key, 128});
}

auto subnet_tree::search(subnet key) const
  -> generator<std::pair<subnet, const data*>> {
  auto* prefix = make_prefix(key);
  auto num_elements = 0;
  patricia_node_t** xs = nullptr;
  patricia_search_all(impl_->tree.get(), prefix, &xs, &num_elements);
  for (auto i = 0; i < num_elements; ++i) {
    co_yield make_subnet_data_pair(xs[i]);
  }
  Deref_Prefix(prefix);
  std::free(xs);
}

auto subnet_tree::nodes() const -> generator<std::pair<subnet, const data*>> {
  patricia_node_t* node;
  PATRICIA_WALK(impl_->tree->head, node) {
    co_yield make_subnet_data_pair(node);
  }
  PATRICIA_WALK_END;
}

auto subnet_tree::insert(subnet key, data value) -> bool {
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
    delete reinterpret_cast<const data*>(node->data);
    result = false;
  }
  // Deleted in Clear_Patricia() or Destroy_Patricia().
  node->data = new data{std::move(value)};
  return result;
}

auto subnet_tree::erase(subnet key) -> bool {
  auto* prefix = make_prefix(key);
  TENZIR_ASSERT(prefix != nullptr);
  auto* node = patricia_search_exact(impl_->tree.get(), prefix);
  Deref_Prefix(prefix);
  if (node == nullptr) {
    return false;
  }
  patricia_remove(impl_->tree.get(), node);
  return true;
}

auto subnet_tree::clear() -> void {
  Clear_Patricia(impl_->tree.get(), subnet_tree::impl::delete_data);
}

} // namespace tenzir::detail
