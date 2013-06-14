#ifndef VAST_UTIL_PP_H
#define VAST_UTIL_PP_H

// Basics.
#define VAST_PP_MAX_ARGS 8
#define VAST_PP_ARG(_1, _2, _3, _4, _5, _6, _7, _8, ...) _8
#define VAST_PP_NARG_1(...) VAST_PP_ARG(__VA_ARGS__, 7, 6, 5, 4, 3, 2, 1, 0,)
#define VAST_PP_HAS_COMMA(...) VAST_PP_ARG(__VA_ARGS__, 1, 1, 1, 1, 1, 1, 0)

// Concatenation.
// 1) Paste first, then evaluate the result.
#define VAST_PP_CAT2(_1, _2) _1 ## _2
// 2) Evaluate first, then paste the result.
#define VAST_PP_PASTE0()
#define VAST_PP_PASTE1(_1) _1
#define VAST_PP_PASTE2(_1, _2) VAST_PP_CAT2(_1, _2)
#define VAST_PP_PASTE3(_1,  _2, _3)                                \
    VAST_PP_PASTE2(VAST_PP_PASTE2(_1, _2), _3)
#define VAST_PP_PASTE4(_1,  _2, _3, _4)                            \
    VAST_PP_PASTE2(VAST_PP_PASTE3(_1, _2, _3), _4)
#define VAST_PP_PASTE5(_1,  _2, _3, _4, _5)                        \
    VAST_PP_PASTE2(VAST_PP_PASTE4(_1, _2, _3, _4), _5)
#define VAST_PP_PASTE6(_1,  _2, _3, _4, _5, _6)                    \
    VAST_PP_PASTE2(VAST_PP_PASTE5(_1, _2, _3, _4, _5), _6)

/// Checking for empty __VA_ARGS__.
/// http://gustedt.wordpress.com/2010/06/08/detect-empty-macro-arguments/
#define VAST_PP_TRIGGER_PARENTHESIS_(...) ,
#define VAST_PP_IS_EMPTY_IMPL(_0, _1, _2, _3) VAST_PP_HAS_COMMA(   \
    VAST_PP_PASTE5(VAST_PP_IS_EMPTY_CASE_, _0, _1, _2, _3))
#define VAST_PP_IS_EMPTY_CASE_0001 ,
#define VAST_PP_IS_EMPTY(...)                                                 \
    VAST_PP_IS_EMPTY_IMPL(                                                    \
      VAST_PP_HAS_COMMA(__VA_ARGS__),                                         \
      VAST_PP_HAS_COMMA(VAST_PP_TRIGGER_PARENTHESIS_ __VA_ARGS__),              \
      VAST_PP_HAS_COMMA(__VA_ARGS__ (/*empty*/)),                             \
      VAST_PP_HAS_COMMA(VAST_PP_TRIGGER_PARENTHESIS_ __VA_ARGS__ (/*empty*/))   \
      )

// Argument counting.
#define VAST_PP_NARG_EMPTY_1(X) 0
#define VAST_PP_NARG_EMPTY_0(X) X
#define VAST_PP_NARG_IMPL_1(B, X) VAST_PP_NARG_IMPL_2(VAST_PP_PASTE2(VAST_PP_NARG_EMPTY_, B), X)
#define VAST_PP_NARG_IMPL_2(B, X) B(X)
#define VAST_PP_NARG_IMPL(...) VAST_PP_NARG_1(__VA_ARGS__)
#define VAST_PP_NARG(...) VAST_PP_NARG_IMPL_1(VAST_PP_IS_EMPTY(__VA_ARGS__), VAST_PP_NARG_IMPL(__VA_ARGS__))

// Overloading.
#define VAST_PP_OVERLOAD(PREFIX, ...) VAST_PP_PASTE2(PREFIX, VAST_PP_NARG(__VA_ARGS__))

#endif

