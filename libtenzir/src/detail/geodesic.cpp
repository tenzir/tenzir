//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file comes from a 3rd party and has been adapted to fit into the Tenzir
// code base. Details about the original file:
//
// - Repository: https://github.com/geographiclib/geographiclib-c
// - Commit:     d367e44e6042e233f87a1740c9c3a3b29f1f0a55
// - Path:       src/geodesic.c
// - Author:     Charles Karney
// - Copyright:  (c) Charles Karney (2012-2025)
// - License:    MIT/X11
//
// The adaptation retains only initialization and inverse-distance calculation.
// It removes the direct-geodesic, geodesic-line, polygon, area, azimuth, and
// public intermediate-result APIs and places the implementation in
// `tenzir::detail`.

#include <tenzir/detail/geodesic.hpp>

#include <float.h>
#include <math.h>

namespace tenzir::detail {

namespace {

// clang-format off

#define GEOGRAPHICLIB_GEODESIC_ORDER 6
#define nA1   GEOGRAPHICLIB_GEODESIC_ORDER
#define nC1   GEOGRAPHICLIB_GEODESIC_ORDER
#define nC1p  GEOGRAPHICLIB_GEODESIC_ORDER
#define nA2   GEOGRAPHICLIB_GEODESIC_ORDER
#define nC2   GEOGRAPHICLIB_GEODESIC_ORDER
#define nA3   GEOGRAPHICLIB_GEODESIC_ORDER
#define nA3x  nA3
#define nC3   GEOGRAPHICLIB_GEODESIC_ORDER
#define nC3x  ((nC3 * (nC3 - 1)) / 2)
#define nC    (GEOGRAPHICLIB_GEODESIC_ORDER + 1)

typedef int boolx;
enum booly { FALSE = 0, TRUE = 1 };
/* qd = quarter turn / degree
 * hd = half turn / degree
 * td = full turn / degree */
static constexpr auto qd = 90.0;
static constexpr auto hd = 2 * qd;
static constexpr auto td = 2 * hd;

struct Geodesic {
  double a;
  double f;
  double f1;
  double ep2;
  double n;
  double b;
  double etol2;
  double A3x[6];
  double C3x[15];
};

static unsigned init = 0;
static unsigned digits, maxit1, maxit2;
static double epsilon, realmin, pi, degree, NaN,
  tiny, tol0, tol1, tol2, tolb, xthresh;

static void Init(void) {
  if (!init) {
    digits = DBL_MANT_DIG;
    epsilon = DBL_EPSILON;
    realmin = DBL_MIN;
#if defined(M_PI)
    pi = M_PI;
#else
    pi = atan2(0.0, -1.0);
#endif
    maxit1 = 20;
    maxit2 = maxit1 + digits + 10;
    tiny = sqrt(realmin);
    tol0 = epsilon;
    /* Increase multiplier in defn of tol1 from 100 to 200 to fix inverse case
     * 52.784459512564 0 -52.784459512563990912 179.634407464943777557
     * which otherwise failed for Visual Studio 10 (Release and Debug) */
    tol1 = 200 * tol0;
    tol2 = sqrt(tol0);
    /* Check on bisection interval */
    tolb = tol0;
    xthresh = 1000 * tol2;
    degree = pi/hd;
    NaN = nan("0");
    init = 1;
  }
}

static double sq(double x) { return x * x; }

static double sumx(double u, double v, double* t) {
  volatile double s = u + v;
  volatile double up = s - v;
  volatile double vpp = s - up;
  up -= u;
  vpp -= v;
  if (t) *t = s != 0 ? 0 - (up + vpp) : s;
  /* error-free sum:
   * u + v =       s      + t
   *       = round(u + v) + t */
  return s;
}

static double polyvalx(int N, const double p[], double x) {
  double y = N < 0 ? 0 : *p++;
  while (--N >= 0) y = y * x + *p++;
  return y;
}

static void swapx(double* x, double* y)
{ double t = *x; *x = *y; *y = t; }

static void norm2(double* sinx, double* cosx) {
#if defined(_MSC_VER) && defined(_M_IX86)
  /* hypot for Visual Studio (A=win32) fails monotonicity, e.g., with
   *   x  = 0.6102683302836215
   *   y1 = 0.7906090004346522
   *   y2 = y1 + 1e-16
   * the test
   *   hypot(x, y2) >= hypot(x, y1)
   * fails.  See also
   *   https://bugs.python.org/issue43088 */
  double r = sqrt(*sinx * *sinx + *cosx * *cosx);
#else
  double r = hypot(*sinx, *cosx);
#endif
  *sinx /= r;
  *cosx /= r;
}

static double LatFix(double x)
{ return fabs(x) > qd ? NaN : x; }

static double AngDiff(double x, double y, double* e) {
  /* Use remainder instead of AngNormalize, since we treat boundary cases
   * later taking account of the error */
  double t, d = sumx(remainder(-x, (double)td), remainder( y, (double)td), &t);
  /* This second sum can only change d if abs(d) < 128, so don't need to
   * apply remainder yet again. */
  d = sumx(remainder(d, (double)td), t, &t);
  /* Fix the sign if d = -180, 0, 180. */
  if (d == 0 || fabs(d) == hd)
    /* If t == 0, take sign from y - x
     * else (t != 0, implies d = +/-180), d and t must have opposite signs */
    d = copysign(d, t == 0 ? y - x : -t);
  if (e) *e = t;
  return d;
}

static double AngRound(double x) {
  /* False positive in cppcheck requires "1.0" instead of "1" */
  const double z = 1.0/16.0;
  volatile double y = fabs(x);
  volatile double w = z - y;
  /* The compiler mustn't "simplify" z - (z - y) to y */
  y = w > 0 ? z - w : y;
  return copysign(y, x);
}

static void sincosdx(double x, double* sinx, double* cosx) {
  /* In order to minimize round-off errors, this function exactly reduces
   * the argument to the range [-45, 45] before converting it to radians. */
  double r, s, c; int q = 0;
  r = remquo(x, (double)qd, &q);
  /* now abs(r) <= 45 */
  r *= degree;
  /* Possibly could call the gnu extension sincos */
  s = sin(r); c = cos(r);
  switch ((unsigned)q & 3U) {
  case 0U: *sinx =  s; *cosx =  c; break;
  case 1U: *sinx =  c; *cosx = -s; break;
  case 2U: *sinx = -s; *cosx = -c; break;
  default: *sinx = -c; *cosx =  s; break; /* case 3U */
  }
  /* http://www.open-std.org/jtc1/sc22/wg14/www/docs/n1950.pdf */
  *cosx += 0;                   /* special values from F.10.1.12 */
  /* special values from F.10.1.13 */
  if (*sinx == 0) *sinx = copysign(*sinx, x);
}

static void sincosde(double x, double t, double* sinx, double* cosx) {
  /* In order to minimize round-off errors, this function exactly reduces
   * the argument to the range [-45, 45] before converting it to radians. */
  double r, s, c; int q = 0;
  r = AngRound(remquo(x, (double)qd, &q) + t);
  /* now abs(r) <= 45 */
  r *= degree;
  /* Possibly could call the gnu extension sincos */
  s = sin(r); c = cos(r);
  switch ((unsigned)q & 3U) {
  case 0U: *sinx =  s; *cosx =  c; break;
  case 1U: *sinx =  c; *cosx = -s; break;
  case 2U: *sinx = -s; *cosx = -c; break;
  default: *sinx = -c; *cosx =  s; break; /* case 3U */
  }
  /* http://www.open-std.org/jtc1/sc22/wg14/www/docs/n1950.pdf */
  *cosx += 0;                   /* special values from F.10.1.12 */
  /* special values from F.10.1.13 */
  if (*sinx == 0) *sinx = copysign(*sinx, x);
}

static void A3coeff(struct Geodesic* g);
static void C3coeff(struct Geodesic* g);
static double SinCosSeries(boolx sinp,
                           double sinx, double cosx,
                           const double c[], int n);
static void Lengths(
                    double eps, double sig12,
                    double ssig1, double csig1, double dn1,
                    double ssig2, double csig2, double dn2,
                    double* ps12b, double* pm12b, double* pm0,
                    /* Scratch area of the right size */
                    double Ca[]);
static double Astroid(double x, double y);
static double InverseStart(const struct Geodesic* g,
                           double sbet1, double cbet1, double dn1,
                           double sbet2, double cbet2, double dn2,
                           double lam12, double slam12, double clam12,
                           double* psalp1, double* pcalp1,
                           /* Only updated if return val >= 0 */
                           double* psalp2, double* pcalp2,
                           /* Only updated for short lines */
                           double* pdnm,
                           /* Scratch area of the right size */
                           double Ca[]);
static double Lambda12(const struct Geodesic* g,
                       double sbet1, double cbet1, double dn1,
                       double sbet2, double cbet2, double dn2,
                       double salp1, double calp1,
                       double slam120, double clam120,
                       double* psalp2, double* pcalp2,
                       double* psig12,
                       double* pssig1, double* pcsig1,
                       double* pssig2, double* pcsig2,
                       double* peps,
                       double* pdomg12,
                       boolx diffp, double* pdlam12,
                       /* Scratch area of the right size */
                       double Ca[]);
static double A3f(const struct Geodesic* g, double eps);
static void C3f(const struct Geodesic* g, double eps, double c[]);
static double A1m1f(double eps);
static void C1f(double eps, double c[]);
static double A2m1f(double eps);
static void C2f(double eps, double c[]);

static void init_geodesic(struct Geodesic* g, double a, double f) {
  if (!init) Init();
  g->a = a;
  g->f = f;
  g->f1 = 1 - g->f;
  double e2 = g->f * (2 - g->f);
  g->ep2 = e2 / sq(g->f1);   /* e2 / (1 - e2) */
  g->n = g->f / ( 2 - g->f);
  g->b = g->a * g->f1;
  /* The sig12 threshold for "really short".  Using the auxiliary sphere
   * solution with dnm computed at (bet1 + bet2) / 2, the relative error in the
   * azimuth consistency check is sig12^2 * abs(f) * min(1, 1-f/2) / 2.  (Error
   * measured for 1/100 < b/a < 100 and abs(f) >= 1/1000.  For a given f and
   * sig12, the max error occurs for lines near the pole.  If the old rule for
   * computing dnm = (dn1 + dn2)/2 is used, then the error increases by a
   * factor of 2.)  Setting this equal to epsilon gives sig12 = etol2.  Here
   * 0.1 is a safety factor (error decreased by 100) and max(0.001, abs(f))
   * stops etol2 getting too large in the nearly spherical case. */
  g->etol2 = 0.1 * tol2 /
    sqrt( fmax(0.001, fabs(g->f)) * fmin(1.0, 1 - g->f/2) / 2 );

  A3coeff(g);
  C3coeff(g);
}

static double geod_geninverse_int(const struct Geodesic* g,
                                  double lat1, double lon1,
                                  double lat2, double lon2) {
  double lon12, lon12s;
  int latsign, lonsign, swapp;
  double sbet1, cbet1, sbet2, cbet2, s12x = 0, m12x = 0;
  double dn1, dn2, lam12, slam12, clam12;
  double sig12, calp1 = 0, salp1 = 0, calp2 = 0, salp2 = 0;
  double Ca[nC];
  boolx meridian;
  /* Compute longitude difference (AngDiff does this carefully).  Result is
   * in [-180, 180] but -180 is only for west-going geodesics.  180 is for
   * east-going and meridional geodesics. */
  lon12 = AngDiff(lon1, lon2, &lon12s);
  /* Make longitude difference positive. */
  lonsign = signbit(lon12) ? -1 : 1;
  lon12 *= lonsign; lon12s *= lonsign;
  lam12 = lon12 * degree;
  /* Calculate sincos of lon12 + error (this applies AngRound internally). */
  sincosde(lon12, lon12s, &slam12, &clam12);
  lon12s = (hd - lon12) - lon12s; /* the supplementary longitude difference */

  /* If really close to the equator, treat as on equator. */
  lat1 = AngRound(LatFix(lat1));
  lat2 = AngRound(LatFix(lat2));
  /* Swap points so that point with higher (abs) latitude is point 1
   * If one latitude is a nan, then it becomes lat1. */
  swapp = fabs(lat1) < fabs(lat2) || lat2 != lat2 ? -1 : 1;
  if (swapp < 0) {
    lonsign *= -1;
    swapx(&lat1, &lat2);
  }
  /* Make lat1 <= -0 */
  latsign = signbit(lat1) ? 1 : -1;
  lat1 *= latsign;
  lat2 *= latsign;
  /* Now we have
   *
   *     0 <= lon12 <= 180
   *     -90 <= lat1 <= -0
   *     lat1 <= lat2 <= -lat1
   *
   * longsign, swapp, latsign register the transformation to bring the
   * coordinates to this canonical form.  In all cases, 1 means no change was
   * made.  We make these transformations so that there are few cases to
   * check, e.g., on verifying quadrants in atan2.  In addition, this
   * enforces some symmetries in the results returned. */

  sincosdx(lat1, &sbet1, &cbet1); sbet1 *= g->f1;
  /* Ensure cbet1 = +epsilon at poles */
  norm2(&sbet1, &cbet1); cbet1 = fmax(tiny, cbet1);

  sincosdx(lat2, &sbet2, &cbet2); sbet2 *= g->f1;
  /* Ensure cbet2 = +epsilon at poles */
  norm2(&sbet2, &cbet2); cbet2 = fmax(tiny, cbet2);

  /* If cbet1 < -sbet1, then cbet2 - cbet1 is a sensitive measure of the
   * |bet1| - |bet2|.  Alternatively (cbet1 >= -sbet1), abs(sbet2) + sbet1 is
   * a better measure.  This logic is used in assigning calp2 in Lambda12.
   * Sometimes these quantities vanish and in that case we force bet2 = +/-
   * bet1 exactly.  An example where is is necessary is the inverse problem
   * 48.522876735459 0 -48.52287673545898293 179.599720456223079643
   * which failed with Visual Studio 10 (Release and Debug) */

  if (cbet1 < -sbet1) {
    if (cbet2 == cbet1)
      sbet2 = copysign(sbet1, sbet2);
  } else {
    if (fabs(sbet2) == -sbet1)
      cbet2 = cbet1;
  }

  dn1 = sqrt(1 + g->ep2 * sq(sbet1));
  dn2 = sqrt(1 + g->ep2 * sq(sbet2));

  meridian = lat1 == -qd || slam12 == 0;

  if (meridian) {

    /* Endpoints are on a single full meridian, so the geodesic might lie on
     * a meridian. */

    double ssig1, csig1, ssig2, csig2;
    calp1 = clam12; salp1 = slam12; /* Head to the target longitude */
    calp2 = 1; salp2 = 0;           /* At the target we're heading north */

    /* tan(bet) = tan(sig) * cos(alp) */
    ssig1 = sbet1; csig1 = calp1 * cbet1;
    ssig2 = sbet2; csig2 = calp2 * cbet2;

    /* sig12 = sig2 - sig1 */
    sig12 = atan2(fmax(0.0, csig1 * ssig2 - ssig1 * csig2) + 0,
                            csig1 * csig2 + ssig1 * ssig2);
    Lengths(g->n, sig12, ssig1, csig1, dn1, ssig2, csig2, dn2,
            &s12x, &m12x, nullptr, Ca);
    /* Add the check for sig12 since zero length geodesics might yield m12 <
     * 0.  Test case was
     *
     *    echo 20.001 0 20.001 0 | GeodSolve -i
     */
    if (sig12 < tol2 || m12x >= 0) {
      /* Need at least 2, to handle 90 0 90 180 */
      if (sig12 < 3 * tiny ||
          /* Prevent negative s12 or m12 for short lines */
          (sig12 < tol0 && (s12x < 0 || m12x < 0)))
        sig12 = m12x = s12x = 0;
      m12x *= g->b;
      s12x *= g->b;
    } else
      /* m12 < 0, i.e., prolate and too close to anti-podal */
      meridian = FALSE;
  }

  if (!meridian &&
      sbet1 == 0 &&           /* and sbet2 == 0 */
      /* Mimic the way Lambda12 works with calp1 = 0 */
      (g->f <= 0 || lon12s >= g->f * hd)) {

    /* Geodesic runs along equator */
    calp1 = calp2 = 0; salp1 = salp2 = 1;
    s12x = g->a * lam12;
    sig12 = lam12 / g->f1;

  } else if (!meridian) {

    /* Now point1 and point2 belong within a hemisphere bounded by a
     * meridian and geodesic is neither meridional or equatorial. */

    /* Figure a starting point for Newton's method */
    double dnm = 0;
    sig12 = InverseStart(g, sbet1, cbet1, dn1, sbet2, cbet2, dn2,
                         lam12, slam12, clam12,
                         &salp1, &calp1, &salp2, &calp2, &dnm,
                         Ca);

    if (sig12 >= 0) {
      /* Short lines (InverseStart sets salp2, calp2, dnm) */
      s12x = sig12 * g->b * dnm;
    } else {

      /* Newton's method.  This is a straightforward solution of f(alp1) =
       * lambda12(alp1) - lam12 = 0 with one wrinkle.  f(alp) has exactly one
       * root in the interval (0, pi) and its derivative is positive at the
       * root.  Thus f(alp) is positive for alp > alp1 and negative for alp <
       * alp1.  During the course of the iteration, a range (alp1a, alp1b) is
       * maintained which brackets the root and with each evaluation of
       * f(alp) the range is shrunk, if possible.  Newton's method is
       * restarted whenever the derivative of f is negative (because the new
       * value of alp1 is then further from the solution) or if the new
       * estimate of alp1 lies outside (0,pi); in this case, the new starting
       * guess is taken to be (alp1a + alp1b) / 2. */
      double ssig1 = 0, csig1 = 0, ssig2 = 0, csig2 = 0, eps = 0, domg12 = 0;
      unsigned numit = 0;
      /* Bracketing range */
      double salp1a = tiny, calp1a = 1, salp1b = tiny, calp1b = -1;
      boolx tripn = FALSE;
      boolx tripb = FALSE;
      for (;; ++numit) {
        /* the WGS84 test set: mean = 1.47, sd = 1.25, max = 16
         * WGS84 and random input: mean = 2.85, sd = 0.60 */
        double dv = 0,
          v = Lambda12(g, sbet1, cbet1, dn1, sbet2, cbet2, dn2, salp1, calp1,
                        slam12, clam12,
                        &salp2, &calp2, &sig12, &ssig1, &csig1, &ssig2, &csig2,
                        &eps, &domg12, numit < maxit1, &dv, Ca);
        if (tripb ||
            /* Reversed test to allow escape with NaNs */
            !(fabs(v) >= (tripn ? 8 : 1) * tol0) ||
            /* Enough bisections to get accurate result */
            numit == maxit2)
          break;
        /* Update bracketing values */
        if (v > 0 && (numit > maxit1 || calp1/salp1 > calp1b/salp1b))
          { salp1b = salp1; calp1b = calp1; }
        else if (v < 0 && (numit > maxit1 || calp1/salp1 < calp1a/salp1a))
          { salp1a = salp1; calp1a = calp1; }
        if (numit < maxit1 && dv > 0) {
          double
            dalp1 = -v/dv;
          if (fabs(dalp1) < pi) {
            double
              sdalp1 = sin(dalp1), cdalp1 = cos(dalp1),
              nsalp1 = salp1 * cdalp1 + calp1 * sdalp1;
            if (nsalp1 > 0) {
              calp1 = calp1 * cdalp1 - salp1 * sdalp1;
              salp1 = nsalp1;
              norm2(&salp1, &calp1);
              /* In some regimes we don't get quadratic convergence because
               * slope -> 0.  So use convergence conditions based on epsilon
               * instead of sqrt(epsilon). */
              tripn = fabs(v) <= 16 * tol0;
              continue;
            }
          }
        }
        /* Either dv was not positive or updated value was outside legal
         * range.  Use the midpoint of the bracket as the next estimate.
         * This mechanism is not needed for the WGS84 ellipsoid, but it does
         * catch problems with more eccentric ellipsoids.  Its efficacy is
         * such for the WGS84 test set with the starting guess set to alp1 =
         * 90deg:
         * the WGS84 test set: mean = 5.21, sd = 3.93, max = 24
         * WGS84 and random input: mean = 4.74, sd = 0.99 */
        salp1 = (salp1a + salp1b)/2;
        calp1 = (calp1a + calp1b)/2;
        norm2(&salp1, &calp1);
        tripn = FALSE;
        tripb = (fabs(salp1a - salp1) + (calp1a - calp1) < tolb ||
                 fabs(salp1 - salp1b) + (calp1 - calp1b) < tolb);
      }
      Lengths(eps, sig12, ssig1, csig1, dn1, ssig2, csig2, dn2,
              &s12x, nullptr, nullptr, Ca);
      s12x *= g->b;
    }
  }

  return 0 + s12x; // Convert -0 to 0.
}

static double SinCosSeries(boolx sinp, double sinx, double cosx,
                    const double c[], int n) {
  /* Evaluate
   * y = sinp ? sum(c[i] * sin( 2*i    * x), i, 1, n) :
   *            sum(c[i] * cos((2*i+1) * x), i, 0, n-1)
   * using Clenshaw summation.  N.B. c[0] is unused for sin series
   * Approx operation count = (n + 5) mult and (2 * n + 2) add */
  double ar, y0, y1;
  c += (n + sinp);              /* Point to one beyond last element */
  ar = 2 * (cosx - sinx) * (cosx + sinx); /* 2 * cos(2 * x) */
  y0 = (n & 1) ? *--c : 0; y1 = 0;        /* accumulators for sum */
  /* Now n is even */
  n /= 2;
  while (n--) {
    /* Unroll loop x 2, so accumulators return to their original role */
    y1 = ar * y0 - y1 + *--c;
    y0 = ar * y1 - y0 + *--c;
  }
  return sinp
    ? 2 * sinx * cosx * y0      /* sin(2 * x) * y0 */
    : cosx * (y0 - y1);         /* cos(x) * (y0 - y1) */
}

static void Lengths(
             double eps, double sig12,
             double ssig1, double csig1, double dn1,
             double ssig2, double csig2, double dn2,
             double* ps12b, double* pm12b, double* pm0,
             /* Scratch area of the right size */
             double Ca[]) {
  double m0 = 0, J12 = 0, A1 = 0, A2 = 0;
  double Cb[nC];

  /* Return m12b = (reduced length)/b; also calculate s12b = distance/b,
   * and m0 = coefficient of secular term in expression for reduced length. */
  boolx redlp = pm12b || pm0;
  if (ps12b || redlp) {
    A1 = A1m1f(eps);
    C1f(eps, Ca);
    if (redlp) {
      A2 = A2m1f(eps);
      C2f(eps, Cb);
      m0 = A1 - A2;
      A2 = 1 + A2;
    }
    A1 = 1 + A1;
  }
  if (ps12b) {
    double B1 = SinCosSeries(TRUE, ssig2, csig2, Ca, nC1) -
      SinCosSeries(TRUE, ssig1, csig1, Ca, nC1);
    /* Missing a factor of b */
    *ps12b = A1 * (sig12 + B1);
    if (redlp) {
      double B2 = SinCosSeries(TRUE, ssig2, csig2, Cb, nC2) -
        SinCosSeries(TRUE, ssig1, csig1, Cb, nC2);
      J12 = m0 * sig12 + (A1 * B1 - A2 * B2);
    }
  } else if (redlp) {
    /* Assume here that nC1 >= nC2 */
    int l;
    for (l = 1; l <= nC2; ++l)
      Cb[l] = A1 * Ca[l] - A2 * Cb[l];
    J12 = m0 * sig12 + (SinCosSeries(TRUE, ssig2, csig2, Cb, nC2) -
                        SinCosSeries(TRUE, ssig1, csig1, Cb, nC2));
  }
  if (pm0) *pm0 = m0;
  if (pm12b)
    /* Missing a factor of b.
     * Add parens around (csig1 * ssig2) and (ssig1 * csig2) to ensure
     * accurate cancellation in the case of coincident points. */
    *pm12b = dn2 * (csig1 * ssig2) - dn1 * (ssig1 * csig2) -
      csig1 * csig2 * J12;
}

static double Astroid(double x, double y) {
  /* Solve k^4+2*k^3-(x^2+y^2-1)*k^2-2*y^2*k-y^2 = 0 for positive root k.
   * This solution is adapted from Geocentric::Reverse. */
  double k;
  double
    p = sq(x),
    q = sq(y),
    r = (p + q - 1) / 6;
  if ( !(q == 0 && r <= 0) ) {
    double
      /* Avoid possible division by zero when r = 0 by multiplying equations
       * for s and t by r^3 and r, resp. */
      S = p * q / 4,            /* S = r^3 * s */
      r2 = sq(r),
      r3 = r * r2,
      /* The discriminant of the quadratic equation for T3.  This is zero on
       * the evolute curve p^(1/3)+q^(1/3) = 1 */
      disc = S * (S + 2 * r3);
    double u = r;
    double v, uv, w;
    if (disc >= 0) {
      double T3 = S + r3, T;
      /* Pick the sign on the sqrt to maximize abs(T3).  This minimizes loss
       * of precision due to cancellation.  The result is unchanged because
       * of the way the T is used in definition of u. */
      T3 += T3 < 0 ? -sqrt(disc) : sqrt(disc); /* T3 = (r * t)^3 */
      /* N.B. cbrt always returns the double root.  cbrt(-8) = -2. */
      T = cbrt(T3);            /* T = r * t */
      /* T can be zero; but then r2 / T -> 0. */
      u += T + (T != 0 ? r2 / T : 0);
    } else {
      /* T is complex, but the way u is defined the result is double. */
      double ang = atan2(sqrt(-disc), -(S + r3));
      /* There are three possible cube roots.  We choose the root which
       * avoids cancellation.  Note that disc < 0 implies that r < 0. */
      u += 2 * r * cos(ang / 3);
    }
    v = sqrt(sq(u) + q);              /* guaranteed positive */
    /* Avoid loss of accuracy when u < 0. */
    uv = u < 0 ? q / (v - u) : u + v; /* u+v, guaranteed positive */
    w = (uv - q) / (2 * v);           /* positive? */
    /* Rearrange expression for k to avoid loss of accuracy due to
     * subtraction.  Division by 0 not possible because uv > 0, w >= 0. */
    k = uv / (sqrt(uv + sq(w)) + w);   /* guaranteed positive */
  } else {               /* q == 0 && r <= 0 */
    /* y = 0 with |x| <= 1.  Handle this case directly.
     * for y small, positive root is k = abs(y)/sqrt(1-x^2) */
    k = 0;
  }
  return k;
}

static double InverseStart(const struct Geodesic* g,
                    double sbet1, double cbet1, double dn1,
                    double sbet2, double cbet2, double dn2,
                    double lam12, double slam12, double clam12,
                    double* psalp1, double* pcalp1,
                    /* Only updated if return val >= 0 */
                    double* psalp2, double* pcalp2,
                    /* Only updated for short lines */
                    double* pdnm,
                    /* Scratch area of the right size */
                    double Ca[]) {
  double salp1 = 0, calp1 = 0, salp2 = 0, calp2 = 0, dnm = 0;

  /* Return a starting point for Newton's method in salp1 and calp1 (function
   * value is -1).  If Newton's method doesn't need to be used, return also
   * salp2 and calp2 and function value is sig12. */
  double
    sig12 = -1,               /* Return value */
    /* bet12 = bet2 - bet1 in [0, pi); bet12a = bet2 + bet1 in (-pi, 0] */
    sbet12 = sbet2 * cbet1 - cbet2 * sbet1,
    cbet12 = cbet2 * cbet1 + sbet2 * sbet1;
  double sbet12a;
  boolx shortline = cbet12 >= 0 && sbet12 < 0.5 && cbet2 * lam12 < 0.5;
  double somg12, comg12, ssig12, csig12;
  sbet12a = sbet2 * cbet1 + cbet2 * sbet1;
  if (shortline) {
    double sbetm2 = sq(sbet1 + sbet2), omg12;
    /* sin((bet1+bet2)/2)^2
     * =  (sbet1 + sbet2)^2 / ((sbet1 + sbet2)^2 + (cbet1 + cbet2)^2) */
    sbetm2 /= sbetm2 + sq(cbet1 + cbet2);
    dnm = sqrt(1 + g->ep2 * sbetm2);
    omg12 = lam12 / (g->f1 * dnm);
    somg12 = sin(omg12); comg12 = cos(omg12);
  } else {
    somg12 = slam12; comg12 = clam12;
  }

  salp1 = cbet2 * somg12;
  calp1 = comg12 >= 0 ?
    sbet12 + cbet2 * sbet1 * sq(somg12) / (1 + comg12) :
    sbet12a - cbet2 * sbet1 * sq(somg12) / (1 - comg12);

  ssig12 = hypot(salp1, calp1);
  csig12 = sbet1 * sbet2 + cbet1 * cbet2 * comg12;

  if (shortline && ssig12 < g->etol2) {
    /* really short lines */
    salp2 = cbet1 * somg12;
    calp2 = sbet12 - cbet1 * sbet2 *
      (comg12 >= 0 ? sq(somg12) / (1 + comg12) : 1 - comg12);
    norm2(&salp2, &calp2);
    /* Set return value */
    sig12 = atan2(ssig12, csig12);
  } else if (fabs(g->n) > 0.1 || /* No astroid calc if too eccentric */
             csig12 >= 0 ||
             ssig12 >= 6 * fabs(g->n) * pi * sq(cbet1)) {
    /* Nothing to do, zeroth order spherical approximation is OK */
  } else {
    /* Scale lam12 and bet2 to x, y coordinate system where antipodal point
     * is at origin and singular point is at y = 0, x = -1. */
    double x, y, lamscale, betscale;
    double lam12x = atan2(-slam12, -clam12); /* lam12 - pi */
    if (g->f >= 0) {            /* In fact f == 0 does not get here */
      /* x = dlong, y = dlat */
      {
        double
          k2 = sq(sbet1) * g->ep2,
          eps = k2 / (2 * (1 + sqrt(1 + k2)) + k2);
        lamscale = g->f * cbet1 * A3f(g, eps) * pi;
      }
      betscale = lamscale * cbet1;

      x = lam12x / lamscale;
      y = sbet12a / betscale;
    } else {                    /* f < 0 */
      /* x = dlat, y = dlong */
      double
        cbet12a = cbet2 * cbet1 - sbet2 * sbet1,
        bet12a = atan2(sbet12a, cbet12a);
      double m12b, m0;
      /* In the case of lon12 = 180, this repeats a calculation made in
       * Inverse. */
      Lengths(g->n, pi + bet12a,
              sbet1, -cbet1, dn1, sbet2, cbet2, dn2,
              nullptr, &m12b, &m0, Ca);
      x = -1 + m12b / (cbet1 * cbet2 * m0 * pi);
      betscale = x < -0.01 ? sbet12a / x :
        -g->f * sq(cbet1) * pi;
      lamscale = betscale / cbet1;
      y = lam12x / lamscale;
    }

    if (y > -tol1 && x > -1 - xthresh) {
      /* strip near cut */
      if (g->f >= 0) {
        salp1 = fmin(1.0, -x); calp1 = - sqrt(1 - sq(salp1));
      } else {
        calp1 = fmax(x > -tol1 ? 0.0 : -1.0, x);
        salp1 = sqrt(1 - sq(calp1));
      }
    } else {
      /* Estimate alp1, by solving the astroid problem.
       *
       * Could estimate alpha1 = theta + pi/2, directly, i.e.,
       *   calp1 = y/k; salp1 = -x/(1+k);  for f >= 0
       *   calp1 = x/(1+k); salp1 = -y/k;  for f < 0 (need to check)
       *
       * However, it's better to estimate omg12 from astroid and use
       * spherical formula to compute alp1.  This reduces the mean number of
       * Newton iterations for astroid cases from 2.24 (min 0, max 6) to 2.12
       * (min 0 max 5).  The changes in the number of iterations are as
       * follows:
       *
       * change percent
       *    1       5
       *    0      78
       *   -1      16
       *   -2       0.6
       *   -3       0.04
       *   -4       0.002
       *
       * The histogram of iterations is (m = number of iterations estimating
       * alp1 directly, n = number of iterations estimating via omg12, total
       * number of trials = 148605):
       *
       *  iter    m      n
       *    0   148    186
       *    1 13046  13845
       *    2 93315 102225
       *    3 36189  32341
       *    4  5396      7
       *    5   455      1
       *    6    56      0
       *
       * Because omg12 is near pi, estimate work with omg12a = pi - omg12 */
      double k = Astroid(x, y);
      double
        omg12a = lamscale * ( g->f >= 0 ? -x * k/(1 + k) : -y * (1 + k)/k );
      somg12 = sin(omg12a); comg12 = -cos(omg12a);
      /* Update spherical estimate of alp1 using omg12 instead of lam12 */
      salp1 = cbet2 * somg12;
      calp1 = sbet12a - cbet2 * sbet1 * sq(somg12) / (1 - comg12);
    }
  }
  /* Sanity check on starting guess.  Backwards check allows NaN through. */
  if (!(salp1 <= 0))
    norm2(&salp1, &calp1);
  else {
    salp1 = 1; calp1 = 0;
  }

  *psalp1 = salp1;
  *pcalp1 = calp1;
  if (shortline)
    *pdnm = dnm;
  if (sig12 >= 0) {
    *psalp2 = salp2;
    *pcalp2 = calp2;
  }
  return sig12;
}

static double Lambda12(const struct Geodesic* g,
                double sbet1, double cbet1, double dn1,
                double sbet2, double cbet2, double dn2,
                double salp1, double calp1,
                double slam120, double clam120,
                double* psalp2, double* pcalp2,
                double* psig12,
                double* pssig1, double* pcsig1,
                double* pssig2, double* pcsig2,
                double* peps,
                double* pdomg12,
                boolx diffp, double* pdlam12,
                /* Scratch area of the right size */
                double Ca[]) {
  double salp2 = 0, calp2 = 0, sig12 = 0,
    ssig1 = 0, csig1 = 0, ssig2 = 0, csig2 = 0, eps = 0,
    domg12 = 0, dlam12 = 0;
  double salp0, calp0;
  double somg1, comg1, somg2, comg2, somg12, comg12, lam12;
  double B312, eta, k2;

  if (sbet1 == 0 && calp1 == 0)
    /* Break degeneracy of equatorial line.  This case has already been
     * handled. */
    calp1 = -tiny;

  /* sin(alp1) * cos(bet1) = sin(alp0) */
  salp0 = salp1 * cbet1;
  calp0 = hypot(calp1, salp1 * sbet1); /* calp0 > 0 */

  /* tan(bet1) = tan(sig1) * cos(alp1)
   * tan(omg1) = sin(alp0) * tan(sig1) = tan(omg1)=tan(alp1)*sin(bet1) */
  ssig1 = sbet1; somg1 = salp0 * sbet1;
  csig1 = comg1 = calp1 * cbet1;
  norm2(&ssig1, &csig1);
  /* norm2(&somg1, &comg1); -- don't need to normalize! */

  /* Enforce symmetries in the case abs(bet2) = -bet1.  Need to be careful
   * about this case, since this can yield singularities in the Newton
   * iteration.
   * sin(alp2) * cos(bet2) = sin(alp0) */
  salp2 = cbet2 != cbet1 ? salp0 / cbet2 : salp1;
  /* calp2 = sqrt(1 - sq(salp2))
   *       = sqrt(sq(calp0) - sq(sbet2)) / cbet2
   * and subst for calp0 and rearrange to give (choose positive sqrt
   * to give alp2 in [0, pi/2]). */
  calp2 = cbet2 != cbet1 || fabs(sbet2) != -sbet1 ?
    sqrt(sq(calp1 * cbet1) +
         (cbet1 < -sbet1 ?
          (cbet2 - cbet1) * (cbet1 + cbet2) :
          (sbet1 - sbet2) * (sbet1 + sbet2))) / cbet2 :
    fabs(calp1);
  /* tan(bet2) = tan(sig2) * cos(alp2)
   * tan(omg2) = sin(alp0) * tan(sig2). */
  ssig2 = sbet2; somg2 = salp0 * sbet2;
  csig2 = comg2 = calp2 * cbet2;
  norm2(&ssig2, &csig2);
  /* norm2(&somg2, &comg2); -- don't need to normalize! */

  /* sig12 = sig2 - sig1, limit to [0, pi] */
  sig12 = atan2(fmax(0.0, csig1 * ssig2 - ssig1 * csig2) + 0,
                          csig1 * csig2 + ssig1 * ssig2);

  /* omg12 = omg2 - omg1, limit to [0, pi] */
  somg12 = fmax(0.0, comg1 * somg2 - somg1 * comg2) + 0;
  comg12 =           comg1 * comg2 + somg1 * somg2;
  /* eta = omg12 - lam120 */
  eta = atan2(somg12 * clam120 - comg12 * slam120,
              comg12 * clam120 + somg12 * slam120);
  k2 = sq(calp0) * g->ep2;
  eps = k2 / (2 * (1 + sqrt(1 + k2)) + k2);
  C3f(g, eps, Ca);
  B312 = (SinCosSeries(TRUE, ssig2, csig2, Ca, nC3-1) -
          SinCosSeries(TRUE, ssig1, csig1, Ca, nC3-1));
  domg12 = -g->f * A3f(g, eps) * salp0 * (sig12 + B312);
  lam12 = eta + domg12;

  if (diffp) {
    if (calp2 == 0)
      dlam12 = - 2 * g->f1 * dn1 / sbet1;
    else {
      Lengths(eps, sig12, ssig1, csig1, dn1, ssig2, csig2, dn2,
              nullptr, &dlam12, nullptr, Ca);
      dlam12 *= g->f1 / (calp2 * cbet2);
    }
  }

  *psalp2 = salp2;
  *pcalp2 = calp2;
  *psig12 = sig12;
  *pssig1 = ssig1;
  *pcsig1 = csig1;
  *pssig2 = ssig2;
  *pcsig2 = csig2;
  *peps = eps;
  *pdomg12 = domg12;
  if (diffp)
    *pdlam12 = dlam12;

  return lam12;
}

static double A3f(const struct Geodesic* g, double eps) {
  /* Evaluate A3 */
  return polyvalx(nA3 - 1, g->A3x, eps);
}

static void C3f(const struct Geodesic* g, double eps, double c[]) {
  /* Evaluate C3 coeffs
   * Elements c[1] through c[nC3 - 1] are set */
  double mult = 1;
  int o = 0, l;
  for (l = 1; l < nC3; ++l) {   /* l is index of C3[l] */
    int m = nC3 - l - 1;        /* order of polynomial in eps */
    mult *= eps;
    c[l] = mult * polyvalx(m, g->C3x + o, eps);
    o += m + 1;
  }
}

/* The scale factor A1-1 = mean value of (d/dsigma)I1 - 1 */
static double A1m1f(double eps)  {
  static const double coeff[] = {
    /* (1-eps)*A1-1, polynomial in eps2 of order 3 */
    1, 4, 64, 0, 256,
  };
  int m = nA1/2;
  double t = polyvalx(m, coeff, sq(eps)) / coeff[m + 1];
  return (t + eps) / (1 - eps);
}

/* The coefficients C1[l] in the Fourier expansion of B1 */
static void C1f(double eps, double c[])  {
  static const double coeff[] = {
    /* C1[1]/eps^1, polynomial in eps2 of order 2 */
    -1, 6, -16, 32,
    /* C1[2]/eps^2, polynomial in eps2 of order 2 */
    -9, 64, -128, 2048,
    /* C1[3]/eps^3, polynomial in eps2 of order 1 */
    9, -16, 768,
    /* C1[4]/eps^4, polynomial in eps2 of order 1 */
    3, -5, 512,
    /* C1[5]/eps^5, polynomial in eps2 of order 0 */
    -7, 1280,
    /* C1[6]/eps^6, polynomial in eps2 of order 0 */
    -7, 2048,
  };
  double
    eps2 = sq(eps),
    d = eps;
  int o = 0, l;
  for (l = 1; l <= nC1; ++l) {  /* l is index of C1p[l] */
    int m = (nC1 - l) / 2;      /* order of polynomial in eps^2 */
    c[l] = d * polyvalx(m, coeff + o, eps2) / coeff[o + m + 1];
    o += m + 2;
    d *= eps;
  }
}

/* The coefficients C1p[l] in the Fourier expansion of B1p */
/* The scale factor A2-1 = mean value of (d/dsigma)I2 - 1 */
static double A2m1f(double eps)  {
  static const double coeff[] = {
    /* (eps+1)*A2-1, polynomial in eps2 of order 3 */
    -11, -28, -192, 0, 256,
  };
  int m = nA2/2;
  double t = polyvalx(m, coeff, sq(eps)) / coeff[m + 1];
  return (t - eps) / (1 + eps);
}

/* The coefficients C2[l] in the Fourier expansion of B2 */
static void C2f(double eps, double c[])  {
  static const double coeff[] = {
    /* C2[1]/eps^1, polynomial in eps2 of order 2 */
    1, 2, 16, 32,
    /* C2[2]/eps^2, polynomial in eps2 of order 2 */
    35, 64, 384, 2048,
    /* C2[3]/eps^3, polynomial in eps2 of order 1 */
    15, 80, 768,
    /* C2[4]/eps^4, polynomial in eps2 of order 1 */
    7, 35, 512,
    /* C2[5]/eps^5, polynomial in eps2 of order 0 */
    63, 1280,
    /* C2[6]/eps^6, polynomial in eps2 of order 0 */
    77, 2048,
  };
  double
    eps2 = sq(eps),
    d = eps;
  int o = 0, l;
  for (l = 1; l <= nC2; ++l) { /* l is index of C2[l] */
    int m = (nC2 - l) / 2;     /* order of polynomial in eps^2 */
    c[l] = d * polyvalx(m, coeff + o, eps2) / coeff[o + m + 1];
    o += m + 2;
    d *= eps;
  }
}

/* The scale factor A3 = mean value of (d/dsigma)I3 */
static void A3coeff(struct Geodesic* g) {
  static const double coeff[] = {
    /* A3, coeff of eps^5, polynomial in n of order 0 */
    -3, 128,
    /* A3, coeff of eps^4, polynomial in n of order 1 */
    -2, -3, 64,
    /* A3, coeff of eps^3, polynomial in n of order 2 */
    -1, -3, -1, 16,
    /* A3, coeff of eps^2, polynomial in n of order 2 */
    3, -1, -2, 8,
    /* A3, coeff of eps^1, polynomial in n of order 1 */
    1, -1, 2,
    /* A3, coeff of eps^0, polynomial in n of order 0 */
    1, 1,
  };
  int o = 0, k = 0, j;
  for (j = nA3 - 1; j >= 0; --j) {             /* coeff of eps^j */
    int m = nA3 - j - 1 < j ? nA3 - j - 1 : j; /* order of polynomial in n */
    g->A3x[k++] = polyvalx(m, coeff + o, g->n) / coeff[o + m + 1];
    o += m + 2;
  }
}

/* The coefficients C3[l] in the Fourier expansion of B3 */
static void C3coeff(struct Geodesic* g) {
  static const double coeff[] = {
    /* C3[1], coeff of eps^5, polynomial in n of order 0 */
    3, 128,
    /* C3[1], coeff of eps^4, polynomial in n of order 1 */
    2, 5, 128,
    /* C3[1], coeff of eps^3, polynomial in n of order 2 */
    -1, 3, 3, 64,
    /* C3[1], coeff of eps^2, polynomial in n of order 2 */
    -1, 0, 1, 8,
    /* C3[1], coeff of eps^1, polynomial in n of order 1 */
    -1, 1, 4,
    /* C3[2], coeff of eps^5, polynomial in n of order 0 */
    5, 256,
    /* C3[2], coeff of eps^4, polynomial in n of order 1 */
    1, 3, 128,
    /* C3[2], coeff of eps^3, polynomial in n of order 2 */
    -3, -2, 3, 64,
    /* C3[2], coeff of eps^2, polynomial in n of order 2 */
    1, -3, 2, 32,
    /* C3[3], coeff of eps^5, polynomial in n of order 0 */
    7, 512,
    /* C3[3], coeff of eps^4, polynomial in n of order 1 */
    -10, 9, 384,
    /* C3[3], coeff of eps^3, polynomial in n of order 2 */
    5, -9, 5, 192,
    /* C3[4], coeff of eps^5, polynomial in n of order 0 */
    7, 512,
    /* C3[4], coeff of eps^4, polynomial in n of order 1 */
    -14, 7, 512,
    /* C3[5], coeff of eps^5, polynomial in n of order 0 */
    21, 2560,
  };
  int o = 0, k = 0, l, j;
  for (l = 1; l < nC3; ++l) {                    /* l is index of C3[l] */
    for (j = nC3 - 1; j >= l; --j) {             /* coeff of eps^j */
      int m = nC3 - j - 1 < j ? nC3 - j - 1 : j; /* order of polynomial in n */
      g->C3x[k++] = polyvalx(m, coeff + o, g->n) / coeff[o + m + 1];
      o += m + 2;
    }
  }
}

// clang-format on

} // namespace

auto wgs84_distance(double lon1, double lat1, double lon2, double lat2)
  -> double {
  static auto const geodesic = [] {
    auto result = Geodesic{};
    init_geodesic(&result, 6'378'137.0, 1.0 / 298.257223563);
    return result;
  }();
  return geod_geninverse_int(&geodesic, lat1, lon1, lat2, lon2);
}

} // namespace tenzir::detail
