

(define (int2-compatible? x)
  (and (integer? x)
       (>= x -32768)
       (<= x 32767)))

(define (int4-compatible? x)
  (and (integer? x)
       (>= x -2147483648)
       (<= x 2147483647)))

(define (int8-compatible? x)
  (and (integer? x)
       (>= x -9223372036854775808)
       (<= x 9223372036854775807)))

(define (float4-compatible? x)
  (and (real? x)
       (inexact? x)))

(define (float8-compatible? x)
  (and (real? x)
       (inexact? x)))
