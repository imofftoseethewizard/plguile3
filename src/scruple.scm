(use-modules
 (srfi srfi-9)  ; define-record-type
 (srfi srfi-11) ; let-values
 (srfi srfi-19) ; date, time, etc (used in scruple.c)
 (rnrs bytevectors))

(define-record-type decimal
  (make-decimal digits scale)
  decimal?
  (digits decimal-digits)
  (scale decimal-scale))

(define (string->decimal s)
  (let* ((dot-index (string-index s #\.))
         (length (string-length s))
         (decimal-point (or dot-index length))
         (scale (if dot-index
                             (- length decimal-point 1)
                             0))
         (integer-part (if (zero? decimal-point)
                           0
                           (string->number (substring/shared s 0 decimal-point))))
         (fractional-part (if (zero? scale)
                              0
                              (string->number (substring/shared s (+ decimal-point 1) length)))))
    (make-decimal (+ (* integer-part (expt 10 scale)) fractional-part)
                  scale)))

(define (decimal->string d)
  (let ((scale (decimal-scale d)))
    (if (zero? scale)
        (number->string (decimal-digits d))
        (let-values (((integer-part fractional-part) (floor/ (decimal-digits d) (expt 10 scale))))
          (format #f "~d.~v,'0d" integer-part scale fractional-part)))))

(define-record-type point
  (make-point x y)
  point?
  (x point-x)
  (y point-y))

(define-record-type line
  (make-line a b c)
  line?
  (a line-a)
  (b line-b)
  (c line-c))

(define-record-type lseg
  (make-lseg a b)
  lseg?
  (a lseg-a)
  (b lseg-b))

(define-record-type box
  (make-box a b)
  box?
  (a box-a)
  (b box-b))

(define-record-type path
  (make-path closed? points)
  path?
  (closed? path-closed?)
  (points path-points))

(define-record-type polygon
  (make-polygon boundbox points)
  polygon?
  (boundbox polygon-boundbox)
  (points polygon-points))

(define-record-type circle
  (make-circle center radius)
  circle?
  (center circle-center)
  (radius circle-radius))

(define-record-type inet
  (make-inet family bits address)
  inet?
  (family inet-family)
  (bits inet-bits)
  (address inet-address))

(define-record-type macaddr
  (make-macaddr data)
  macaddr?
  (data macaddr-data))

(define-record-type macaddr8
  (make-macaddr8 data)
  macaddr8?
  (data macaddr8-data))

(define-record-type bit-string
  (make-bit-string data length)
  bit-string?
  (data bit-string-data)
  (length bit-string-length))

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
