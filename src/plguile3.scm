;; base for all modules -- system, public, and private -- that
;; can be imported into scheme code
(define-module (trusted))

(define-module (plguile3 base)
  #:use-module (ice-9 sandbox)
  #:use-module (srfi srfi-1)  ; fold-right
  #:use-module (srfi srfi-9)  ; define-record-type
  #:use-module (srfi srfi-11) ; let-values
  #:use-module (srfi srfi-19) ; date, time, etc (used in plguile3.c)
  #:export (%cursor-open
            %execute
            %execute-with-receiver
            %fetch
            %move
            notice
            stop-command-execution
            unbox-datum
            warning
            start-transaction
            commit
            commit-and-chain
            rollback
            rollback-and-chain

            execute
            cursor-open
            fetch

            make-boxed-datum
            boxed-datum?
            boxed-datum-type
            boxed-datum-value

            make-decimal
            decimal?
            decimal-digits
            decimal-scale

            decimal->inexact
            decimal->string
            string->decimal
            valid-decimal?

            make-record
            record?
            record-attr-names
            record-attr-names-hash
            record-types

            attr-name->number
            record-ref
            record-set!

            make-table
            table?
            table-attr-names
            table-attr-names-hash
            table-rows
            table-types

            table-row
            table-length
            table-width
            scalar

            make-point
            point?
            point-x
            point-y

            make-line
            line?
            line-a
            line-b
            line-c

            make-lseg
            lseg?
            lseg-a
            lseg-b

            make-box
            box?
            box-a
            box-b

            make-path
            path?
            path-closed?
            path-points

            make-polygon
            polygon?
            polygon-boundbox
            polygon-points

            make-circle
            circle?
            circle-center
            circle-radius

            make-inet
            inet?
            inet-family
            inet-bits
            inet-address

            make-macaddr
            macaddr?
            macaddr-data

            make-macaddr8
            macaddr8?
            macaddr8-data

            make-bit-string
            bit-string?
            bit-string-data
            bit-string-length

            make-tsposition
            tsposition?
            tsposition-index
            tsposition-weight

            make-tslexeme
            tslexeme?
            tslexeme-lexeme
            tslexeme-positions

            make-tsvector
            tsvector?
            tsvector-lexemes

            make-tsquery
            tsquery?
            tsquery-ast

            make-jsonpath
            jsonpath?
            jsonpath-strict?
            jsonpath-expr

            make-range
            range?
            range-lower
            range-upper
            range-flags

            make-multirange
            multirange?
            multirange-ranges

            make-jsonb
            jsonb?
            jsonb-expr

            make-cursor
            cursor?
            cursor-name))

(define (flush-function-cache h ids)
  (let ((new (make-hash-table)))
    (hash-for-each
     (if ids
         (lambda (key value)
           (unless (member key ids)
             (hash-set! new key value))
           new)
         (lambda (key value)
            (hash-set! new key (cons (car value) #f))))
     h)
    new))

(define hash/role-id->func-oids (make-hash-table))

(define (role->func-oids role-id)
  (hash-ref hash/role-id->func-oids role-id '()))

(define (role-add-func-oid! role-id func-oid)
  (let ((func-oids (role->func-oids role-id)))
    (unless (member func-oid func-oids)
      (hash-set! hash/role-id->func-oids role-id (cons func-oid func-oids)))))

(define (role-remove-func-oid! role-id func-oid)
  (let ((func-oids (role->func-oids role-id)))
    (when (member func-oid func-oids)
      (hash-set! hash/role-id->func-oids role-id (delete func-oid func-oids)))))

(define* (execute command
                  #:optional (args '())
                  #:key (count 0) receiver)
  (if receiver
      (%execute-with-receiver receiver command args count)
      (%execute command args count)))

(define* (cursor-open command
                      #:optional (args '())
                      #:key (hold #f) (name #f) (scroll '()))
  (%cursor-open command args count hold name scroll))

(define* (fetch cursor #:optional direction count)
  (if count
      (if (memq direction '(all first last next prior))
          (raise-exception `(fetch-direction-takes-no-count ,direction))
          (%fetch cursor direction count))
      (case direction
        ((absolute relative)
         (raise-exception `(fetch-direction-requires-count ,direction)))

        ((all)
         (%fetch cursor 'forward 'all))

        ((backward)
         (%fetch cursor 'backward 1))

        ((first)
         (%fetch cursor 'absolute 1))

        ((forward)
         (%fetch cursor 'forward 1))

        ((last)
         (%fetch cursor 'absolute -1))

        ((#f next)
         (%fetch cursor 'forward 1))

        ((prior)
         (%fetch cursor 'backward 1))

        (else
         (if (number? direction)
             (%fetch cursor 'forward direction)
             (raise-exception `(fetch-unknown-direction ,direction)))))))

(define-record-type boxed-datum
  ; the type is an Oid, the value is a Datum.
  (make-boxed-datum type value)
  boxed-datum?
  (type boxed-datum-type)
  (value boxed-datum-value))

(define-record-type record
  (make-record types attrs attr-names attr-names-hash)
  record?
  (attrs record-attrs)
  (attr-names record-attr-names)
  (attr-names-hash record-attr-names-hash)
  (types record-types))

(define-record-type table
  (make-table types records attr-names attr-names-hash)
  table?
  (attr-names table-attr-names)
  (attr-names-hash table-attr-names-hash)
  (records table-rows)
  (types table-types))

(define (attr-name->number r k)
  (or (hash-ref (record-attr-names-hash r) k)
      (raise-exception `(unknown-attr-name ,k))))

(define (record-ref r k)
  (let ((i (if (number? k)
               k
               (attr-name->number r k))))
    (vector-ref (record-attrs r) i)))

(define (record-set! r k x)
  (let ((i (if (number? k)
               k
               (attr-name->number r k))))
    (vector-set! (record-attrs r) i x)))

(define (table-row t row)
  (list-ref (table-rows t) row))

(define (table-length t)
  (length (table-rows t)))

(define (table-width t)
  (length (table-types t)))

(define (scalar t)
  (if (or (< 1 (table-length t)) (< 1 (table-width t)))
      (raise-exception `(non-scalar-result #:length ,(table-length t) #:width ,(table-width t))))
  (record-ref (table-row t 0) 0))

(define-record-type decimal
  (%make-decimal digits scale)
  decimal?
  (digits decimal-digits)
  (scale decimal-scale))

(define (make-decimal digits scale)
  (if (or
       (equal? digits "NaN")
       (equal? digits "Infinity")
       (equal? digits "-Infinity")
       (and (number? digits)
            (exact-integer? digits)
            (number? scale)
            (exact-integer? scale)
            (>= scale 0)))
      (%make-decimal digits scale)
      (raise-exception `(invalid-decimal #:digits ,digits #:scale ,scale) #:continuable? #t)))

(define (string->decimal s)
  (cond
   ((string=? s "NaN")       (make-decimal s 0))
   ((string=? s "Infinity")  (make-decimal s 0))
   ((string=? s "-Infinity") (make-decimal s 0))
   (else
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
                    scale)))))

(define (decimal->string d)
  (let ((digits (decimal-digits d))
        (scale (decimal-scale d)))
    (cond
     ((equal? digits "NaN")       digits)
     ((equal? digits "Infinity")  digits)
     ((equal? digits "-Infinity") digits)
     ((zero? scale) (number->string digits))
     (else
      (let-values (((integer-part fractional-part) (floor/ digits (expt 10 scale))))
        (string-join (list (number->string integer-part)
                           "."
                           (let ((f (number->string fractional-part)))
                             (string-join (list (make-string (- scale (string-length f)) #\0)
                                                f)
                                          "")))
                     ""))))))

(define (decimal->inexact d)
  (let ((digits (decimal-digits d))
        (scale (decimal-scale d)))
    (cond
     ((equal? digits "NaN")       +nan.0)
     ((equal? digits "Infinity")  +inf.0)
     ((equal? digits "-Infinity") -inf.0)
     (else
      (/ digits (expt 10.0 scale))))))

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

(define-record-type tsposition
  (make-tsposition index weight)
  tsposition?
  (index tsposition-index)
  (weight tsposition-weight))

(define-record-type tslexeme
  (make-tslexeme lexeme positions)
  tslexeme?
  (lexeme tslexeme-lexeme)
  (positions tslexeme-positions))

(define-record-type tsvector
  (make-tsvector lexemes)
  tsvector?
  (lexemes tsvector-lexemes))

(define-record-type tsquery
  (make-tsquery ast)
  tsquery?
  (ast tsquery-ast))

(define (normalize-tsvector v)
  (make-tsvector (merge-lexemes (tsvector-lexemes v))))

(define (merge-lexemes ls)
  (map normalize-tslexeme
       (fold-right (lambda (current result)
                     (if (null? result)
                         (list current)
                         (if (string=? (tslexeme-lexeme current) (tslexeme-lexeme (car result)))
                             (cons (merge-tslexemes current (car result)) (cdr result))
                             (cons current result))))
                   '()
                   (sort ls tslexeme<?))))

(define (tslexeme<? a b)
  (string<? (tslexeme-lexeme a) (tslexeme-lexeme b)))

(define (merge-tslexemes a b)
  (make-tslexeme (tslexeme-lexeme a)
                 (append (tslexeme-positions a) (tslexeme-positions b))))

(define (normalize-tslexeme l)
  (make-tslexeme (tslexeme-lexeme l)
                 (merge-positions (tslexeme-positions l))))

(define (merge-positions ps)
  (fold-right (lambda (current result)
                (if (null? result)
                    (list current)
                    (if (= (tsposition-index current) (tsposition-index (car result)))
                        result
                        (cons current result))))
              '()
              (sort ps tsposition<?)))

(define (tsposition<? a b)
  (let ((index-a (tsposition-index a))
        (index-b (tsposition-index b)))
    (or (< index-a index-b)
      (and (= index-a index-b)
           (< (tsposition-weight a) (tsposition-weight b))))))

(define-record-type tsquery
  (make-tsquery expr)
  tsquery?
  (expr tsquery-expr))

(define (validate-tsquery q)
  (validate-tsquery-expr (tsquery-expr q)))

(define (validate-tsquery-expr e)
  (if (not (pair? e))
      (raise-exception `(invalid-tsquery-expr ,e))
      (case (car e)
        ((value) (unless (and (= 4 (length e))
                               (string? (cadr e))
                               (exact? (caddr e))
                               (<= 0 (caddr e) 15)
                               (boolean? (cadddr e)))
                    (raise-exception `(invalid-tsquery-value-expr ,e))))
        ((not) (if (not (= 2 (length e)))
                    (raise-exception `(invalid-tsquery-not-expr ,e))
                    (validate-tsquery-expr (cadr e))))
        ((and or) (if (not (= 3 (length e)))
                       (raise-exception `(invalid-tsquery-binop-expr ,e))
                       (begin
                         (validate-tsquery-expr (cadr e))
                         (validate-tsquery-expr (caddr e)))))
        ((phrase) (if (not (and (= 4 (length e))
                                 (exact? (cadddr e))
                                 (<= 0 (cadddr e))))
                       (raise-exception `(invalid-tsquery-phrase-expr ,e))
                       (begin
                         (validate-tsquery-expr (cadr e))
                         (validate-tsquery-expr (caddr e))))))))

(define-record-type jsonpath
  (make-jsonpath strict? expr)
  jsonpath?
  (strict? jsonpath-strict?)
  (expr jsonpath-expr))

(define (validate-jsonpath jsp)
  ;; returns #t if the path is valid, otherwise a pair describing
  ;; the way in which it fails.

  (validate-jsonpath-expr (jsonpath-expr jsp) validate-jsonpath-expr))

(define (validate-jsonpath-expr e validate-expr)
  (cond
   ((or (null? e) (boolean? e) (decimal? e) (string? e))
    #t)

   ((pair? e)
    (let ((len (length e))
          (op (car e)))
      (case op
        ((root)
         (or (= 1 len)
             (raise-exception `(jsonpath-invalid-root-expr ,e))))

        ((abs any-array any-key ceiling double exists floor keyvalue negate nop size type unknown?)
         (or (and (= 2 len)
                  (validate-expr (list-ref e 1) validate-expr))
             (raise-exception `(jsonpath-invalid-unary-expr ,e))))

        ((+ -)
         (case len
           ((2)
            (validate-expr (list-ref e 1) validate-expr))

           ((3)
            (and
             (validate-expr (list-ref e 1) validate-expr)
             (validate-expr (list-ref e 2) validate-expr)))

           (else
            (raise-exception `(jsonpath-invalid-unary-or-binary-expr ,e)))))

        ((* / % = != > >= < <= and or starts-with)
         (or (and (= 3 len)
                  (validate-expr (list-ref e 1) validate-expr)
                  (validate-expr (list-ref e 2) validate-expr))
             (raise-exception `(jsonpath-invalid-binary-expr ,e))))

        ((any)
         (or (and (= 4 len)
                  (validate-jsonpath-any-bounds (list-ref e 1))
                  (validate-jsonpath-any-bounds (list-ref e 2))
                  (validate-expr (list-ref e 3) validate-expr))
             (raise-exception `(jsonpath-invalid-any-expr ,e))))

        ((datetime)
         (case len
           ((2)
            (validate-expr (list-ref e 1) validate-expr))

           ((3)
            (and
             (validate-expr (list-ref e 1) validate-expr)
             (validate-expr (list-ref e 2) validate-expr)))

           (else
            (raise-exception `(jsonpath-invalid-datetime-expr ,e)))))

        ((filter)
         (or (and (= 3 len)
                  (validate-jsonpath-filter-expr (list-ref e 1) validate-expr)
                  (validate-expr (list-ref e 2) validate-expr))
             (raise-exception `(jsonpath-invalid-filter-expr ,e))))

        ((index-array)
         (or (and (= 3 len)
                  (validate-jsonpath-index-exprs (list-ref e 1) validate-expr)
                  (validate-expr (list-ref e 2) validate-expr))
             (raise-exception `(jsonpath-invalid-index-array-expr ,e))))

        ((key)
         (or (and (= 3 len)
                  (string? (list-ref e 1))
                  (validate-expr (list-ref e 2) validate-expr))
             (raise-exception `(jsonpath-invalid-key-expr ,e))))

        ((like-regex)
         (or (and (= 4 len)
                  (validate-expr (list-ref e 1) validate-expr)
                  (string? (list-ref e 2))
                  (validate-jsonpath-regex-flags (list-ref e 3)))
             (raise-exception `(jsonpath-invalid-like-regex-expr ,e))))

        ((var)
         (or (and (= 2 len)
                  (string? (list-ref e 1)))
             (raise-exception `(jsonpath-invalid-string-expr ,e))))

        (else
         (raise-exception `(jsonpath-invalid-expr-unknown-op ,e))))))

   (else
    `(unknown-jsonpath-expr ,e))))

(define (validate-jsonpath-any-bounds e)
  (or (not e)
      (int4-compatible? e)
      (raise-exception `(jsonpath-invalid-any-bounds ,e))))

(define (validate-jsonpath-filter-expr e validate-expr)
  (letrec ((validate (lambda (e _)
                       (or (equal? e '(@))
                           (validate-expr e validate)))))
    (validate e #f)))

(define (validate-jsonpath-index-exprs exprs validate-expr)
  (or (and (pair? exprs)
           (not (null? exprs))
           (letrec ((validate (lambda (e _)
                                (or (eq? e '(last))
                                    (validate-expr e validate)))))
             (let loop ((es exprs))
               (or (null? es)
                   (and (pair? es)
                        (validate (list-ref (car es) 0) #f)
                        (validate (list-ref (car es) 1) #f)
                        (loop (cdr es)))))))

      (raise-exception `(jsonpath-invalid-index-exprs ,exprs))))

(define (validate-jsonpath-regex-flags flags)
  (or (null? flags)
      (and (pair? flags)
           (let loop ((fs flags))
             (and (pair? fs)
                  (case (car fs)
                    ((dot-matches-newline
                      ignore-case
                      multi-line
                      literal
                      whitespace)
                     #t)
                    (else
                     (raise-exception `(jsonpath-invalid-regex-flag ,(car fs)))))
                  (loop (cdr fs)))
             #t))

      (raise-exception `(jsonpath-invalid-regex-flags ,flags))))

(define-record-type range
  (make-range lower upper flags)
  range?
  (lower range-lower)
  (upper range-upper)
  (flags range-flags))

(define-record-type multirange
  (make-multirange ranges)
  multirange?
  (ranges multirange-ranges))

(define-record-type jsonb
  (make-jsonb expr)
  jsonb?
  (expr jsonb-expr))

(define-record-type cursor
  (make-cursor name)
  cursor?
  (name cursor-name))

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

(define (apply-with-limits proc args time-limit allocation-limit)
  (let ((thunk (lambda () (apply proc args))))
    (call-with-time-and-allocation-limits time-limit allocation-limit thunk)))

(define-syntax define-public-module
  (syntax-rules ()
    ((define-public-module exp ...)
     (begin
       (%prepare-public-module-definition)
       (define-module exp ...)))))

(define original-define-module* define-module*)

(define (define-trusted-module* name . args)
  (let* ((qualified-name (%begin-define-module name))
         (processed-args (let loop ((args args)
                                    (result (list qualified-name)))
                           (if (null? args)
                               (reverse result)
                               (if (eq? #:use-module (car args))
                                   (loop (cddr args)
                                         (cons (%resolve-use-module-name (cadr args))
                                               (cons #:use-module result)))
                                   (loop (cddr args)
                                         (cons (cadr args)
                                               (cons (car args)
                                                     result))))))))
    (apply original-define-module* processed-args)))

(define (eval-with-limits exp module time-limit allocation-limit)
  (dynamic-wind
    (lambda ()
      (set! (@ (guile) define-module*) define-trusted-module*))
    (lambda ()
      (eval-in-sandbox exp
                       #:time-limit time-limit
                       #:allocation-limit allocation-limit
                       #:module module
                       #:sever-module? #f))
    (lambda ()
      (set! (@ (guile) define-module*) original-define-module*))))

(define (untrusted-eval exp module)
  (eval-in-sandbox exp
                   #:module module
                   #:sever-module? #f))

(define exception-bindings
  '(()))

(define bytevector-bindings
  '(((guile)
     string-utf8-length)
    ((scheme base)
     bytevector
     bytevector-copy
     bytevector-copy!
     bytevector-append)
    ((rnrs bytevectors)
     make-bytevector
     bytevector?
     bytevector-length
     bytevector=?
     bytevector-fill!
     bytevector-uint-ref
     bytevector-sint-ref
     bytevector-uint-set!
     bytevector-sint-set!
     bytevector-u8-ref
     bytevector-s8-ref
     bytevector-u16-ref
     bytevector-s16-ref
     bytevector-u32-ref
     bytevector-s32-ref
     bytevector-u64-ref
     bytevector-s64-ref
     bytevector-uint-ref
     bytevector-sint-ref
     bytevector-u8-set!
     bytevector-s8-set!
     bytevector-u16-set!
     bytevector-s16-set!
     bytevector-u32-set!
     bytevector-s32-set!
     bytevector-u64-set!
     bytevector-s64-set!
     bytevector-u16-native-ref
     bytevector-s16-native-ref
     bytevector-u32-native-ref
     bytevector-s32-native-ref
     bytevector-u64-native-ref
     bytevector-s64-native-ref
     bytevector-u16-native-set!
     bytevector-s16-native-set!
     bytevector-u32-native-set!
     bytevector-s32-native-set!
     bytevector-u64-native-set!
     bytevector-s64-native-set!
     bytevector->u8-list
     u8-list->bytevector
     bytevector->uint-list
     bytevector->sint-list
     uint-list->bytevector
     sint-list->bytevector
     bytevector-ieee-single-ref
     bytevector-ieee-double-ref
     bytevector-ieee-single-set!
     bytevector-ieee-double-set!
     bytevector-ieee-single-native-ref
     bytevector-ieee-double-native-ref
     bytevector-ieee-single-native-set!
     bytevector-ieee-double-native-set!
     string->utf8
     string->utf16
     string->utf32
     utf8->string
     utf16->string
     utf32->string)))

(define srfi-9-bindings
  '(((srfi srfi-9)
     define-record-type)))

(define srfi-19-bindings
  '(((srfi srfi-19)
     time-utc
     time-tai
     time-monotonic
     time-duration
     time?
     make-time
     time-type
     time-nanosecond
     time-second
     set-time-type!
     set-time-nanosecond!
     set-time-second!
     copy-time
     current-time
     time-resolution
     time<=?
     time<?
     time=?
     time>=?
     time>?
     time-difference
     time-difference!
     add-duration
     add-duration!
     subtract-duration
     subtract-duration!
     date?
     make-date
     date-nanosecond
     date-second
     date-minute
     date-hour
     date-day
     date-month
     date-year
     date-zone-offset
     date-year-day
     date-week-day
     date-week-number
     current-date
     current-julian-day
     current-modified-julian-day
     date->julian-day
     date->modified-julian-day
     date->time-monotonic
     date->time-tai
     date->time-utc
     julian-day->date
     julian-day->time-monotonic
     julian-day->time-tai
     julian-day->time-utc
     modified-julian-day->date
     modified-julian-day->time-monotonic
     modified-julian-day->time-tai
     modified-julian-day->time-utc
     time-monotonic->date
     time-monotonic->time-tai
     time-monotonic->time-tai!
     time-monotonic->time-utc
     time-monotonic->time-utc!
     time-tai->date
     time-tai->julian-day
     time-tai->modified-julian-day
     time-tai->time-monotonic
     time-tai->time-monotonic!
     time-tai->time-utc
     time-tai->time-utc!
     time-utc->date
     time-utc->julian-day
     time-utc->modified-julian-day
     time-utc->time-monotonic
     time-utc->time-monotonic!
     time-utc->time-tai
     time-utc->time-tai!
     date->string
     string->date)))

(define srfi-43-bindings
  '(((srfi srfi-43)
     reverse-list->vector
     reverse-vector->list
     vector-any
     vector-append
     vector-binary-search
     vector-concatenate
     vector-count
     vector-empty?
     vector-every
     vector-fold
     vector-fold-right
     vector-for-each
     vector-index
     vector-index-right
     vector-map
     vector-map!
     vector-reverse!
     vector-reverse-copy
     vector-reverse-copy!
     vector-skip
     vector-skip-right
     vector-swap!
     vector-unfold
     vector-unfold-right
     vector=)))

(define plguile3-bindings
  '(((guile)
     with-exception-handler)
    ((plguile3 base)
     %cursor-open
     %execute
     %execute-with-receiver
     %fetch
     %move
     notice
     stop-command-execution
     unbox-datum
     warning
     start-transaction
     commit
     commit-and-chain
     rollback
     rollback-and-chain

     execute
     cursor-open
     fetch

     make-boxed-datum
     boxed-datum?
     boxed-datum-type
     boxed-datum-value

     make-decimal
     decimal?
     decimal-digits
     decimal-scale

     decimal->inexact
     decimal->string
     string->decimal
     valid-decimal?

     make-record
     record?
     record-attr-names
     record-attr-names-hash
     record-types

     attr-name->number
     record-ref
     record-set!

     make-table
     table?
     table-attr-names
     table-attr-names-hash
     table-rows
     table-types

     table-row
     table-length
     table-width
     scalar

     make-point
     point?
     point-x
     point-y

     make-line
     line?
     line-a
     line-b
     line-c

     make-lseg
     lseg?
     lseg-a
     lseg-b

     make-box
     box?
     box-a
     box-b

     make-path
     path?
     path-closed?
     path-points

     make-polygon
     polygon?
     polygon-boundbox
     polygon-points

     make-circle
     circle?
     circle-center
     circle-radius

     make-inet
     inet?
     inet-family
     inet-bits
     inet-address

     make-macaddr
     macaddr?
     macaddr-data

     make-macaddr8
     macaddr8?
     macaddr8-data

     make-bit-string
     bit-string?
     bit-string-data
     bit-string-length

     make-tsposition
     tsposition?
     tsposition-index
     tsposition-weight

     make-tslexeme
     tslexeme?
     tslexeme-lexeme
     tslexeme-positions

     make-tsvector
     tsvector?
     tsvector-lexemes

     make-tsquery
     tsquery?
     tsquery-ast

     make-jsonpath
     jsonpath?
     jsonpath-strict?
     jsonpath-expr

     make-range
     range?
     range-lower
     range-upper
     range-flags

     make-multirange
     multirange?
     multirange-ranges

     make-jsonb
     jsonb?
     jsonb-expr

     make-cursor
     cursor?
     cursor-name)))

(define trusted-bindings
  (append all-pure-and-impure-bindings
          bytevector-bindings
          srfi-9-bindings  ; define-record-type
          srfi-19-bindings ; dates and times
          srfi-43-bindings ; vectors
          plguile3-bindings))
