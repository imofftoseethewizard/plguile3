;; base for all modules -- system, public, and private -- that
;; can be imported into scheme code
(define-module (trusted))

(define-module (plguile3 base)
  #:use-module (plguile3 primitives)
  #:use-module (ice-9 hash-table)
  #:use-module (ice-9 match)
  #:use-module (ice-9 sandbox)
  #:use-module (rnrs io ports)
  #:use-module (srfi srfi-1)  ; fold-right
  #:use-module (srfi srfi-9)  ; define-record-type
  #:use-module (srfi srfi-11) ; let-values
  #:use-module (srfi srfi-19) ; date, time, etc (used in plguile3.c)
  #:re-export (%cursor-open
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
               rollback-and-chain)

  #:export ((safe-@ . @)
            define-public-module
            re-export-curated-builtin-module
            module-copy

            to-string

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

(define (to-string x)
  (format #f "~s" x))

(define original-resolve-module resolve-module)

(define try-load-trusted-module
  (let ((try-load-module try-load-module))
    (lambda (name version)
      (if (eq? (car name) 'trusted)
          (let ((m (resolve-module name #f version #:ensure #f)))
            (notice (format #f "~s" (list 'try-load-trusted-module name version)))
            (notice (format #f "resolved: ~s" m))
            (when m
              (notice (format #f "public-i: ~s" (module-public-interface m))))
            (or m (%load-trusted-module name)))
          (try-load-module name version)))))

(set! (@@ (guile) try-load-module) try-load-trusted-module)

(define-syntax-rule (safe-@ module name)
  (let ((resolved-name (%resolve-trusted-module-name 'module)))
    (unless resolved-name
      (error (format #f "Module named ~s does not exist" 'module)))
    (let* ((m (resolve-module resolved-name #f))
           (public-i (module-public-interface m))
           (variable (module-variable public-i 'name)))
      (unless (and variable (variable-bound? variable))
        (error "No variable named" 'name 'in 'module))
      (variable-ref variable))))

(define (unload-trusted-modules)
  (notice "unloading trusted modules")
  (set-module-submodules! (resolve-module '(trusted) #f #f #:ensure #f) (make-hash-table)))

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
  (parameterize ((current-input-port   default-input-port)
                 (current-output-port  default-output-port)
                 (current-error-port   default-error-port)
                 (current-warning-port default-warning-port))
    (let ((thunk (lambda () (apply proc args))))
      (call-with-time-and-allocation-limits time-limit allocation-limit thunk))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; Output Ports
;;

(define (make-log-output-port level fmt)
  (let* ((port-id (string-append "log-" (symbol->string level) "-port"))
         (output-handler
         (case level
           ((info) notice)
           ((warning error) warning)
           (else
            (error "make-log-output-port: level must be one of 'info, 'warning, or 'error." ))))
         (write! (lambda (str idx count)
                    (output-handler (format #f fmt (substring str idx (+ idx count))))
                    count)))
    (make-custom-textual-output-port port-id write! #f #f #f)))

(define default-input-port   (%make-void-port OPEN_READ))
(define default-output-port  (make-log-output-port 'info    "~s"))
(define default-warning-port (make-log-output-port 'warning "~s"))
(define default-error-port   (make-log-output-port 'error   "plguile3 ERROR: ~s"))

(define-syntax define-public-module
  (syntax-rules ()
    ((define-public-module exp ...)
     (begin
       (%prepare-public-module-definition)
       (define-module exp ...)))))

(define-syntax with-module-definer
  (syntax-rules ()
    ((define-public-module module-definer exp ...)
     (let ((prior-define-module* (@ (guile) define-module*)))
      (dynamic-wind
        (lambda () (set! (@ (guile) define-module*) module-definer))
        (lambda () exp ...)
        (lambda () (set! (@ (guile) define-module*) prior-define-module*)))))))

(define original-define-module* define-module*)

(define (define-trusted-module* name . args)
  (let* ((qualified-name (%begin-define-module name))
         (processed-args (let loop ((args args)
                                    (imports '())
                                    (result (list qualified-name)))
                           (if (null? args)
                               (append (reverse result)
                                       (list #:imports imports #:pure #t))
                               (case (car args)

                                 ;; resolve (foo bar) to (trusted <role-id> foo bar) or
                                 ;; (trusted public foo bar) or throw an error

                                 ((#:imports)
                                  (loop (cddr args)
                                        (%resolve-import-specs (cadr args))
                                        result))

                                 ;; ignore #:pure and #:declarative?
                                 ((#:pure #:declarative?)
                                  (loop (cddr args) imports result))

                                 (else
                                  (loop (cddr args)
                                        imports
                                        (cons (cadr args)
                                              (cons (car args)
                                                    result)))))))))

    (with-module-definer original-define-module*
      (let ((m (apply original-define-module* processed-args)))
        (module-use-interfaces! m sandbox-iface-specs)
        (notice (format #f "resolve-module post define: ~a" (resolve-module qualified-name #f #f #:ensure #f)))
        (notice (format #f "resolve-module post define: itf ~a" (module-public-interface (resolve-module qualified-name #f #f #:ensure #f))))
                                        ;(notice (format #f "resolve-module post define: v ~a" (module-version (resolve-module qualified-name #f #f #:ensure #f))))
        m))))

(define (eval-with-limits exp time-limit allocation-limit)
  (parameterize ((current-input-port   default-input-port)
                 (current-output-port  default-output-port)
                 (current-error-port   default-error-port)
                 (current-warning-port default-warning-port))
    (with-module-definer define-trusted-module*
      (call-with-time-and-allocation-limits time-limit
                                            allocation-limit
                                            (lambda () (primitive-eval exp))))))

(define (untrusted-eval exp module)
  (eval-in-sandbox exp
                   #:module module
                   #:sever-module? #f))

(define (hash-table-copy h)
  (let ((result (make-hash-table 0)))
    (hash-for-each (lambda (k v)
                     (hashq-set! result k v))
                   h)
    result))

(define (module-copy m)
  (module-constructor (hash-table-copy (module-obarray m))
                      (module-uses m)
                      (module-binder m)
                      ;; declarative
                      #f
                      macroexpand
                      ;; name
                      #f
                      (module-kind m)
                      ;; duplicate-handlers
                      #f
                      (hash-table-copy (module-import-obarray m))
                      ;; observers
                      '()
                      ;; weak observers
                      (make-weak-key-hash-table)
                      ;; version
                      #f
                      (hash-table-copy (module-submodules m))
                      ;; submodule-binder
                      #f
                      (let ((public-i (module-public-interface m)))
                        (and public-i
                             (module-copy public-i)))
                      ;; filename next-unique-id
                      #f 0
                      (hash-table-copy (module-replacements m))
                      ;; inlinable exports
                      #f))

(define-syntax-rule (re-export-curated-builtin-module name)
  (let ((m (current-module))
        (bindings (hash-ref curated-bindings 'name))
        (adjust-module (hash-ref curated-module-adjustments 'name (lambda (m) m))))

    (module-use-interfaces! m (list (resolve-builtin-interface 'name bindings)))

    (module-export! m bindings)
    (adjust-module m)
    m))

(define (resolve-builtin-interface name bindings)
  (dynamic-wind
    (lambda ()
      (set! (@ (guile) define-module*) original-define-module*))
    (lambda ()
      (resolve-interface name #:select bindings))
    (lambda ()
      (set! (@ (guile) define-module*) define-trusted-module*))))

(define curated-module-adjustments
  (alist->hash-table
   `(((srfi srfi-64)
      ,(lambda (m)
         ;; test-log-to-file defaults to true
         (set! (@ (srfi srfi-64) test-log-to-file) #f))))))

(define curated-bindings
  (alist->hash-table
   '(((ice-9 and-let-star)
      and-let*)

     ((ice-9 arrays)
      array-copy)

     ((ice-9 atomic)
      atomic-box-compare-and-swap!
      atomic-box-ref
      atomic-box-set!
      atomic-box-swap!
      atomic-box?
      make-atomic-box)

     ((ice-9 binary-ports)
      call-with-input-bytevector
      call-with-output-bytevector
      eof-object
      get-bytevector-all
      get-bytevector-n
      get-bytevector-n!
      get-bytevector-some
      get-bytevector-some!
      get-string-n!
      get-u8
      lookahead-u8
      make-custom-binary-input-port
      make-custom-binary-input/output-port
      make-custom-binary-output-port
      open-bytevector-input-port
      open-bytevector-output-port
      put-bytevector
      put-u8
      unget-bytevector)

     ((ice-9 calling)
      let-with-configuration-getter-and-setter
      with-configuration-getter-and-setter
      with-delegating-configuration-getter-and-setter
      with-delegating-getter-and-setter
      with-excursion-function
      with-excursion-getter-and-setter
      with-getter
      with-getter-and-setter)

     ((ice-9 common-list)
      adjoin
      and?
      butlast
      count-if
      delete-if!
      delete-if-not!
      every
      find-if
      has-duplicates?
      intersection
      member-if
      notany
      notevery
      or?
      pick
      pick-mappings
      reduce-init reduce
      remove-if
      remove-if-not
      set-difference
      some
      union
      uniq)

     ((ice-9 control)
      %
      abort
      abort-to-prompt
      call-with-escape-continuation
      call-with-prompt
      call/ec
      default-prompt-tag
      let-escape-continuation
      let/ec
      make-prompt-tag
      reset
      reset*
      shift
      shift*
      suspendable-continuation?)

     ((ice-9 copy-tree)
      copy-tree)

     ((ice-9 curried-definitions) ;; these are replacing bindings and need specific testing
      define
      define*
      define*-public
      define-public)

     ((ice-9 exceptions)
      &assertion-failure
      &error
      &exception
      &external-error
      &implementation-restriction
      &irritants
      &lexical
      &message
      &non-continuable
      &origin
      &programming-error
      &quit-exception
      &syntax
      &undefined-variable
      &warning
      assertion-failure?
      define-exception-type
      error?
      exception-accessor
      exception-args
      exception-irritants
      exception-kind
      exception-message
      exception-origin
      exception-predicate
      exception-type?
      exception-with-irritants?
      exception-with-message?
      exception-with-origin?
      exception?
      external-error?
      guard
      implementation-restriction-error?
      lexical-error?
      make-assertion-failure
      make-error
      make-exception
      make-exception-type
      make-exception-with-irritants
      make-exception-with-message
      make-exception-with-origin
      make-external-error
      make-implementation-restriction-error
      make-lexical-error
      make-non-continuable-error
      make-programming-error
      make-quit-exception
      make-syntax-error
      make-undefined-variable-error
      make-warning
      non-continuable-error?
      programming-error?
      quit-exception?
      raise-continuable
      raise-exception
      simple-exceptions
      syntax-error-form
      syntax-error-subform
      syntax-error?
      undefined-variable-error?
      warning?
      with-exception-handler)

     ((ice-9 format)
       format)

     ((ice-9 futures)
      future
      future?
      make-future
      touch)

     ((ice-9 gap-buffer)
      gb->lines
      gb->string
      gb-delete-char!
      gb-erase!
      gb-filter!
      gb-filter-lines!
      gb-goto-char
      gb-insert-char!
      gb-insert-string!
      gb-point
      gb-point-max
      gb-point-min
      gb?
      make-gap-buffer
      make-gap-buffer-port)

     ((ice-9 hash-table)
      alist->hash-table
      alist->hashq-table
      alist->hashv-table
      alist->hashx-table)

     ((ice-9 hcons)
      hashq-cons
      hashq-cons-assoc
      hashq-cons-create-handle!
      hashq-cons-get-handle
      hashq-cons-hash
      hashq-cons-ref
      hashq-cons-set!
      hashq-conser
      make-gc-buffer)

     ((ice-9 i18n)
      %global-locale
      %locale-dump
      char-locale-ci<?
      char-locale-ci=?
      char-locale-ci>?
      char-locale-downcase
      char-locale-titlecase
      char-locale-upcase
      char-locale<?
      char-locale>?
      locale-am-string
      locale-currency-symbol
      locale-currency-symbol-precedes-negative?
      locale-currency-symbol-precedes-positive?
      locale-date+time-format
      locale-date-format
      locale-day
      locale-day-short
      locale-decimal-point
      locale-digit-grouping
      locale-encoding
      locale-era
      locale-era-date+time-format
      locale-era-date-format
      locale-era-time-format
      locale-era-year
      locale-monetary-decimal-point
      locale-monetary-fractional-digits
      locale-monetary-grouping
      locale-monetary-negative-sign
      locale-monetary-positive-sign
      locale-monetary-thousands-separator
      locale-month
      locale-month-short
      locale-negative-separated-by-space?
      locale-negative-sign-position
      locale-no-regexp
      locale-pm-string
      locale-positive-separated-by-space?
      locale-positive-sign-position
      locale-string->inexact
      locale-string->integer
      locale-thousands-separator
      locale-time+am/pm-format
      locale-time-format
      locale-yes-regexp
      locale?
      make-locale
      monetary-amount->locale-string
      number->locale-string
      string-locale-ci<?
      string-locale-ci=?
      string-locale-ci>?
      string-locale-downcase
      string-locale-titlecase
      string-locale-upcase
      string-locale<?
      string-locale>?)

     ((ice-9 iconv)
      bytevector->string
      call-with-encoded-output-string
      string->bytevector)

     ((ice-9 lineio)
      lineio-port?
      make-line-buffering-input-port
      read-string
      unread-string)

     ((ice-9 list)
      rassoc
      rassq
      rassv)

     ((ice-9 match)
      match
      match-lambda
      match-lambda*
      match-let
      match-let*
      match-letrec)

     ((ice-9 occam-channel)
      !
      ?
      alt
      handshake-channel
      immediate-receive
      late-receive
      make-channel
      make-timer
      mutex
      oc:consequence
      oc:first-channel
      oc:immediate-dispatch
      oc:late-dispatch
      oc:lock
      oc:set-handshake-channel
      oc:unlock
      oc:unset-handshake-channel
      sender-waiting?)

     ((ice-9 peg cache)
      cg-cached-parser)

     ((ice-9 peg codegen)
      add-peg-compiler!
      compile-peg-pattern
      wrap-parser-for-users)

     ((ice-9 peg simplify-tree)
      context-flatten
      keyword-flatten
      string-collapse)

     ((ice-9 peg string-peg)
      define-peg-string-patterns
      peg-as-peg
      peg-grammar)

     ((ice-9 peg using-parsers)
      define-peg-pattern
      make-prec
      match-pattern
      peg-record?
      peg:end
      peg:start
      peg:string
      peg:substring
      peg:tree
      prec
      search-for-pattern)

     ((ice-9 peg)
      compile-peg-pattern
      context-flatten
      define-peg-pattern
      define-peg-string-patterns
      keyword-flatten
      match-pattern
      peg-record?
      peg:end
      peg:start
      peg:string
      peg:substring
      peg:tree
      search-for-pattern)

     ((ice-9 poe)
      perfect-funcq
      pure-funcq)

     ((ice-9 ports)
      %make-void-port
      %port-property
      %set-port-property!
      call-with-input-string
      call-with-output-string
      call-with-port
      char-ready?
      close-input-port
      close-output-port
      close-port
      current-error-port
      current-input-port
      current-load-port
      current-output-port
      current-warning-port
      drain-input
      eof-object?
      flush-all-ports
      force-output
      inherit-print-state
      input-port?
      output-port?
      peek-char
      port-closed?
      port-column
      port-conversion-strategy
      port-encoding
      port-for-each
      port-line
      port-mode
      port?
      read-char
      set-current-error-port
      set-current-input-port
      set-current-output-port
      set-port-column!
      set-port-conversion-strategy!
      set-port-encoding!
      set-port-line!
      setvbuf
      the-eof-object
      unread-char
      unread-string
      with-error-to-port
      with-error-to-string
      with-input-from-port
      with-input-from-string
      with-output-to-port
      with-output-to-string)

     ((ice-9 pretty-print)
      pretty-print
      truncated-print)

     ((ice-9 q)
      deq!
      enq!
      make-q
      q-empty-check
      q-empty?
      q-front
      q-length
      q-pop!
      q-push!
      q-rear
      q-remove!
      q?
      sync-q!)

     ((ice-9 rdelim)
      %read-delimited!
      %read-line
      read-delimited
      read-delimited!
      read-line
      read-line!
      read-string
      read-string!
      write-line)

     ((ice-9 receive)
      receive)

     ((ice-9 regex)
      fold-matches
      list-matches
      match:count
      match:end
      match:prefix
      match:start
      match:string
      match:substring
      match:suffix
      regexp-match?
      regexp-quote
      regexp-substitute
      regexp-substitute/global
      string-match)

     ((ice-9 runq)
      fair-strip-subtask
      make-exclusive-runq
      make-fair-runq
      make-subordinate-runq-to
      make-void-runq
      runq-control
      strip-sequence)

     ((ice-9 serialize)
      call-with-parallelization
      call-with-serialization
      parallelize
      serialize)

     ((ice-9 stack-catch)
      stack-catch)

     ((ice-9 streams)
      list->stream
      make-stream
      port->stream
      stream->list
      stream->list&length
      stream->reversed-list
      stream->reversed-list&length
      stream->vector
      stream-car
      stream-cdr
      stream-fold
      stream-for-each
      stream-map
      stream-null?
      vector->stream)

     ((ice-9 string-fun)
      has-trailing-newline?
      sans-final-newline
      sans-leading-whitespace
      sans-surrounding-whitespace
      sans-trailing-whitespace
      separate-fields-after-char
      separate-fields-before-char
      separate-fields-discarding-char
      split-after-char
      split-after-char-last
      split-after-predicate
      split-before-char
      split-before-char-last
      split-before-predicate
      split-discarding-char
      split-discarding-char-last
      split-discarding-predicate
      string-prefix-predicate
      string-prefix=?
      string-replace-substring)

     ((ice-9 suspendable-ports)
      current-read-waiter
      current-write-waiter
      install-suspendable-ports!
      uninstall-suspendable-ports!)

     ((ice-9 textual-ports)
      get-char
      get-line
      get-string-all
      get-string-n
      get-string-n!
      lookahead-char
      put-char
      put-string
      unget-char
      unget-string)

     ((ice-9 threads)
      %thread-handler
      all-threads
      begin-thread
      broadcast-condition-variable
      call-with-new-thread
      cancel-thread
      condition-variable?
      current-processor-count
      current-thread
      join-thread
      letpar
      lock-mutex
      make-condition-variable
      make-mutex
      make-recursive-mutex
      make-thread
      monitor
      mutex-level
      mutex-locked?
      mutex-owner
      mutex?
      n-for-each-par-map
      n-par-for-each
      n-par-map
      par-for-each
      par-map
      parallel
      signal-condition-variable
      thread-exited?
      thread?
      total-processor-count
      try-mutex
      unlock-mutex
      wait-condition-variable
      with-mutex
      yield)

     ((ice-9 time)
      time)

     ((ice-9 unicode)
      char->formal-name
      formal-name->char)

     ((ice-9 vlist)
      alist->vhash
      block-growth-factor
      list->vlist
      vhash-assoc
      vhash-assq
      vhash-assv
      vhash-cons
      vhash-consq
      vhash-consv
      vhash-delete
      vhash-delq
      vhash-delv
      vhash-fold
      vhash-fold*
      vhash-fold-right
      vhash-foldq*
      vhash-foldv*
      vhash?
      vlist->list
      vlist-append
      vlist-cons
      vlist-delete
      vlist-drop
      vlist-filter
      vlist-fold
      vlist-fold-right
      vlist-for-each
      vlist-head
      vlist-length
      vlist-map
      vlist-null
      vlist-null?
      vlist-ref
      vlist-reverse
      vlist-tail
      vlist-take
      vlist-unfold
      vlist-unfold-right
      vlist?)

     ((ice-9 weak-vector)
      list->weak-vector
      make-weak-vector
      weak-vector
      weak-vector-ref
      weak-vector-set!
      weak-vector?)

     ((oop goops)
      %compute-applicable-methods
      <accessor-method>
      <accessor>
      <applicable-struct-class>
      <applicable-struct-with-setter-class>
      <applicable-struct-with-setter>
      <applicable-struct>
      <applicable>
      <array>
      <atomic-box>
      <bitvector>
      <boolean>
      <bytevector>
      <char>
      <character-set>
      <class>
      <complex>
      <condition-variable>
      <directory>
      <dynamic-object>
      <dynamic-state>
      <extended-accessor>
      <extended-generic-with-setter>
      <extended-generic>
      <file-input-output-port>
      <file-input-port>
      <file-output-port>
      <file-port>
      <fluid>
      <foreign-slot>
      <foreign>
      <fraction>
      <frame>
      <generic-with-setter>
      <generic>
      <guardian>
      <hashtable>
      <hidden-slot>
      <hook>
      <input-output-port>
      <input-port>
      <integer>
      <keyword>
      <list>
      <macro>
      <method>
      <module>
      <mutex>
      <null>
      <number>
      <object>
      <opaque-slot>
      <output-port>
      <pair>
      <port>
      <primitive-generic>
      <procedure-class>
      <procedure>
      <promise>
      <protected-hidden-slot>
      <protected-opaque-slot>
      <protected-read-only-slot>
      <protected-slot>
      <random-state>
      <read-only-slot>
      <real>
      <redefinable-class>
      <regexp>
      <scm-slot>
      <slot>
      <string>
      <symbol>
      <syntax>
      <thread>
      <top>
      <unknown>
      <uvec>
      <vector>
      <vm-continuation>
      <vm>
      accessor-method-slot-definition
      add-method!
      allocate-instance
      apply-generic
      apply-method
      apply-methods
      change-class
      class
      class-direct-methods
      class-direct-slots
      class-direct-subclasses
      class-direct-supers
      class-methods
      class-name
      class-of
      class-precedence-list
      class-redefinition
      class-slot-definition
      class-slot-ref
      class-slot-set!
      class-slots
      class-subclasses
      compute-applicable-methods
      compute-cpl
      compute-get-n-set
      compute-getter-method
      compute-setter-method
      compute-slots
      compute-std-cpl
      deep-clone
      define-accessor
      define-class
      define-extended-generic
      define-extended-generics
      define-generic
      define-method
      enable-primitive-generic!
      ensure-accessor
      ensure-generic
      ensure-metaclass
      ensure-metaclass-with-supers
      find-method
      generic-function-methods
      generic-function-name
      get-keyword
      goops-error
      initialize
      instance?
      is-a?
      make
      make
      make-accessor
      make-class
      make-extended-generic
      make-generic
      make-instance
      max-fixnum
      method
      method-formals
      method-generic-function
      method-more-specific?
      method-procedure
      method-source
      method-specializers
      min-fixnum
      no-applicable-method
      no-method
      no-next-method
      primitive-generic-generic
      shallow-clone
      slot-bound?
      slot-definition-accessor
      slot-definition-allocation
      slot-definition-getter
      slot-definition-init-form
      slot-definition-init-keyword
      slot-definition-init-thunk
      slot-definition-init-value
      slot-definition-name
      slot-definition-options
      slot-definition-setter
      slot-exists?
      slot-init-function
      slot-missing
      slot-ref
      slot-set!
      slot-unbound
      sort-applicable-methods
      standard-define-class
      update-instance-for-different-class)

     ((oop goops accessors)
      standard-define-class
      define-class-with-accessors
      define-class-with-accessors-keywords)

     ((oop goops active-slot)
      <active-class>)

     ((oop goops composite-slot)
      <composite-class>)

     ((oop goops describe)
      describe)

     ((oop goops save)
      enumerate!
      enumerate-component!
      literal?
      load-objects
      make-readable
      make-unbound
      readable
      restore
      save-objects
      write-component
      write-component-procedure
      write-readably)

     ((oop goops simple)
      define-class)

     ((scheme char)
      char-alphabetic?
      char-ci<=?
      char-ci<?
      char-ci=?
      char-ci>=?
      char-ci>?
      char-downcase
      char-foldcase
      char-lower-case?
      char-numeric?
      char-upcase
      char-upper-case?
      char-whitespace?
      digit-value
      string-ci<=?
      string-ci<?
      string-ci=?
      string-ci>=?
      string-ci>?
      string-downcase
      string-foldcase
      string-upcase)

     ((scheme lazy)
      delay
      delay-force
      force
      make-promise
      promise?)

     ((scheme time)
      current-jiffy
      current-second
      jiffies-per-second)

     ((scheme write)
      display
      write
      write-shared
      write-simple)

     ((srfi srfi-1)
      alist-cons
      alist-copy
      alist-delete
      alist-delete!
      any
      append
      append!
      append-map
      append-map!
      append-reverse
      append-reverse!
      assoc
      assq
      assv
      break
      break!
      caaaar
      caaadr
      caaar
      caadar
      caaddr
      caadr
      caar
      cadaar
      cadadr
      cadar
      caddar
      cadddr
      caddr
      cadr
      car
      car+cdr
      cdaaar
      cdaadr
      cdaar
      cdadar
      cdaddr
      cdadr
      cdar
      cddaar
      cddadr
      cddar
      cdddar
      cddddr
      cdddr
      cddr
      cdr
      circular-list
      circular-list?
      concatenate
      concatenate!
      cons
      cons*
      count
      delete
      delete!
      delete-duplicates
      delete-duplicates!
      dotted-list?
      drop
      drop-right
      drop-right!
      drop-while
      eighth
      every
      fifth
      filter
      filter!
      filter-map
      find
      find-tail
      first
      fold
      fold-right
      for-each
      fourth
      iota
      last
      last-pair
      length
      length+
      list
      list-copy
      list-copy
      list-index
      list-ref
      list-tabulate
      list=
      lset-adjoin
      lset-diff+intersection
      lset-diff+intersection!
      lset-difference
      lset-difference!
      lset-intersection
      lset-intersection!
      lset-union
      lset-union!
      lset-xor
      lset-xor!
      lset<=
      lset=
      make-list
      map
      map!
      map-in-order
      member
      memq
      memv
      ninth
      not-pair?
      null-list?
      null?
      pair-fold
      pair-fold-right
      pair-for-each
      pair?
      partition
      partition!
      proper-list?
      reduce
      reduce-right
      remove
      remove!
      reverse
      reverse!
      second
      set-car!
      set-cdr!
      seventh
      sixth
      span
      span!
      split-at
      split-at!
      take
      take!
      take-right
      take-while
      take-while!
      tenth
      third
      unfold
      unfold-right
      unzip1
      unzip2
      unzip3
      unzip4
      unzip5
      xcons
      zip)

     ((srfi srfi-2)
      and-let*)

     ((srfi srfi-4)
      f32vector-copy
      f32vector-copy!
      f64vector-copy
      f64vector-copy!
      list->c32vector
      list->c64vector
      make-c32vector
      make-c64vector
      s16vector-copy
      s16vector-copy!
      s32vector-copy
      s32vector-copy!
      s64vector-copy
      s64vector-copy!
      s8vector-copy
      s8vector-copy!
      u16vector-copy
      u16vector-copy!
      u32vector-copy
      u32vector-copy!
      u64vector-copy
      u64vector-copy!
      u8vector-copy
      u8vector-copy!)

     ((srfi srfi-4 gnu)
      any->c32vector
      any->c64vector
      any->f32vector
      any->f64vector
      any->s16vector
      any->s32vector
      any->s64vector
      any->s8vector
      any->u16vector
      any->u32vector
      any->u64vector
      any->u8vector
      c32vector
      c32vector->list
      c32vector-copy
      c32vector-copy!
      c32vector-length
      c32vector-ref
      c32vector-set!
      c32vector?
      c64vector
      c64vector->list
      c64vector-copy
      c64vector-copy!
      c64vector-length
      c64vector-ref
      c64vector-set!
      c64vector?
      f32vector-copy
      f32vector-copy!
      f64vector-copy
      f64vector-copy!
      make-srfi-4-vector
      s16vector-copy
      s16vector-copy!
      s32vector-copy
      s32vector-copy!
      s64vector-copy
      s64vector-copy!
      s8vector-copy
      s8vector-copy!
      srfi-4-vector-type-size
      u16vector-copy
      u16vector-copy!
      u32vector-copy
      u32vector-copy!
      u64vector-copy
      u64vector-copy!
      u8vector-copy
      u8vector-copy!)

     ((srfi srfi-6)
      get-output-string
      open-input-string
      open-output-string)

     ((srfi srfi-8)
      receive)

     ((srfi srfi-9)
      define-record-type)

     ((srfi srfi-9 gnu)
      define-immutable-record-type
      set-field
      set-fields
      set-record-type-printer!)

     ((srfi srfi-11)
      let*-values
      let-values)

     ((srfi srfi-13)
       list->string
       make-string
       reverse-list->string
       string
       string->list
       string-any
       string-append
       string-append/shared
       string-ci<
       string-ci<=
       string-ci<>
       string-ci=
       string-ci>
       string-ci>=
       string-compare
       string-compare-ci
       string-concatenate
       string-concatenate-reverse
       string-concatenate-reverse/shared
       string-concatenate/shared
       string-contains
       string-contains-ci
       string-copy
       string-copy!
       string-count
       string-delete
       string-downcase
       string-downcase!
       string-drop
       string-drop-right
       string-every
       string-fill!
       string-filter
       string-fold
       string-fold-right
       string-for-each
       string-for-each-index
       string-hash
       string-hash-ci
       string-index
       string-index-right
       string-join
       string-length
       string-map
       string-map!
       string-null?
       string-pad
       string-pad-right
       string-prefix-ci?
       string-prefix-length
       string-prefix-length-ci
       string-prefix?
       string-ref
       string-replace
       string-reverse
       string-reverse!
       string-set!
       string-skip
       string-skip-right
       string-suffix-ci?
       string-suffix-length
       string-suffix-length-ci
       string-suffix?
       string-tabulate
       string-take
       string-take-right
       string-titlecase
       string-titlecase!
       string-tokenize
       string-trim
       string-trim-both
       string-trim-right
       string-unfold
       string-unfold-right
       string-upcase
       string-upcase!
       string-xcopy!
       string<
       string<=
       string<>
       string=
       string>
       string>=
       string?
       substring/shared
       xsubstring)

     ((srfi srfi-14)
      ->char-set
      char-set
      char-set->list
      char-set->string
      char-set-adjoin
      char-set-adjoin!
      char-set-any
      char-set-complement
      char-set-complement!
      char-set-contains?
      char-set-copy
      char-set-count
      char-set-cursor
      char-set-cursor-next
      char-set-delete
      char-set-delete!
      char-set-diff+intersection
      char-set-diff+intersection!
      char-set-difference
      char-set-difference!
      char-set-every
      char-set-filter
      char-set-filter!
      char-set-fold
      char-set-for-each
      char-set-hash
      char-set-intersection
      char-set-intersection!
      char-set-map
      char-set-ref
      char-set-size
      char-set-unfold
      char-set-unfold!
      char-set-union
      char-set-union!
      char-set-xor
      char-set-xor!
      char-set:ascii
      char-set:blank
      char-set:digit
      char-set:empty
      char-set:full
      char-set:graphic
      char-set:hex-digit
      char-set:iso-control
      char-set:letter
      char-set:letter+digit
      char-set:lower-case
      char-set:printing
      char-set:punctuation
      char-set:symbol
      char-set:title-case
      char-set:upper-case
      char-set:whitespace
      char-set<=
      char-set=
      char-set?
      end-of-char-set?
      list->char-set
      list->char-set!
      string->char-set
      string->char-set!
      ucs-range->char-set
      ucs-range->char-set!)

     ((srfi srfi-16)
      case-lambda)

     ((srfi srfi-17)
      caaaar
      caaadr
      caaar
      caadar
      caaddr
      caadr
      caar
      cadaar
      cadadr
      cadar
      caddar
      cadddr
      caddr
      cadr
      car
      cdaaar
      cdaadr
      cdaar
      cdadar
      cdaddr
      cdadr
      cdar
      cddaar
      cddadr
      cddar
      cdddar
      cddddr
      cdddr
      cddr
      cdr
      getter-with-setter
      setter
      string-ref
      vector-ref)

     ((srfi srfi-18)
      abandoned-mutex-exception?
      condition-variable-broadcast!
      condition-variable-name
      condition-variable-signal!
      condition-variable-specific
      condition-variable-specific-set!
      condition-variable?
      current-exception-handler
      current-thread
      current-time
      current-time
      join-timeout-exception?
      make-condition-variable
      make-condition-variable
      make-mutex
      make-mutex
      make-thread
      make-thread
      mutex
      mutex-lock!
      mutex-name
      mutex-specific
      mutex-specific-set!
      mutex-state
      mutex-unlock!
      mutex?
      raise
      seconds->time
      terminated-thread-exception?
      thread-join!
      thread-name
      thread-sleep!
      thread-specific
      thread-specific-set!
      thread-start!
      thread-terminate!
      thread-yield!
      thread?
      time->seconds
      time?
      uncaught-exception-reason
      uncaught-exception?
      with-exception-handler)

     ((srfi srfi-19)
      add-duration
      add-duration!
      copy-time
      current-date
      current-julian-day
      current-modified-julian-day
      current-time
      date->julian-day
      date->modified-julian-day
      date->string
      date->time-monotonic
      date->time-tai
      date->time-utc
      date-day
      date-hour
      date-minute
      date-month
      date-nanosecond
      date-second
      date-week-day
      date-week-number
      date-year
      date-year-day
      date-zone-offset
      date?
      julian-day->date
      julian-day->time-monotonic
      julian-day->time-tai
      julian-day->time-utc
      make-date
      make-time
      modified-julian-day->date
      modified-julian-day->time-monotonic
      modified-julian-day->time-tai
      modified-julian-day->time-utc
      set-time-nanosecond!
      set-time-second!
      set-time-type!
      string->date
      subtract-duration
      subtract-duration!
      time-difference
      time-difference!
      time-duration
      time-monotonic
      time-monotonic->date
      time-monotonic->julian-day
      time-monotonic->modified-julian-day
      time-monotonic->time-tai
      time-monotonic->time-tai!
      time-monotonic->time-utc
      time-monotonic->time-utc!
      time-nanosecond
      time-process
      time-resolution
      time-second
      time-tai
      time-tai->date
      time-tai->julian-day
      time-tai->modified-julian-day
      time-tai->time-monotonic
      time-tai->time-monotonic!
      time-tai->time-utc
      time-tai->time-utc!
      time-thread
      time-type
      time-utc
      time-utc->date
      time-utc->julian-day
      time-utc->modified-julian-day
      time-utc->time-monotonic
      time-utc->time-monotonic!
      time-utc->time-tai
      time-utc->time-tai!
      time<=?
      time<?
      time=?
      time>=?
      time>?
      time?)


     ((srfi srfi-26)
      cut
      cute)

     ((srfi srfi-27)
      default-random-source
      make-random-source
      random-real
      random-source-make-integers
      random-source-make-reals
      random-source-pseudo-randomize!
      random-source-randomize!
      random-source-state-ref
      random-source-state-set!
      random-source?
      random-integer)

     ((srfi srfi-28)
      format)

     ((srfi srfi-31)
      rec)

     ((srfi srfi-34)
      guard
      raise
      with-exception-handler)

     ((srfi srfi-35)
      &condition
      &error
      &message
      &serious
      condition
      condition-has-type?
      condition-message
      condition-ref
      condition-type?
      condition?
      define-condition-type
      error?
      extract-condition
      make-compound-condition
      make-condition
      make-condition-type
      message-condition?
      serious-condition?)

     ((srfi srfi-38)
      read-with-shared-structure
      write-with-shared-structure)

     ((srfi srfi-39)
      current-error-port
      current-input-port
      current-output-port
      parameterize
      make-parameter
      with-parameters*)

     ((srfi srfi-41)
      define-stream
      list->stream
      port->stream
      stream
      stream->list
      stream-append
      stream-car
      stream-cdr
      stream-concat
      stream-cons
      stream-constant
      stream-drop
      stream-drop-while
      stream-filter
      stream-fold
      stream-for-each
      stream-from
      stream-iterate
      stream-lambda
      stream-length
      stream-let
      stream-map
      stream-match
      stream-null
      stream-null?
      stream-of
      stream-pair?
      stream-range
      stream-ref
      stream-reverse
      stream-scan
      stream-take
      stream-take-while
      stream-unfold
      stream-unfolds
      stream-zip
      stream?)

     ((srfi srfi-42)
      :
      :-dispatch-ref
      :-dispatch-set!
      :char-range
      :dispatched
      :do
      :generator-proc
      :integers
      :let
      :list
      :parallel
      :port
      :range
      :real-range
      :string
      :until
      :vector
      :while
      any?-ec
      append-ec
      dispatch-union
      do-ec
      every?-ec
      first-ec
      fold-ec
      fold3-ec
      last-ec
      list-ec
      make-initial-:-dispatch
      max-ec
      min-ec
      product-ec
      string-append-ec
      string-ec
      sum-ec
      vector-ec
      vector-of-length-ec)

     ((srfi srfi-43)
      list->vector
      make-vector
      reverse-list->vector
      reverse-vector->list
      vector
      vector->list
      vector-any
      vector-append
      vector-binary-search
      vector-concatenate
      vector-copy
      vector-copy!
      vector-count
      vector-empty?
      vector-every
      vector-fill!
      vector-fold
      vector-fold-right
      vector-for-each
      vector-index
      vector-index-right
      vector-length
      vector-map
      vector-map!
      vector-ref
      vector-reverse!
      vector-reverse-copy
      vector-reverse-copy!
      vector-set!
      vector-skip
      vector-skip-right
      vector-swap!
      vector-unfold
      vector-unfold-right
      vector=
      vector?)

     ((srfi srfi-45)
      delay
      eager
      force
      lazy
      promise?)

     ((srfi srfi-60)
      any-bits-set?
      arithmetic-shift
      ash
      bit-count
      bit-field
      bit-set?
      bitwise-and
      bitwise-if
      bitwise-ior
      bitwise-merge
      bitwise-not
      bitwise-xor
      booleans->integer
      copy-bit
      copy-bit-field
      first-set-bit
      integer->list
      integer-length
      list->integer
      log2-binary-factors
      logand
      logbit?
      logcount
      logior
      logtest
      logxor
      reverse-bit-field
      rotate-bit-field)

     ((srfi srfi-64)
      test-apply
      test-approximate
      test-assert
      test-assert
      test-begin
      test-end
      test-eq
      test-equal
      test-eqv
      test-error
      test-expect-fail
      test-group
      test-group-with-cleanup
      test-match-all
      test-match-any
      test-match-name
      test-match-nth
      test-on-bad-count-simple
      test-on-bad-end-name-simple
      test-on-final-simple
      test-on-final-simple
      test-on-group-begin-simple
      test-on-group-end-simple
      test-on-test-end-simple
      test-passed?
      test-read-eval-string
      test-result-alist
      test-result-alist!
      test-result-clear
      test-result-kind
      test-result-ref
      test-result-remove
      test-result-set!
      test-runner-aux-value
      test-runner-aux-value!
      test-runner-create
      test-runner-current
      test-runner-factory
      test-runner-fail-count
      test-runner-fail-count!
      test-runner-get
      test-runner-group-path
      test-runner-group-stack
      test-runner-group-stack!
      test-runner-null
      test-runner-on-bad-count
      test-runner-on-bad-count!
      test-runner-on-bad-end-name
      test-runner-on-bad-end-name!
      test-runner-on-final
      test-runner-on-final!
      test-runner-on-group-begin
      test-runner-on-group-begin!
      test-runner-on-group-end
      test-runner-on-group-end!
      test-runner-on-test-begin
      test-runner-on-test-begin!
      test-runner-on-test-end
      test-runner-on-test-end!
      test-runner-pass-count
      test-runner-pass-count!
      test-runner-reset
      test-runner-simple
      test-runner-skip-count
      test-runner-skip-count!
      test-runner-test-name
      test-runner-xfail-count
      test-runner-xfail-count!
      test-runner-xpass-count
      test-runner-xpass-count!
      test-runner?
      test-skip
      test-with-runner)

     ((srfi srfi-67)
      </<=?
      </<?
      <=/<=?
      <=/<?
      <=?
      <?
      =?
      >/>=?
      >/>?
      >=/>=?
      >=/>?
      >=?
      >?
      boolean-compare
      chain<=?
      chain<?
      chain=?
      chain>=?
      chain>?
      char-compare
      char-compare-ci
      compare-by<
      compare-by<=
      compare-by=/<
      compare-by=/>
      compare-by>
      compare-by>=
      complex-compare
      cond-compare
      debug-compare
      default-compare
      if-not=?
      if3
      if<=?
      if<?
      if=?
      if>=?
      if>?
      integer-compare
      kth-largest
      list-compare
      list-compare-as-vector
      max-compare
      min-compare
      not=?
      number-compare
      pair-compare
      pair-compare-car
      pair-compare-cdr
      pairwise-not=?
      rational-compare
      real-compare
      refine-compare
      select-compare
      string-compare
      string-compare-ci
      symbol-compare
      vector-compare
      vector-compare-as-list)

     ((srfi srfi-69)
      alist->hash-table
      hash
      hash-by-identity
      hash-table->alist
      hash-table-copy
      hash-table-delete!
      hash-table-equivalence-function
      hash-table-exists?
      hash-table-fold
      hash-table-hash-function
      hash-table-keys
      hash-table-merge!
      hash-table-ref
      hash-table-ref/default
      hash-table-set!
      hash-table-size
      hash-table-update!
      hash-table-update!/default
      hash-table-values
      hash-table-walk
      hash-table?
      hash-table?
      make-hash-table
      make-hash-table
      string-ci-hash
      string-hash)

     ((srfi srfi-71)
      let
      let*
      letrec
      uncons
      unlist
      unvector
      values->list
      values->vector)

     ((srfi srfi-88)
      keyword->string
      keyword?
      string->keyword)

     ((srfi srfi-171)
      bytevector-u8-transduce
      generator-transduce
      list-transduce
      port-transduce
      rany
      rcons
      rcount
      reverse-rcons
      revery
      string-transduce
      tadd-between
      tappend-map
      tconcatenate
      tdelete-duplicates
      tdelete-neighbor-duplicates
      tdrop
      tdrop-while
      tenumerate
      tfilter
      tfilter-map
      tflatten
      tlog
      tmap
      tpartition
      tremove
      treplace
      tsegment
      ttake
      ttake-while
      vector-transduce)

     ((sxml apply-templates)
      apply-templates)

     ((sxml fold)
      fold-layout
      fold-values
      foldt
      foldts
      foldts*
      foldts*-values)

     ((sxml match)
      sxml-match
      sxml-match-let
      sxml-match-let*)

     ((sxml simple)
      sxml->string
      sxml->xml
      xml->sxml)

     ((sxml ssax)
      attlist->alist
      attlist-add
      attlist-fold
      attlist-null?
      attlist-remove-top
      current-ssax-error-port
      define-parsed-entity!
      make-empty-attlist
      reset-parsed-entity-definitions!
      ssax:complete-start-tag
      ssax:make-elem-parser
      ssax:make-parser
      ssax:make-pi-parser
      ssax:read-attributes
      ssax:read-cdata-body
      ssax:read-char-data
      ssax:read-char-ref
      ssax:read-external-id
      ssax:read-markup-token
      ssax:read-pi-body-as-string
      ssax:reverse-collect-str-drop-ws
      ssax:skip-internal-dtd
      ssax:uri-string->symbol
      ssax:xml->sxml
      with-ssax-error-to-port
      xml-token-head
      xml-token-kind
      xml-token?)

     ((sxml ssax input-parse)
      assert-curr-char
      find-string-from-port?
      next-token
      next-token-of
      peek-next-char
      read-string
      read-text-line
      skip-until
      skip-while)

     ((sxml transform)
      SRV:send-reply
      foldts
      post-order
      pre-post-order
      replace-range)

     ((sxml xpath)
      filter
      map-union
      node-closure
      node-eq?
      node-equal?
      node-join
      node-or
      node-parent
      node-pos
      node-reduce
      node-reverse
      node-self
      node-trace
      node-typeof?
      nodeset?
      select-kids
      sxpath
      take-after
      take-until)

     ((texinfo)
      call-with-file-and-dir
      stexi->sxml
      texi->stexi
      texi-command-depth
      texi-command-specs
      texi-fragment->stexi)

     ((texinfo docbook)
      *sdocbook->stexi-rules*
      *sdocbook-block-commands*
      filter-empty-elements
      replace-titles
      sdocbook-flatten)

     ((texinfo html)
      add-ref-resolver!
      stexi->shtml
      urlify)

     ((texinfo indexing)
      stexi-extract-index)

     ((texinfo plain-text)
      *line-width*
      stexi->plain-text)

     ((texinfo reflection)
      module-stexi-documentation
      object-stexi-documentation
      package-stexi-documentation
      package-stexi-documentation-for-include
      package-stexi-extended-menu
      package-stexi-generic-menu
      package-stexi-standard-copying
      package-stexi-standard-menu
      package-stexi-standard-prologue
      package-stexi-standard-titlepage
      script-stexi-documentation)

     ((texinfo serialize)
      stexi->texi)

     ((texinfo string-utils)
      center-string
      collapse-repeated-chars
      escape-special-chars
      expand-tabs
      fill-string
      left-justify-string
      make-text-wrapper
      right-justify-string
      string->wrapped-lines
      transform-string)

     ((web http)
      &chunked-input-error-prematurely
      chunked-input-ended-prematurely-error?
      declare-header!
      declare-opaque-header!
      header->string
      header-parser
      header-validator
      header-writer
      http-proxy-port?
      known-header?
      make-chunked-input-port
      make-chunked-output-port
      parse-header
      parse-http-method
      parse-http-version
      parse-request-uri
      read-header
      read-headers
      read-request-line
      read-response-line
      set-http-proxy-port?!
      string->header
      valid-header?
      write-header
      write-headers
      write-request-line
      write-response-line)

     ((web request)
      build-request
      read-request
      read-request-body
      request-absolute-uri
      request-accept
      request-accept-charset
      request-accept-encoding
      request-accept-language
      request-allow
      request-authorization
      request-cache-control
      request-connection
      request-content-encoding
      request-content-language
      request-content-length
      request-content-location
      request-content-md5
      request-content-range
      request-content-type
      request-date
      request-expect
      request-expires
      request-from
      request-headers
      request-host
      request-if-match
      request-if-modified-since
      request-if-none-match
      request-if-range
      request-if-unmodified-since
      request-last-modified
      request-max-forwards
      request-meta
      request-method
      request-port
      request-pragma
      request-proxy-authorization
      request-range
      request-referer
      request-te
      request-trailer
      request-transfer-encoding
      request-upgrade
      request-uri
      request-user-agent
      request-version
      request-via
      request-warning
      request?
      write-request
      write-request-body)

     ((web response)
      adapt-response-version
      build-response
      read-response
      read-response-body
      response-accept-ranges
      response-age
      response-allow
      response-body-port
      response-cache-control
      response-code
      response-connection
      response-content-encoding
      response-content-language
      response-content-length
      response-content-location
      response-content-md5
      response-content-range
      response-content-type
      response-date
      response-etag
      response-expires
      response-headers
      response-last-modified
      response-location
      response-must-not-include-body?
      response-port
      response-pragma
      response-proxy-authenticate
      response-reason-phrase
      response-retry-after
      response-server
      response-trailer
      response-transfer-encoding
      response-upgrade
      response-vary
      response-version
      response-via
      response-warning
      response-www-authenticate
      response?
      text-content-type?
      write-response
      write-response-body)

     ((web uri)
      build-relative-ref
      build-uri
      build-uri-reference
      build-uri-reference
      declare-default-port!
      encode-and-join-uri-path
      relative-ref?
      split-and-decode-uri-path
      string->relative-ref
      string->uri
      string->uri-reference
      string->uri-reference
      uri->string
      uri-decode
      uri-encode
      uri-fragment
      uri-host
      uri-path
      uri-port
      uri-query
      uri-reference?
      uri-scheme
      uri-userinfo
      uri?))))

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
     define-module
     define-public
     display
     newline
     use-modules
     resolve-module
     format
     with-exception-handler)

    ((plguile3 base)
     @
     define-public-module
     re-export-curated-builtin-module

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

(define sandbox-iface-specs
  (let ((sandbox-bindings (append all-pure-and-impure-bindings
                                  plguile3-bindings)))
    (map (lambda (x)
           (let ((mod-name (car x))
                 (bindings (cdr x)))
             (resolve-interface mod-name #:select bindings)))
         sandbox-bindings)))

(define indent "")
(define output '())
(define-syntax-rule (instrument target)
  (let* ((orig (@@ (guile) target))
         (wrapper
          (lambda args
            (let ((base-indent indent))
              (set! output orig)
              (notice (format #f "~a~s" base-indent (cons 'target args)))
              (set! indent (string-append indent "  "))
              (let ((result (apply orig args)))
                (set! indent base-indent)
                result)))))
    (notice "instrument")
    (notice (format #f "~s" orig))
    (set! (@@ (guile) target) wrapper)
    (notice (format #f "~s" orig))
    ))

;; (instrument resolve-module)
