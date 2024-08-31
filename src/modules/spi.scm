(define-module (plguile3 spi)
  #:use-module (plguile3 primitives)
  #:use-module (pg types)
  #:use-module ((srfi srfi-1) #:select (count))
  #:re-export (commit
               commit-and-chain
               rollback
               rollback-and-chain
               start-transaction
               stop-command-execution)

  #:export (cursor-open
            execute
            fetch
            scalar))

(define* (cursor-open command
                      #:optional (args '())
                      #:key (hold #f) (name #f) (scroll '()))
  (%cursor-open command args count hold name scroll))

(define* (execute command
                  #:optional (args '())
                  #:key (count 0) receiver)
  (if receiver
      (%execute-with-receiver receiver command args count)
      (%execute command args count)))

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

(define (scalar t)
  (if (or (< 1 (table-length t)) (< 1 (table-width t)))
      (raise-exception `(non-scalar-result #:length ,(table-length t) #:width ,(table-width t))))
  (record-ref (table-row t 0) 0))
