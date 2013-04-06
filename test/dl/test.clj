(ns dl.test
  (:require [dl.core :as dl])
  (:use clojure.test
        [clojure.walk :only [postwalk]]))

(def implementations-to-test
  {dl/create-node-graph "Node implementation"
   dl/create-dl-graph "Map implementation"})

;; Helper so that we can write tests that cover different implementations
;; TODO: actually there is a more generic pattern here which is data-driven testing
;; i.e. the testing function can take a set of parameters with which to evaluate the body
(defmacro deftest-with-impls
  [name & code]
  `(deftest ~name
     ~@(for [[impl name] implementations-to-test]
         (apply list 'testing
               (str "Testing: " name)
               (postwalk
                (fn [exp]
                  (if (and (list? exp) (= (first exp) 'dl/graph))
                    (apply list 'dl/graph-with-impl impl (rest exp))
                    exp))
                code)))))


(deftest-with-impls test-static-rule
  (is (= 3 
         (let [g (dl/graph $x <- 3)]
           (dl/get! g :x)))))

(deftest-with-impls test-get-with-unknown-key
  (let [g (dl/graph $x <- 1)]
    (is (= dl/unset
           (dl/get! g :y)))))

(deftest-with-impls test-set-with-unknown-key
  (let [g (dl/graph $x <- 1)]
    (dl/set! g :y 2)
    (is (= 2 (dl/get! g :y)))))

(deftest-with-impls test-dependency
  (is (= 2 (let [g (dl/graph $x <- (inc $y)
                             $y <- 1)]
             (dl/get! g :x)))))

(deftest-with-impls test-multi-dependency
  (is (= 4 (let [g (dl/graph $x <- (* 2 $y)
                             $y <- (inc $z)
                             $z <- 1)]
             (dl/get! g :x)))))

(deftest-with-impls test-unset-dependency
  (is (= dl/unset
         (let [g (dl/graph $x <- $y)]
           (dl/get! g :x)))))

(deftest-with-impls test-clear
  (let [g (dl/graph $x <- $y
                    $y <- 1)]
    (is (= 1 (dl/get! g :x)))
    (dl/clear! g :y)
    (is (= dl/unset (dl/get! g :y)))
    (is (= dl/unset (dl/get! g :x)))))


(deftest-with-impls test-assign-dependency
  (is (= 2 (let [g (dl/graph $x <- (inc $y))]
             (dl/set! g :y 1)
             (dl/get! g :x)))))

(deftest-with-impls test-cyclic-dependency
  (let [g (dl/graph $x <- (inc $y)
                    $y <- (dec $x))]
    (is (= dl/unset (dl/get! g :x)))
    (is (= dl/unset (dl/get! g :y)))
    
    (dl/set! g :x 1)
    (is (= 0 (dl/get! g :y)))
    (is (= 1 (dl/get! g :x)))))

(deftest-with-impls test-higher-functions
  (let [g (dl/graph $x <- (reduce + $y)
                    $y <- (range 11))]
    (is (= 55 (dl/get! g :x))))

  (let [g (dl/graph $x <- (reduce + (map #(/ % 2) (filter even? $y)))
                    $y <- (range 21))]
    (is (= 55 (dl/get! g :x)))))


(deftest-with-impls test-transitive-updates
  (let [g (dl/graph $x <- (inc $y)
                    $y <- (* 2 $z)
                    $z <- 1
                    $a <- (inc $z)
                    $b <- (* 2 $a))]
    (is (= 3 (dl/get! g :x)))
    (is (= 4 (dl/get! g :b)))
    (dl/set! g :z 2)
    (is (= 5 (dl/get! g :x)))
    (is (= 6 (dl/get! g :b)))))

(deftest-with-impls test-store
  (let [cnt (atom 0)
        g (dl/graph $x <= (do (swap! cnt inc) 1))]
    (is (= 1 (dl/get! g :x)))
    (is (= 1 @cnt))
    (dl/get! g :x)
    (is (= 2 @cnt))))

(deftest-with-impls test-namespace-qualifiers
  (let [g (dl/graph $ns.output/x <- (inc $ns.input/x)
                    $ns.input/x <- 1)]
    (is (= 1 (dl/get! g :ns.input/x)))
    (is (= 2 (dl/get! g :ns.output/x)))))

(deftest-with-impls test-no-recomputation
  (let [g (dl/graph $x <- (+ $y (rand))
                    $y <- 1)
        startx (dl/get! g :x)]
    (dl/set! g :y 1)
    (is (= startx (dl/get! g :x)))))

(deftest-with-impls test-no-redundant-updates
  (let [cnt (atom 0) 
        g (dl/graph $x <- (do (swap! cnt inc) (+ $y $z))
                    $y <- 1
                    $z <- 2)]
    (is (= 3 (dl/get! g :x)))
    (is (= 1 @cnt))
    (dl/set! g :y 2)
    (dl/set! g :z 3)
    (is (= 5 (dl/get! g :x)))
    (is (= 2 @cnt))))

(deftest-with-impls test-namespace-prefixing
  (let [g (dl/graph myns
                    $x <- (inc $y)
                    $y <- 1)]
    (is (= 2 (dl/get! g :myns/x)))))


(deftest-with-impls test-read-dl-file
  (let [g (dl/parse-dl-file (.getBytes "sample $x <- (inc $y) $y <- 2"))]
    (is (= 3 (dl/get! g :sample/x)))))

(deftest test-run-dl-file
  (is (= 3 (dl/run-dl-file 
            (.getBytes "$x <- (+ $y $z)")
            {:y 1 :z 2}
            :x))))

(deftest test-run-dl-project
  (is (= 3 (dl/run-dl-project
            [(.getBytes "$x <- (+ $y $z)")
             (.getBytes "$y <- 1")]
            {:z 2}
            :x))))

(deftest-with-impls test-vanilla-merge-graphs
  (let [g1 (dl/graph $x <- (inc $y))
        g2 (dl/graph $y <- 1)]
    (is (= 2 (dl/get! (dl/merge! g1 g2) :x)))))

(deftest-with-impls test-merge-with-conflicts
  (let [g1 (dl/graph $x <- (inc $y)
                     $y <- 1)
        g2 (dl/graph $y <- 2)]
    (is (= 3 (dl/get! (dl/merge! g1 g2) :x)))))

(deftest-with-impls test-merge-with-value-conservation
  (let [g1 (dl/graph $x <- (inc $y))
        g2 (dl/graph $y <- 1)]
    (dl/set! g2 :y 2)
    (is (= 3 (dl/get! (dl/merge! g1 g2) :x)))))


;; end
