(ns dl.benchmark
  (:require [dl.core :as dl])
  (:use criterium.core))

(defn run-standard-bench [] 
  (let [g (dl/graph $x <- (* 2 $y)
                    $y <- (inc $z)
                    $z <- 1)] 

    (println "=== Arithmetic chain of two steps ===")
    (quick-bench (do (dl/set! g :z (rand-int 10))
                     (dl/get! g :x)))
    (println "")

    (println "=== Repeated gets ===")
    (quick-bench (dl/get! g :x))
    (println "")))

(defn run-parsing-bench []
  (println "=== Parsing 6 rule graph ===")
  (quick-bench (dl/graph $x <- (inc $y)
                    $y <- (* 2 $z)
                    $z <- 1
                    $a <- (inc $z)
                    $b <- (* 2 $a)))
  (println))

