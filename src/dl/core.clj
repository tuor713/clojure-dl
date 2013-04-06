(ns dl.core
  (:use [clojure.walk :only [postwalk]]
        [clojure.java.io :only [reader]]))

;; Feature list
;; - require / include namespaces
;; - real world examples
;; - eagerly updating refs (<<=, <<-)
;; - factory style server side execution


;;;;;;;;;;;;;;;;;;
;; Basic utilities
;;;;;;;;;;;;;;;;;; 

(defn- var-descendants
  ([children-map var] (var-descendants children-map var #{var}))
  ([children-map var accu]
     (let [children (get children-map var)
           new-accu (into accu children)
           new-members (remove accu children)]
       (reduce
        (fn [res v] (var-descendants children-map v res))
        new-accu
        new-members))))

(defn- compute-descendants [children-map]
  (into {}
        (map 
         (fn [v] [v (var-descendants children-map v)])
         (keys children-map))))

(defn- retrieve-val [graph var]
  ((get-in graph [:accessors var] #(get-in % [:values var] ::nil))
   graph))

(defn set-val [graph var val]
  (let [cur (retrieve-val graph var)]
    (if (= cur val)
      graph
      (-> graph
          (update-in [:dirty] into (get-in graph [:descendants var]))
          (update-in [:dirty] disj var)
          (assoc-in [:values var] val)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;; New SPI, protocol based
;;;;;;;;;;;;;;;;;;;;;;;;;;


(defprotocol DLGraph
  (add-rule [self var f deps opts] "Adds a rule to the graph and returns the new graph object")
  (rules [self] "Obtain the set of rules as list of map (var, function, deps, options, current-value)")
  (realize-var [self var] "Make sure that a variable is computed")
  (get-var [self var] "Get the value of a variable")
  (set-var [self var val] "Overwrite the value of a variable"))

(defrecord StatelessGraph
    [parents children descendants rule-map recompute accessors dirty values]
  
  DLGraph
  (add-rule [self var f deps opts]
    (let [recompute? (get opts :recompute false)
          new-children (reduce
                        #(update-in %1 [%2] conj var)
                        children
                        deps)]
      (StatelessGraph.
       (assoc parents var deps)
       new-children
       (compute-descendants new-children)
       (assoc rule-map var f)
       (if recompute? (conj recompute var) recompute)
       (merge
        (into {}
              (map 
               (fn [v] [v #(get-in % [:values v] ::nil)])
               deps))
        (assoc accessors var
               (if recompute?
                 (fn [g] (apply f
                                (map 
                                 #(retrieve-val g %)
                                 deps)))
                 #(get-in % [:values var] ::nil))))
       (conj dirty var)
       (assoc values var ::nil))))

  (rules [self]
    (for [[v f] rule-map]
      {:var v
       :rule f
       :deps (parents v)
       :opts {:recompute (contains? recompute v)}
       :value (if (contains? dirty v)
                ::nil
                (get values v ::nil))}))
  
  (realize-var [graph var]
    (if (contains? dirty var)
      (let [var-parents (parents var)
            newdirty (assoc graph :dirty (disj dirty var))
            new-graph (reduce #(realize-var %1 %2) newdirty var-parents)]
        (if (contains? recompute var)
          new-graph
          (assoc-in new-graph
                    [:values var]
                    (let [pvals (map #(retrieve-val new-graph %) var-parents)]
                      (if (some #{::nil} pvals)
                        ::nil
                        (apply (get rule-map var (constantly ::nil))
                               pvals))))))

      graph))

  (get-var [self var]
    (retrieve-val self var))

  (set-var [self var val]
    (set-val self var val)))

(defrecord RefGraph
    [graph-ref]

  DLGraph
  (add-rule [self var f deps opts]
    (alter graph-ref add-rule var f deps opts)
    self)
  
  (rules [self]
    (rules @graph-ref))

  (realize-var [self var]
    (alter graph-ref realize-var var)
    self)
  
  (get-var [self var]
    (get-var (alter graph-ref realize-var var)
             var))

  (set-var [self var val]
    (alter graph-ref set-var var val)
    self))

(defn create-dl-graph 
  "DL Graph maintaining all state in one hash-map embedded in a single ref"
  []
  (RefGraph. 
   (ref
    (StatelessGraph. {} {} {} {} #{} {} #{} {}))))


(defprotocol Node
  (mark-dirty [self])
  (add-child [self node])
  (set-rule [self f deps opts])
  (to-rule [self] "Creates a rule representation of this node")
  
  (value [self])
  (set-value [self val]))

(defrecord DefaultNode [attrs]
    Node
    (mark-dirty [self]
      (when-not (:dirty? @attrs)
        (alter attrs assoc :dirty? true)
        (doseq [n (:children @attrs)]
          (mark-dirty n)))
      self)

    (to-rule [self]
      {:rule (:rule @attrs)
       :value (if (:dirty? @attrs) 
                ::nil 
                (get @attrs :value ::nil))
       :deps (map #(:var (deref (:attrs %))) (:parents @attrs))
       :opts {:recompute (:recompute? @attrs)}
       :var (:var @attrs)})
    
    (value [self]
      (if (or (:dirty? @attrs) (:recompute? @attrs))
        (do 
          (alter attrs assoc :dirty? false)
          (let [pvals (map value (:parents @attrs))
                nval (if (some #{::nil} pvals) 
                       ::nil
                       (apply (:rule @attrs) pvals))]
            (when-not (:recompute? @attrs)
              (alter attrs assoc :value nval))
            nval))
        (get @attrs :value ::nil)))

    (set-value [self val]
      (when-not (= val (:value @attrs))
        (alter attrs assoc :value val :dirty? false)
        (mark-dirty self)
        ;; in case of cyclic dependencies we want to make sure, we don't get set to dirty unnecessarily
        (alter attrs assoc :dirty? false))
      self)

    (add-child [self node]
      (alter attrs update-in [:children] conj node)
      self)
    
    (set-rule [self f deps opts]
      (alter attrs assoc
             :rule f
             :parents deps
             :recompute? (:recompute opts))
      (mark-dirty self)
      self))

(defrecord NodeGraph
    [nodes]
  
  DLGraph
  (add-rule [self var f deps opts]
    (doseq [v (filter #(nil? (@nodes %)) (cons var deps))]
      (alter nodes assoc v
             (DefaultNode. (ref {:dirty? true 
                                 :children #{}
                                 :var v
                                 :rule (constantly ::nil)}))))
    
    (let [parents (map @nodes deps)
          current (@nodes var)]
      (doseq [p parents]
        (add-child p current))
      (set-rule current f 
                (map @nodes deps)
                opts))
    self)

  (rules [self]
    (for [n (vals @nodes)]
      (to-rule n)))

  (realize-var [self var]
    ;; no-op
    self)

  (get-var [self var]
    (if-let [node (get @nodes var)]
      (value node)
      ::nil))

  (set-var [self var val]
    (if-let [n (get @nodes var)]
      (set-value n val)
      (alter nodes assoc var
             (DefaultNode. (ref {:dirty? false
                                 :children #{}
                                 :var var
                                 :value val
                                 :rule (constantly val)}))))
    self))

(defn create-node-graph 
  "DL Graph using separate refs for each node in the tree. Since refs are separate, potentially
concurrent updates can operate on different subsets of nodes"
  []
  (NodeGraph. (ref {})))

;;;;;;;;;;;;;;
;; DSL Helpers
;;;;;;;;;;;;;;

(defn- sym->s [s default-ns]
  (cond
   (not= nil (namespace s)) (str (namespace s) "/" (name s))
   (not= nil default-ns) (str "$" default-ns "/" (.substring (name s) 1))
   :else (name s)))

(defn- sym->kw [s default-ns]
  (keyword (.substring (sym->s s default-ns) 1)))

(defn- dl-var? [s]
  (and (symbol? s) (.startsWith (sym->s s nil) "$")))

(defn- denamespacify [s default-ns]
  (symbol (.replaceAll (.replaceAll (sym->s s default-ns)
                                    "\\." "_")
                       "/" "_")))

(defn- collect-vars [body]
  (:vars
   (meta
    (postwalk
     (fn [exp]
       (if (dl-var? exp)
         (with-meta exp {:vars #{exp}})
         (cond 
           (sequential? exp)
           (with-meta exp
             {:vars
              (set (apply concat
                          (map (comp :vars meta) exp)))})

           (map? exp)
           (with-meta exp
             {:vars
              (set (mapcat (fn [[k v]] (concat (:vars (meta k))
                                               (:vars (meta v)))) 
                           exp))})

           :else exp)))
     body))))

(defn- create-gen [syms default-ns rule]
  (if (empty? syms)
    `(fn [] ~rule)
    (let [v (gensym)]
      `(fn [~@(map #(denamespacify % default-ns) syms)]
         ~(postwalk 
           #(if (dl-var? %) (denamespacify % default-ns) %)
           rule)))))


;;;;;;
;; API
;;;;;;

(def ^{:dynamic true} default-imp create-dl-graph)

(defmacro graph
  [& inputs]
  (let [has-ns? (= 1 (mod (count inputs) 3))
        rules (if has-ns? (rest inputs) inputs)
        nsname (if has-ns? (first inputs) nil)
        g (gensym)]
    `(let [~g (dl.core/default-imp)]
       (dosync
        ~@(map
           (fn [[v op rule]]
             (let [var (sym->kw v nsname)
                   syms (collect-vars rule)
                   deps (vec (map #(sym->kw % nsname) syms))
                   f (create-gen syms nsname rule)]
               `(add-rule ~g  ~var ~f ~deps {:recompute ~(= op '<=)})))
           (partition 3 rules)))
       ~g)))

(defmacro graph-with-impl
  [impl & inputs]
  `(binding [dl.core/default-imp ~impl]
     (graph ~@inputs)))

(def unset ::nil)

(defn get! [g var] 
  (dosync (get-var g var)))

(defn set! [g var val]
  (dosync (set-var g var val)))

(defn clear! [g var]
  (dosync (set-var g var unset)))

(defn merge! 
  "Merges together graphs (updates the first graph). Implementation will be from the first graph, rules in the later 
graphs overwrite rules in earlier graphs"
  [& graphs]
  (dosync
   (reduce
    (fn [res g]
      (doseq [r (rules g)]
        (add-rule res (:var r) (:rule r) (:deps r) (:opts r))
        (when-not (= ::nil (:value r))
          (dl.core/set! res (:var r) (:value r))))
      res)
    (first graphs)
    (rest graphs))))


;;;;;;;;;;;;;;;;;;;
;; External DL code
;;;;;;;;;;;;;;;;;;;

(defn- read-input
  "Reads all forms from the given input (any supported by clojure.java.io/reader)"
  [in]
  (with-open [r (java.io.PushbackReader. (reader in))]
    (doall
     (take-while #(not= ::eof %)
                 (repeatedly #(read r false ::eof))))))

(defn parse-dl-file [input]
  (let [code (read-input input)]
    (eval (apply list 'dl.core/graph code))))


(defn run-dl-project
  "Loads code sources in order, then sets inputs and returns the value of the specified output var"
  [code-sources inputs output-var]
  (let [g (apply merge! (map parse-dl-file code-sources))]
    (doseq [[k v] inputs]
      (dl.core/set! g k v))
    (get! g output-var)))

(defn run-dl-file [code-source inputs output-var]
  (run-dl-project [code-source] inputs output-var))

;; end
