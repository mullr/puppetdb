(ns puppetlabs.puppetdb.facts-plus
  (:require [schema.core :as s]
            [puppetlabs.puppetdb.schema :as pls]
            [puppetlabs.puppetdb.cheshire :as json]
            [clojure.java.io :as io]
            [honeysql.core :as hcore]
            [puppetlabs.puppetdb.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.string :as string]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [clojure.core.async :as async]))

;; top level switch for the feature flag; remove this when it becomes the default.
(def enable (atom true))

;;; Dynamic fact table schemas

(def fact-type (s/enum "string" "object" "float" "boolean" "integer"))
(def single-fact-schema {s/Str fact-type})
(def multi-fact-schema {s/Str single-fact-schema})

(pls/defn-validated load-default-fact-schema :- multi-fact-schema
  []
  (-> (io/resource "builtin_facts.json")
      slurp
      json/parse-string))

(defn table-name [section]
  (str "facts_" section))

(pls/defn-validated ensure-table [section :- s/Str]
  (jdbc/do-commands
   (format
    "CREATE TABLE IF NOT EXISTS %s (
       factset_id BIGINT UNIQUE NOT NULL REFERENCES factsets(id) ON DELETE CASCADE,
       hash BIGINT NOT NULL
     )"
    (table-name section))))

(def fact-type->sql-type
  {"string" "text"
   "object" "jsonb"
   "float" "float"
   "boolean" "bool"
   "integer" "bigint"})

(def sql-type->fact-type
  (set/map-invert fact-type->sql-type))

(pls/defn-validated ensure-columns
  [section :- s/Str
   fs :- single-fact-schema]
  (let [existing-fs (->> (jdbc/query ["SELECT column_name, data_type FROM information_schema.columns
                                         WHERE table_schema='public'
                                         AND table_name=?"
                                      (table-name section)])
                         (remove #(#{"factset_id" "hash"} (:column_name %)))
                         (map (fn [{:keys [column_name data_type]}]
                                [column_name
                                 (sql-type->fact-type (str/lower-case data_type))]))
                         (into {}))
        desired-cols (set (keys fs))
        existing-cols (set (keys existing-fs))
        missing-cols (set/difference existing-cols desired-cols)
        extra-cols (set/difference desired-cols existing-cols)]
    (doseq [[fact fact-type] fs]
      (let [[_ existing-fact-type] (get existing-fs fact)]
        (when (not= fact-type existing-fact-type)
          (log/warn (format "Fact column %s should have type %s according to the schema, but actually has type %s"
                            (str (table-name section) "." fact)
                            fact-type
                            existing-fact-type)))
        (do
          (log/info (format "Adding fact column %s"
                            (str (table-name section) "." fact)))
         (jdbc/do-commands
          (format "ALTER TABLE %s ADD COLUMN %s %s"
                  (table-name section)
                  fact
                  (fact-type->sql-type fact-type))))))

    (doseq [c extra-cols]
      (log/warn (format "Column %s is in the database but not in the facts schema"
                        (str (table-name section) "." c))))))

(pls/defn-validated apply-schema [mfs :- multi-fact-schema]
  (doseq [[section fs] mfs]
    (ensure-table section)
    (ensure-columns section fs)))

;;; Fact storage

(def invert-fact-schema
  (memoize
   (fn [mfs]
     (->> mfs
          (mapcat (fn [[ section fs]]
                    (map (partial cons section) fs)))
          (map (fn [[section fact type]]
                 [fact {:table (table-name section)
                        :type type}]))
          (into {})))))

(defn fact-table-and-type [fact mfs]
  (get (invert-fact-schema mfs) fact))

(pls/defn-validated fact-value->sql [value, type :- fact-type]
  (when value
    (if (= type "object")
      (sutils/munge-jsonb-for-storage value)
      value)))

(defn select-table-hashes [factset-id tables]
  (let [sql (->> tables
                 (map-indexed (fn [n t]
                                (format "(SELECT hash, %s AS n
                                          FROM %s
                                          WHERE factset_id = factset_id)"
                                        n t)))
                 (str/join " UNION ALL "))
        hashes (->> (jdbc/query (str sql " ORDER BY n ASC"))
                    (map :hash))]
    (zipmap tables hashes)))

(defn insert-fact-row [table factset-id row-hash fact-maps]
  (let [sorted-fact-maps (sort-by :fact fact-maps)]
    (jdbc/do-prepared
     (format "INSERT INTO %s (factset_id, hash, %s) VALUES (?, ?, %s)"
             table
             (->> sorted-fact-maps
                  (map :fact)
                  (str/join ", "))
             (->> (repeat (count sorted-fact-maps) "?")
                  (str/join ", ")))
     (concat
      [factset-id row-hash]
      (->> sorted-fact-maps
           (map (fn [{:keys [value type]}]
                  (fact-value->sql value type))))))))

(defn update-fact-row [table factset-id row-hash fact-maps]
  (let [sorted-fact-maps (sort-by :fact fact-maps)]
    (jdbc/do-prepared
     (format "UPDATE %s SET (hash, %s) = (?, %s) where factset_id=?"
             table
             (->> sorted-fact-maps
                  (map :fact)
                  (str/join ", "))
             (->> (repeat (count sorted-fact-maps) "?")
                  (str/join ", ")))
     (concat [row-hash]
             (map :value sorted-fact-maps)
             [factset-id]))))

(defn store-fact-values [factset-id mfs values]
  (let [default-values (->> mfs
                            (mapcat (fn [[sections fs]] (keys fs)))
                            (map (fn [fact] [fact nil]))
                            (into {}))
        defaulted-values (merge default-values values)
        facts-by-table (->> defaulted-values
                            (map (fn [[fact value]]
                                   (assoc (fact-table-and-type fact mfs)
                                          :fact fact
                                          :value value)))
                            (remove #(nil? (:table %)))
                            (group-by :table))
        affected-tables (keys facts-by-table)
        current-hashes-by-table (when (seq affected-tables)
                                  (select-table-hashes factset-id affected-tables))]

    (doseq [[table fact-maps] facts-by-table]
      (let [current-hash (current-hashes-by-table table)
            ;; keep the hash consistent
            fact-maps (sort-by :fact fact-maps)
            new-hash (hash fact-maps)]
        (cond
          (nil? current-hash) (insert-fact-row table factset-id new-hash fact-maps)
          (= current-hash new-hash) ::noop
          ;; if anything is different, update the whole row; postgres is going
          ;; to internally rewrite the whole thing anyway.
          :else (update-fact-row table factset-id new-hash fact-maps))))))


(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn gen-stable []
  (-> (into {}
            (for [fact-num (range 150)]
              [(str "fact_" fact-num) (rand-str (rand-int 256))]))
      (assoc "rand-float" (rand)
             "two-vals" (rand-nth ["a" "b"])
             "three-vals" (rand-nth ["a" "b" "c"])
             "four-vals" (rand-nth ["a" "b" "c" "d"]))))

(defn gen-volatile []
  (-> (into {}
            (for [fact-num (range 10)]
              [(str "volatile_" fact-num) (rand-str (rand-int 256))]))))

(comment
  (jdbc/do-commands "drop table json_facts")
  (jdbc/do-commands "create table json_facts (eid bigint unique not null, facts jsonb)")
  (jdbc/do-commands "delete from json_facts")

  (let [in (async/chan 1000)
        out (async/chan 1000)]

    (async/go-loop [n 0]
      (if (< n 100000)
        (do
          (async/>! in n)
          (recur (inc n)))
        (async/close! in)))

    (async/go-loop []
      (when (async/<! out)
        (recur)))

    (async/pipeline-blocking 16
                             out
                             (map (fn [x]
                                    (jdbc/do-prepared
                                     "insert into json_facts(eid, facts) values (?, ?)"
                                     [x
                                      (fact-value->sql (gen-facts) "object")])))
                             in
                             true))


  (let [in (async/chan 1000)
        out (async/chan 1000)]

    (async/go-loop [n 0]
      (if (< n 100000)
        (do
          (async/>! in n)
          (recur (inc n)))
        (async/close! in)))

    (async/go-loop []
      (when (async/<! out)
        (recur)))

    (async/pipeline-blocking 16
                             out
                             (map (fn [x]
                                    (jdbc/do-prepared
                                     "update json_facts set volatile=? where eid=?"
                                     [(fact-value->sql (gen-volatile) "object") x])))
                             in
                             true))



  (jdbc/query "select * from json_facts limit 2")

  )
