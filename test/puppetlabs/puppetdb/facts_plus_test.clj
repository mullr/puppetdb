(ns puppetlabs.puppetdb.facts-plus-test
  (:require [clojure.test :refer :all]
            [puppetlabs.puppetdb.facts-plus :refer :all]
            [clojure.java.jdbc :as sql]
            [puppetlabs.puppetdb.testutils.db :refer [*db* with-test-db]]
            [puppetlabs.puppetdb.jdbc :as jdbc
             :refer [call-with-query-rows query-to-vec]]
            [clojure.set :as set]
            [puppetlabs.puppetdb.scf.storage :as scf-store]
            [puppetlabs.kitchensink.core :as ks]
            [puppetlabs.puppetdb.scf.storage-utils :as sutils]
            [puppetlabs.puppetdb.cheshire :as json])
  (:import [org.postgresql.util PGobject]))


(deftest apply-fact-schema-test
  (with-test-db
    (let [mfs (load-default-fact-schema)
          _ (apply-schema mfs)
          tables (->> (jdbc/query ["select table_name from information_schema.tables
                                    where table_schema='public'"])
                      (map :table_name)
                      (into #{}))
          volatile-facts (->> (jdbc/query ["select column_name from information_schema.columns
                                            where table_schema='public'
                                            and table_name='facts_volatile'"])
                              (map :column_name)
                              (into #{}))]
      (is (set/subset? #{"facts_modern" "facts_legacy" "facts_volatile"}
                       tables))
      (is (contains? volatile-facts "uptime")))))


(defn select-facts [factset-id tablename]
  (->>  (-> (jdbc/query [(format "select * from %s where factset_id=?" tablename)
                         factset-id])
            first
            (dissoc :factset_id :hash))
        (map (fn [[k v]] (when v
                           [(name k)
                            (if (instance? PGobject v)
                              (json/parse-string (str v))
                              v)])))
        (remove nil?)
        (into {})))

(defn select-hash [factset-id tablename]
  (-> (jdbc/query [(format "select hash from %s where factset_id=?" tablename)
                   factset-id])
      first
      :hash))

(deftest store-fact-values-test
  (with-test-db
    (let [mfs (load-default-fact-schema)]
      (apply-schema mfs)
      (jdbc/do-commands
       "insert into certnames(id, certname) values (0, 'node1')"
       "insert into factsets(id, certname, timestamp, producer_timestamp, hash)
        values (0, 'node1', now(), now(), ''::bytea)")

      ;; insert new facts
      (store-fact-values 0 mfs
                         {"os" {"family" "Linux"}
                          "domain" "mynewdomain.com"
                          "fqdn" "myhost.mynewdomain.com"
                          "hostname" "myhost"
                          "kernel" "Linux"
                          "uptime_seconds" 3600})

      (prn (jdbc/query "select * from facts_legacy"))

      (is (= {"domain" "mynewdomain.com"
              "fqdn" "myhost.mynewdomain.com"
              "hostname" "myhost"}
             (select-facts 0 "facts_legacy")))

      (is (= {"os" {"family" "Linux"}
              "kernel" "Linux"}
             (select-facts 0 "facts_modern")))

      (is (= {"uptime_seconds" 3600}
             (select-facts 0 "facts_volatile")))

      ;; update exiting facts
      (let [hashes-before {:modern (select-hash 0 "facts_modern")
                           :legacy (select-hash 0 "facts_legacy")
                           :volatile (select-hash 0 "facts_volatile")}]
        (store-fact-values 0 mfs
                           {"os" {"family" "Linux"}
                            "domain" "mynewdomain.com"
                            "fqdn" "myhost.mynewdomain.com"
                            "hostname" "myhost"
                            "kernel" "Linux"
                            "uptime_seconds" 3602})
        (let [hashes-after {:modern (select-hash 0 "facts_modern")
                            :legacy (select-hash 0 "facts_legacy")
                            :volatile (select-hash 0 "facts_volatile")}]

          (is (= (select-keys hashes-before [:modern :legacy])
                 (select-keys hashes-after [:modern :legacy])))

          (is (not (= (:volatile hashes-before)
                      (:volatile hashes-after))))

          (is (= {"uptime_seconds" 3602}
                 (select-facts 0 "facts_volatile")))))))) 
