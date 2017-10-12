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
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.facts-plus :as facts-plus]
            [puppetlabs.puppetdb.testutils :as tu]
            [puppetlabs.puppetdb.time :as time]
            [clj-time.core :refer [now]])
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


(defn select-facts-from-table [factset-id tablename]
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

(defn factset-map [certname]
  (let [factset-id (-> (jdbc/query ["select id from factsets where certname=?"
                                    certname])
                       first
                       :id)]
    (merge (select-facts-from-table factset-id "facts_modern")
           (select-facts-from-table factset-id "facts_legacy")
           (select-facts-from-table factset-id "facts_volatile"))))

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

      (is (= {"domain" "mynewdomain.com"
              "fqdn" "myhost.mynewdomain.com"
              "hostname" "myhost"}
             (select-facts-from-table 0 "facts_legacy")))

      (is (= {"os" {"family" "Linux"}
              "kernel" "Linux"}
             (select-facts-from-table 0 "facts_modern")))

      (is (= {"uptime_seconds" 3600}
             (select-facts-from-table 0 "facts_volatile")))

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
                 (select-facts-from-table 0 "facts_volatile")))))))) 

(def reference-time "2014-10-28T20:26:21.727Z")
(def previous-time "2014-10-26T20:26:21.727Z")

(deftest facts-plus-in-storage-layer
  (with-test-db

    (facts-plus/apply-schema (facts-plus/load-default-fact-schema))

    (testing "Persisted facts"
      (let [certname "some_certname"
            facts {"domain" "mydomain.com"
                   "fqdn" "myhost.mydomain.com"
                   "hostname" "myhost"
                   "kernel" "Linux"
                   "operatingsystem" "Debian"}
            producer "bar.com"]
        (scf-store/add-certname! certname)

        (is (nil?
             (jdbc/with-db-transaction []
               (scf-store/timestamp-of-newest-record :factsets "some_certname"))))
        (is (empty? (factset-map "some_certname")))

        (scf-store/add-facts! {:certname certname
                               :values facts
                               :timestamp previous-time
                               :environment nil
                               :producer_timestamp previous-time
                               :producer producer})

        (testing "should have entries for each fact"
          (is (= {"domain" "mydomain.com"
                  "fqdn" "myhost.mydomain.com"
                  "hostname" "myhost"
                  "kernel" "Linux"
                  "operatingsystem" "Debian" }
                 (factset-map certname))))

        (testing "should have entries for each fact"
          (is (= facts (factset-map certname)))
          (is (jdbc/with-db-transaction []
                (scf-store/timestamp-of-newest-record :factsets  "some_certname")))
          (is (= facts (factset-map "some_certname"))))

        (testing "should add the certname if necessary"
          (is (= (query-to-vec "SELECT certname FROM certnames")
                 [{:certname certname}])))

        (testing "replacing facts"
          ;;Ensuring here that new records are inserted, updated
          ;;facts are updated (not deleted and inserted) and that
          ;;the necessary deletes happen
          (tu/with-wrapped-fn-args [updates jdbc/update!]
            (let [new-facts {"domain" "mynewdomain.com"
                             "fqdn" "myhost.mynewdomain.com"
                             "hostname" "myhost"
                             "kernel" "Linux"
                             "uptime_seconds" 3600}]
              (scf-store/replace-facts! {:certname certname
                                         :values new-facts
                                         :environment "DEV"
                                         :producer_timestamp reference-time
                                         :timestamp reference-time
                                         :producer producer})

              (testing "should have only the new facts"
                (is (not (get (factset-map certname)
                              "operatingsystem"))))

              (testing "producer_timestamp should store current time"
                (is (= (jdbc/query-to-vec "SELECT producer_timestamp FROM factsets")
                       [{:producer_timestamp (time/to-timestamp reference-time)}])))

              (testing "should update existing keys"
                (is (some #{{:timestamp (time/to-timestamp reference-time)
                             :environment_id 1
                             :hash "1a4b10a865b8c7b435ec0fe06968fdc62337f57f"
                             :producer_timestamp (time/to-timestamp reference-time)
                             :producer_id 1}}
                          ;; Again we grab the pertinent non-id bits
                          (map (fn [itm]
                                 (-> (second itm)
                                     (update-in [:hash] sutils/parse-db-hash)))
                               @updates)))
                (is (some (fn [update-call]
                            (and (= :factsets (first update-call))
                                 (:timestamp (second update-call))))
                          @updates))))))

        (testing "replacing all new facts"
          (scf-store/delete-certname-facts! certname)
          (scf-store/replace-facts! {:certname certname
                                     :values facts
                                     :environment "DEV"
                                     :producer_timestamp (now)
                                     :timestamp (now)
                                     :producer producer})
          (is (= facts (factset-map "some_certname"))))

        (testing "replacing all facts with new ones"
          (scf-store/delete-certname-facts! certname)
          (scf-store/add-facts! {:certname certname
                                 :values facts
                                 :timestamp previous-time
                                 :environment nil
                                 :producer_timestamp previous-time
                                 :producer nil})
          (scf-store/replace-facts! {:certname certname
                                     :values {"operatingsystem" "BeOS"}
                                     :environment "DEV"
                                     :producer_timestamp (now)
                                     :timestamp (now)
                                     :producer producer})
          (is (= {"operatingsystem" "BeOS"} (factset-map "some_certname"))))

        (testing "replace-facts with only additions"
          (let [fact-map (factset-map "some_certname")]
            (scf-store/replace-facts! {:certname certname
                                       :values (assoc fact-map
                                                      "bios_vendor" "American Megatrends")
                                       :environment "DEV"
                                       :producer_timestamp (now)
                                       :timestamp (now)
                                       :producer producer})
            (is (= (assoc fact-map "bios_vendor" "American Megatrends")
                   (factset-map "some_certname")))))

        (testing "replace-facts with no change"
          (let [fact-map (factset-map "some_certname")]
            (scf-store/replace-facts! {:certname certname
                                       :values fact-map
                                       :environment "DEV"
                                       :producer_timestamp (now)
                                       :timestamp (now)
                                       :producer producer})
            (is (= fact-map
                   (factset-map "some_certname")))))

        (testing "stable hash when no facts change"
          (let [fact-map (factset-map "some_certname")
                {old-hash :hash} (first (query-to-vec (format "SELECT %s AS hash FROM factsets where certname=?" (sutils/sql-hash-as-str "hash")) certname))]
            (scf-store/replace-facts! {:certname certname
                                       :values fact-map
                                       :environment "DEV"
                                       :producer_timestamp (now)
                                       :timestamp (now)
                                       :producer producer})
            (let [{new-hash :hash} (first (query-to-vec (format "SELECT %s AS hash FROM factsets where certname=?" (sutils/sql-hash-as-str "hash")) certname))]
              (is (= old-hash new-hash)))
            (scf-store/replace-facts! {:certname certname
                                       :values (assoc fact-map "another thing" "goes here")
                                       :environment "DEV"
                                       :producer_timestamp (now)
                                       :timestamp (now)
                                       :producer producer})
            (let [{new-hash :hash} (first (query-to-vec (format "SELECT %s AS hash FROM factsets where certname=?" (sutils/sql-hash-as-str "hash")) certname))]
              (is (not= old-hash new-hash)))))))))
