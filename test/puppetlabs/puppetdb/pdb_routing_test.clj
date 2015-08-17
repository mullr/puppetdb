(ns puppetlabs.puppetdb.pdb-routing-test
  (:require [clojure.test :refer :all]
            [clj-http.client :as client]
            [puppetlabs.puppetdb.testutils.services :as svc-utils]
            [puppetlabs.puppetdb.testutils :as tu]
            [puppetlabs.puppetdb.cheshire :as json]
            [puppetlabs.puppetdb.time :as time]
            [puppetlabs.puppetdb.client :as pdb-client]
            [clj-time.coerce :refer [to-string]]
            [clj-time.core :refer [now]]
            [puppetlabs.puppetdb.testutils.dashboard :as dtu]
            [puppetlabs.puppetdb.utils :as utils]
            [puppetlabs.puppetdb.cli.services :as clisvc]
            [puppetlabs.puppetdb.pdb-routing :refer :all]
            [puppetlabs.trapperkeeper.app :as tk-app]))

(defn command-base-url
  [base-url]
  (utils/pdb-cmd-base-url (:host base-url)
                          (:port base-url)
                          :v1))

(defn pdb-get [base-url url-suffix]
  (let [resp (client/get (str (utils/base-url->str base-url)
                              url-suffix)
                         {:throw-exceptions false})]
    (if (tu/json-content-type? resp)
      (update resp :body #(json/parse-string % true))
      resp)))

(defn submit-facts [base-url facts]
  (svc-utils/sync-command-post (command-base-url base-url)
                               "replace facts"
                               4
                               facts))

(defn query-fact-names [base-url]
  (pdb-get (assoc svc-utils/*base-url*
             :prefix "/pdb/query"
             :version :v4)
           "/fact-names"))

(defn export [base-url]
  (pdb-get (assoc base-url
             :prefix "/pdb/admin"
             :version :v1)
           "/archive"))

(defn query-server-time [base-url]
  (pdb-get (assoc svc-utils/*base-url*
             :prefix "/pdb/meta"
             :version :v1)
           "/server-time"))

(def test-facts {:certname "foo.com"
                 :environment "DEV"
                 :producer_timestamp (to-string (now))
                 :values {:foo 1
                          :bar "2"
                          :baz 3}})

(deftest top-level-routes
  (svc-utils/call-with-puppetdb-instance
   (fn []
     (let [pdb-resp (client/get (dtu/dashboard-base-url->str (assoc svc-utils/*base-url*
                                                               :prefix "/pdb")))]
       (tu/assert-success! pdb-resp)
       (is (dtu/dashboard-page? pdb-resp))

       (is (-> (query-server-time svc-utils/*base-url*)
               (get-in [:body :server_time])
               time/from-string))



       (let [resp (export svc-utils/*base-url*)]
         (tu/assert-success! resp)
         (is (.contains (get-in resp [:headers "Content-Disposition"]) "puppetdb-export"))
         (is (:body resp)))))))

(deftest maintenance-mode
  (svc-utils/call-with-puppetdb-instance
   (fn []
     (let [maint-mode-service (tk-app/get-service svc-utils/*server* :MaintenanceMode)]
       (is (= 200 (:status (submit-facts svc-utils/*base-url* test-facts))))
       (is (= #{"foo" "bar" "baz"}
              (-> (query-fact-names svc-utils/*base-url*)
                  :body
                  set)))
       (enable-maint-mode maint-mode-service)
       (is (= (:status (query-fact-names svc-utils/*base-url*))
              503))

       (disable-maint-mode maint-mode-service)
       (is (= #{"foo" "bar" "baz"}
              (-> (query-fact-names svc-utils/*base-url*)
                  :body
                  set)))))))
