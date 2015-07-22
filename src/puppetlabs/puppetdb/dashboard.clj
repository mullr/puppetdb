(ns puppetlabs.puppetdb.dashboard
  (:require [clojure.tools.logging :as log]
            [net.cgrand.moustache :refer [app]]
            [puppetlabs.puppetdb.middleware :refer [wrap-with-puppetdb-middleware]]
            [puppetlabs.trapperkeeper.core :refer [defservice]]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.util.request :as rreq]
            [ring.util.response :as rr]
            [compojure.core :as compojure]))

(def metrics-views
  [{:mbean "java.lang:type=Memory"
    :dataPath ["HeapMemoryUsage" "used"]
    :format ",.3s"
    :description "JVM Heap"
    :addendum "bytes"}

   {:mbean "puppetlabs.puppetdb.query.population:type=default,name=num-nodes"
    :dataPath ["Value"]
    :format ","
    :description "Nodes"
    :addendum "in the population"}

   {:mbean "puppetlabs.puppetdb.query.population:type=default,name=num-resources"
    :dataPath ["Value"]
    :format ","
    :description "Resources"
    :addendum "in the population"}

   {:mbean "puppetlabs.puppetdb.query.population:type=default,name=pct-resource-dupes"
    :dataPath ["Value"]
    :format ",.1%"
    :description "Resource duplication"
    :addendum "% of resources stored"}

   {:mbean "puppetlabs.puppetdb.scf.storage:type=default,name=duplicate-pct"
    :dataPath ["Value"]
    :format ",.1%"
    :description "Catalog duplication"
    :addendum "% of catalogs encountered"}

   {:mbean "org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=puppetlabs.puppetdb.commands"
    :dataPath ["QueueSize"]
    :format ",s"
    :description "Command Queue"
    :addendum "depth"}

   {:mbean "puppetlabs.puppetdb.command:type=global,name=processing-time"
    :dataPath ["50thPercentile"]
    :scale 0.001
    :format ",.3s"
    :description "Command Processing"
    :addendum "sec/command"}

   {:mbean "puppetlabs.puppetdb.command:type=global,name=processed"
    :dataPath ["FiveMinuteRate"]
    :format ",.3s"
    :clampToZero 0.001
    :description "Command Processing"
    :addendum "commands/sec"}

   {:mbean "puppetlabs.puppetdb.command:type=global,name=processed"
    :dataPath ["Count"]
    :format ","
    :description "Processed"
    :addendum "since startup"}

   {:mbean "puppetlabs.puppetdb.command:type=global,name=retried"
    :dataPath ["Count"]
    :format ","
    :description "Retried"
    :addendum "since startup"}

   {:mbean "puppetlabs.puppetdb.command:type=global,name=discarded"
    :dataPath ["Count"]
    :format ","
    :description "Discarded"
    :addendum "since startup"}

   {:mbean "puppetlabs.puppetdb.command:type=global,name=fatal"
    :dataPath ["Count"]
    :format ","
    :description "Rejected"
    :addendum "since startup"}

   {:mbean "puppetlabs.puppetdb.http.command:type=/pdb/cmd/v1,name=service-time"
    :dataPath ["50thPercentile"]
    :scale 0.001
    :format ",.3s"
    :description "Enqueueing"
    :addendum "service time, seconds"}

   {:mbean "puppetlabs.puppetdb.http.server:type=/pdb/query/v4/resources,name=service-time"
    :dataPath ["50thPercentile"]
    :scale 0.001
    :format ",.3s"
    :description "Collection Queries"
    :addendum "service time, seconds"}

   {:mbean "puppetlabs.puppetdb.scf.storage:type=default,name=gc-time"
    :dataPath ["50thPercentile"]
    :scale 0.001
    :format ",.3s"
    :description "DB Compaction"
    :addendum "round trip time, seconds"}

   {:mbean "puppetlabs.puppetdb.command.dlo:type=global,name=compression"
    :dataPath ["50thPercentile"]
    :scale 0.001
    :format ",.3s"
    :description "DLO Compression"
    :addendum "round trip time, seconds"}

   {:mbean "puppetlabs.puppetdb.command.dlo:type=global,name=filesize"
    :dataPath ["Value"]
    :format ",.3s"
    :description "DLO Size on Disk"
    :addendum "bytes"}

   {:mbean "puppetlabs.puppetdb.command.dlo:type=global,name=messages"
    :dataPath ["Value"]
    :format ","
    :description "Discarded Messages"
    :addendum "to be reviewed"}])

(def dashboard
  (let [index-handler #(->> %
                            rreq/request-url
                            (format "%s/dashboard/index.html")
                            rr/redirect)]
    (-> (app [""] {:get index-handler})
        (wrap-resource "public"))))

(defservice dashboard-service
  [[:PuppetDBServer shared-globals]
   [:WebroutingService add-ring-handler get-route]]

  (start [this context]
         (let [app (-> dashboard
                       (wrap-with-puppetdb-middleware (:authorizer (shared-globals))))]
           (log/info "Starting dashboard service")
           (->> app
                (compojure/context (get-route this) [])
                (add-ring-handler this))
           context)))

(def dashboard-redirect
  (app ["" &] {:get (fn [req] (rr/redirect "/pdb/dashboard/index.html"))}))

(defservice dashboard-redirect-service
  [[:PuppetDBServer shared-globals]
   [:WebroutingService add-ring-handler get-route]]

  (start [this context]
         (log/info "Redirecting / to the PuppetDB dashboard")
         (->> dashboard-redirect
              (add-ring-handler this))
         context))
