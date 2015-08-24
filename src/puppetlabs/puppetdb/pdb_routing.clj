(ns puppetlabs.puppetdb.pdb-routing
  (:require [puppetlabs.trapperkeeper.core :as tk]
            [compojure.core :as compojure]
            [puppetlabs.trapperkeeper.services :as tksvc]
            [ring.middleware.resource :refer [resource-request]]
            [ring.util.request :as rreq]
            [ring.util.response :as rr]
            [puppetlabs.puppetdb.meta :as meta]
            [puppetlabs.puppetdb.admin :as admin]
            [puppetlabs.puppetdb.http.command :as cmd]
            [puppetlabs.puppetdb.http.server :as server]
            [clojure.tools.logging :as log]
            [puppetlabs.puppetdb.middleware :as mid]))


(defn resource-request-handler [req]
  (resource-request req "public"))

(defn maint-mode-handler [maint-mode-fn]
  (fn [req]
    (when (maint-mode-fn)
      (log/info "HTTP request received while in maintenance mode")
      {:status 503
       :body "PuppetDB is currently down. Try again later."})))

(defn wrap-with-context [uri route]
  (compojure/context uri [] route))

(defn pdb-core-routes [get-shared-globals submit-command-fn query-fn enqueue-raw-command-fn response-pub]
  (let [cmd-mq #(:command-mq (get-shared-globals))
        meta-cfg #(select-keys (get-shared-globals) [:update-server
                                                     :product-name
                                                     :scf-read-db])
        get-response-pub #(response-pub)]
    (map #(apply wrap-with-context %)
         (partition
          2
          ;; The remaining get-shared-globals args are for wrap-with-globals.
          ["/meta" (meta/build-app meta-cfg)
           "/cmd" (cmd/command-app cmd-mq get-shared-globals
                                   enqueue-raw-command-fn get-response-pub)
           "/query" (server/build-app get-shared-globals)
           "/admin" (admin/build-app submit-command-fn query-fn)]))))

(defn pdb-app [root get-shared-globals maint-mode-fn app-routes]
  (-> (compojure/context root []
                         resource-request-handler
                         (maint-mode-handler maint-mode-fn)
                         (compojure/GET "/" req
                                        (->> req
                                             rreq/request-url
                                             (format "%s/dashboard/index.html")
                                             rr/redirect))
                         (apply compojure/routes app-routes))
      (mid/wrap-with-puppetdb-middleware #(:authorizer (get-shared-globals)))))

(defprotocol MaintenanceMode
  (enable-maint-mode [this])
  (disable-maint-mode [this])
  (maint-mode? [this]))

(tk/defservice maint-mode-service
  MaintenanceMode
  []
  (init [this context]
        (assoc context ::maint-mode? (atom true)))
  (enable-maint-mode [this]
                     (-> (tksvc/service-context this)
                         (update ::maint-mode? reset! true)))
  (disable-maint-mode [this]
                      (-> (tksvc/service-context this)
                          (update ::maint-mode? reset! false)))
  (maint-mode? [this]
               (let [maint-mode-atom (::maint-mode? (tksvc/service-context this) ::not-found)]
                 ;;There's a small gap in time after the routes have
                 ;;been added but before the TK service context gets
                 ;;updated. This handles that and counts it as the app
                 ;;being in maintenance mode
                 (if (= ::not-found maint-mode-atom)
                   true
                   @maint-mode-atom))))

(tk/defservice pdb-routing-service
  [[:WebroutingService add-ring-handler get-route]
   [:PuppetDBServer shared-globals query set-url-prefix]
   [:PuppetDBCommand submit-command]
   [:PuppetDBCommandDispatcher enqueue-command enqueue-raw-command response-pub]
   [:MaintenanceMode enable-maint-mode maint-mode? disable-maint-mode]]
  (init [this context]
        (let [context-root (get-route this)
              query-prefix (str context-root "/query")
              shared-with-prefix #(assoc (shared-globals) :url-prefix query-prefix)]
          (set-url-prefix query-prefix)
          (log/info "Starting PuppetDB, entering maintenance mode")
          (add-ring-handler this (pdb-app context-root
                                          shared-with-prefix
                                          maint-mode?
                                          (pdb-core-routes shared-with-prefix
                                                           submit-command
                                                           query
                                                           enqueue-raw-command
                                                           response-pub))))
        (enable-maint-mode)
        context)
  (start [this context]
         (log/info "PuppetDB finished starting, disabling maintenance mode")
         (disable-maint-mode)
         context))