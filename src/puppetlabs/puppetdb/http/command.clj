(ns puppetlabs.puppetdb.http.command
  (:require [puppetlabs.puppetdb.command :as command]
            [puppetlabs.puppetdb.http :as http]
            [puppetlabs.puppetdb.middleware :as mid]
            [puppetlabs.puppetdb.command.constants :refer [command-names]]
            [cheshire.core :as cheshire])
  (:import [com.github.fge.jsonschema.main JsonSchemaFactory]
           [com.github.fge.jackson JsonLoader]))

(defn enqueue-command
  "Enqueue the comman in the request parameters return a UUID"
  [{:keys [body-string globals] :as request}]
  (let [uuid (command/enqueue-raw-command!
              (get-in globals [:command-mq :connection-string])
              (get-in globals [:command-mq :endpoint])
              body-string)]
    (http/json-response {:uuid uuid})))

(def resource-ref-schema
  {:type "object"
   :properties {}
   :required ["type", "title"]})

(def resource-schema
  {:title "resource"
   :type "object"
   :properties {"type"       {:type "string"}
                "title"      {:type "string"}
                "exported"   {:type "boolean"}
                "file"       {:type "string"}
                "line"       {:type "integer"}
                "tags"       {:type "array", :items {:type "string"}}
                "aliases"    {:type "array", :items {:type "string"}}
                "parameters" {:type "object"}}
   :required ["type" "title" "exported" "file" "line" "tags" "aliases" "parameters"]})

(def edge-relationship-schema
  {:enum ["contains" "before" "required-by" "notifies" "subscription-of"]})

(def edge-schema
  {:title "edge"
   :type "object"
   :properties {"source" resource-ref-schema
                "target" resource-ref-schema
                "relationship" edge-relationship-schema}})

(def catalog-schema
  {:title "catalog"
   :type "object"
   :properties {"version" {:type "version"}
                "name" {:type "string"}
                "environment" {:type "string"}
                "transaction_uuid" {:type "string"}
                "producer_timestamp" {:type "string", :format "date-time"}
                "resources" {:type "array", :items resource-schema}
                "edges" {:type "array", :items edge-schema}}
   :required ["version" "name", "environment", "edges", "resources"]})

(defmulti payload-schema (fn [jnode]
                           [(.. jnode (get "command") asText)
                            (.. jnode (get "version") asInt)]))

(defmethod payload-schema :default [_] {:type "object"})
(defmethod payload-schema ["replace catalog" 6] [_] catalog-schema)

(def schema-factory (JsonSchemaFactory/byDefault))
(def prepare-schema
  (memoize
   (fn [schema]
     (let [parsed-schema (-> schema cheshire/generate-string JsonLoader/fromString)]
       (.getJsonSchema schema-factory parsed-schema)))))

(defn json-valid? [schema jnode]
  (.validInstanceUnchecked (prepare-schema schema) jnode))

(def command-schema
  {:title "command"
   :properties {"command" {:enum (vals command-names)}
                "version" {:type "integer"}
                "payload" {:type "object"}}
   :required ["command" "version" "payload"]})

(defn validate-command-json [app]
  (fn [{:keys [body-string] :as req}]
    (let [parsed-body (JsonLoader/fromString body-string)]
      (if (json-valid? command-schema parsed-body)
        (let [payload (.findValue parsed-body "payload")
              payload-schema (prepare-schema (payload-schema parsed-body))]
          (println "*********" payload)
          (println (.validate payload-schema payload)))))
    (app req)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

;; The below fns expect to be called from a moustache handler and
;; return functions that accept a ring request map

(defn command-app
  "Function validating the request then submitting a command"
  [version]
  (-> enqueue-command
    mid/verify-accepts-json
    mid/verify-checksum
    (mid/validate-query-params {:optional ["checksum"]})
    validate-command-json
    mid/payload-to-body-string
    (mid/verify-content-type ["application/json"])
))
