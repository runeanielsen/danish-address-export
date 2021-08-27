(ns danish-address-search-indexer.core
  (:require [danish-address-search-indexer.dawa :as dawa]
            [cheshire.core :as json]
            [clojure.java.io :as io]))

(defn export-access-addresses
  [transaction-id export-path]
  (with-open [w (io/writer (str export-path "/access-addresses.json") :append true)]
    (dawa/get-all-access-addresses
     transaction-id
     (fn [dawa-address]
       (let [mapped-address (dawa/map-access-address dawa-address)]
         (.write w (str (json/generate-string mapped-address) "\n")))))))

(defn export-unit-addresses
  [transaction-id export-path]
  (with-open [w (io/writer (str export-path "/unit-addresses.json") :append true)]
    (dawa/get-all-unit-addresses
     transaction-id
     (fn [dawa-address]
       (let [mapped-address (dawa/map-unit-address dawa-address)]
         (.write w (str (json/generate-string mapped-address) "\n")))))))

(defn export-post-codes
  [transaction-id export-path]
  (let [mapped-post-codes (map dawa/map-post-code (dawa/get-post-codes transaction-id))]
    (with-open [w (io/writer (str export-path "/post-codes.json") :append true)]
      (doseq [post-code mapped-post-codes]
        (.write w (str (json/generate-string post-code) "\n"))))))

(defn export-roads
  [transaction-id export-path]
  (let [mapped-roads (map dawa/map-road (dawa/get-roads transaction-id))]
    (with-open [w (io/writer (str export-path "/roads.json") :append true)]
      (doseq [roads mapped-roads]
        (.write w (str (json/generate-string roads) "\n"))))))

(defn start-bulk-import
  [export-path]
  (let [transaction-id (:id (dawa/get-latest-transaction-id))
        export-access-addresses-future (future (export-access-addresses transaction-id export-path))
        export-unit-addresses-future (future (export-unit-addresses transaction-id export-path))
        export-post-codes-future (future (export-post-codes transaction-id export-path))
        export-road-future (future (export-roads transaction-id export-path))]
    @export-post-codes-future
    @export-road-future
    @export-unit-addresses-future
    @export-access-addresses-future))

(defn -main [& args]
  (let [[export-path] args]
    (println "Starting bulk import.")
    (start-bulk-import export-path)
    (println "Finished import.")
    (shutdown-agents)))
