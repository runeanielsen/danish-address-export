(ns danish-address-search-indexer.core
  (:require [danish-address-search-indexer.dawa :as dawa]
            [cheshire.core :as json]
            [clojure.java.io :as io]))

(defn import-access-addresses
  [transaction-id export-path]
  (dawa/get-all-access-addresses
   transaction-id
   (fn [dawa-address]
     (let [mapped-address (dawa/map-access-address dawa-address)]
       (with-open [w (io/writer (str export-path "/access-addresses.json") :append true)]
         (.write w (str (json/generate-string mapped-address) "\n")))))))

(defn import-unit-addresses
  [transaction-id export-path]
  (dawa/get-all-unit-addresses
   transaction-id
   (fn [dawa-address]
     (let [mapped-address (dawa/map-unit-address dawa-address)]
       (with-open [w (io/writer (str export-path "/unit-addresses.json") :append true)]
         (.write w (str (json/generate-string mapped-address) "\n")))))))

(defn start-bulk-import
  [export-path]
  (let [latest-transaction (dawa/get-latest-transaction-id)
        transaction-id (:id latest-transaction)
        import-aa-future (future (import-access-addresses transaction-id export-path))
        import-ua-future (future (import-unit-addresses transaction-id export-path))]
    @import-aa-future
    @import-ua-future))

(defn -main [& args]
  (let [[export-path] args]
    (println "Starting bulk import.")
    (start-bulk-import export-path)
    (println "Finished import.")))
