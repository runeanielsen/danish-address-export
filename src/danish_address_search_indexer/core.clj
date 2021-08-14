(ns danish-address-search-indexer.core
  (:require [danish-address-search-indexer.dawa :as dawa]))

(declare start-import)
(declare start-import-no-future)
(declare map-unit-addresses)

(defn -main []
  (map-unit-addresses 500))

(defn start-import
  "Starts the dawa import"
  []
  (let [transaction-id 0
        roads (future (dawa/get-roads transaction-id))
        post-codes (future (dawa/get-post-codes transaction-id))]))

(defn map-unit [unit-address]
  (into {:access-address-id (:adgangsadresseid unit-address)
         :status (:status unit-address)
         :floor (:etage unit-address)
         :door (:dør unit-address)
         :id (:id unit-address)
         :created (:oprettet unit-address)
         :updated (:ændret unit-address)}))

(defn map-unit-addresses
  [batch-size]
  (let [addresses (atom [])
        imported-count (atom 0)]
    (dawa/get-all-unit-addresses
     0
     #(doseq [elem %]
        (swap! addresses conj (map-unit elem))
        (when (= (count @addresses) batch-size)
          (swap! imported-count + batch-size)
          (reset! addresses [])
          (println "Imported: " @imported-count))))
    (swap! imported-count + (count @addresses))
    (println "Total imported: " @imported-count)))
