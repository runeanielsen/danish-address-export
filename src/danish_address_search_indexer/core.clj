(ns danish-address-search-indexer.core
  (:require [danish-address-search-indexer.dawa :as dawa]))

(declare start-import)
(declare start-import-no-future)
(declare import-unit-addresses)
(declare import-access-addresses)

(defn -main []
  (println "Starting import access addresses")
  (import-access-addresses 500)
  (println "Starting import unit addresses")
  (import-unit-addresses 500))

(defn start-import
  "Starts the dawa import"
  []
  (let [transaction-id 0
        roads (future (dawa/get-roads transaction-id))
        post-codes (future (dawa/get-post-codes transaction-id))]))

(defn map-unit-addresses [unit-address]
  (into {:access-address-id (:adgangsadresseid unit-address)
         :status (:status unit-address)
         :floor (:etage unit-address)
         :door (:dør unit-address)
         :id (:id unit-address)
         :created (:oprettet unit-address)
         :updated (:ændret unit-address)}))

(defn map-access-addresses [access-address]
  (into {:access-address-id (:adgangsadresseid access-address)
         :status (:status access-address)
         :id (:id access-address)
         :road-code (:vejkode access-address)
         :house-number (:husnr access-address)
         :post-district-code (:postnr access-address)
         :east-coordinate (:etrs89koordinat_øst access-address)
         :north-coordinate (:etrs89koordinat_nord access-address)
         :location-updated (:adressepunktændringsdato access-address)
         :town-name (:supplerendebynavn access-address)
         :plot-id (:matrikelnr access-address)
         :road-id (:navngivenvej_id access-address)
         :municipal-code (:kommunekode access-address)
         :created (:oprettet access-address)
         :updated (:ændret access-address)}))

(defn import-access-addresses
  [batch-size]
  (let [addresses (atom [])
        imported-count (atom 0)]
    (dawa/get-all-access-addresses
     0
     #(swap! addresses conj (map-access-addresses %)
             (when (= (count @addresses) batch-size)
               (swap! imported-count + batch-size)
               (reset! addresses [])
               (println "Imported: " @imported-count))))
    (swap! imported-count + (count @addresses))
    (println "Total imported: " @imported-count)))

(defn import-unit-addresses
  [batch-size]
  (let [addresses (atom [])
        imported-count (atom 0)]
    (dawa/get-all-unit-addresses
     0
     #(swap! addresses conj (map-unit-addresses %)
             (when (= (count @addresses) batch-size)
               (swap! imported-count + batch-size)
               (reset! addresses [])
               (println "Imported: " @imported-count))))
    (swap! imported-count + (count @addresses))
    (println "Total imported: " @imported-count)))
