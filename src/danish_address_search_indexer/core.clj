(ns danish-address-search-indexer.core
  (:require [danish-address-search-indexer.dawa :as dawa]))

(defn import-access-addresses
  [transaction-id]
  (dawa/get-all-access-addresses
   transaction-id
   #(dawa/map-access-address %)))

(defn import-unit-addresses
  [transaction-id]
  (dawa/get-all-unit-addresses
   transaction-id
   #(dawa/map-unit-address %)))

(defn start-bulk-import
  []
  (let [latest-transaction (dawa/get-latest-transaction-id)
        transaction-id (:id latest-transaction)]
    (import-access-addresses transaction-id)
    (import-unit-addresses transaction-id)))

(defn -main []
  (start-bulk-import))
