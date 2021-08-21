(ns danish-address-search-indexer.core
  (:require [danish-address-search-indexer.dawa :as dawa]
            [clojure.core.async :refer [<! >!! go chan]]))

(defn import-access-addresses
  [transaction-id]
  (let [println-ch (chan)]
    (go (loop [n 0]
          (when (zero? (mod n 10000))
            (println n))
          (dawa/map-access-address (<! println-ch))
          (recur (inc n))))
    (dawa/get-all-access-addresses
     transaction-id
     (fn [dawa-acesss-address]
       (>!! println-ch dawa-acesss-address)))))

(defn import-unit-addresses
  [transaction-id]
  (dawa/get-all-unit-addresses
   transaction-id
   (fn [dawa-unit-address]
     (let [unit-address (dawa/map-unit-address dawa-unit-address)]
       (println unit-address)))))

(defn start-bulk-import
  []
  (let [latest-transaction (dawa/get-latest-transaction-id)
        transaction-id (:id latest-transaction)]
    (time
     (import-access-addresses transaction-id))))

(defn -main []
  (start-bulk-import))
