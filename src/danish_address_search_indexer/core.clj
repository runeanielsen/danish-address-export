(ns danish-address-search-indexer.core
  (:require [danish-address-search-indexer.dawa :as dawa]
            [clojure.core.async :refer [<! >!! go chan close!]]))

(defn batch-xf [max-size]
  (comp cat (partition-all max-size)))

(defn import-access-addresses
  [transaction-id]
  (let [out (chan 1 (batch-xf 500))]
    (go (loop [total 0]
          (println total)
          (when-let [addresses (<! out)]
            (map dawa/map-access-address addresses)
            (recur (+ total (count addresses))))))
    (dawa/get-all-access-addresses
     transaction-id
     (fn [dawa-acesss-address]
       (>!! out [dawa-acesss-address])))
    (close! out)))

(defn start-bulk-import
  []
  (let [latest-transaction (dawa/get-latest-transaction-id)
        transaction-id (:id latest-transaction)]
    (import-access-addresses transaction-id)))

(defn -main []
  (start-bulk-import))
