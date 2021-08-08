(ns danish-address-search-indexer.dawa
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.instant :as instant]))

(def dawa-base-url "https://api.dataforsyningen.dk/replikering")

(declare handle-response)

(defn get-latest-transaction-id
  "Gets the latest transaction id from DAWA."
  []
  (let [result
        (-> (client/get (str dawa-base-url "/senestetransaktion"))
            :body
            (json/parse-string true))]
    {:transaction-id (:txid result)
     :timestamp (->> result :tidspunkt instant/read-instant-timestamp)}))

(defn get-post-codes
  "Retrieves address post-codes from DAWA."
  [transaction-id]
  (->> (client/get (str dawa-base-url "/udtraek?entitet=postnummer&ndjson&txid=" transaction-id))
       handle-response))

(defn- handle-response [http-response]
  (->> http-response
       :body
       (string/split-lines)
       (map #(json/parse-string % true))))
