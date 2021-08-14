(ns danish-address-search-indexer.dawa
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.instant :as instant]))

(def dawa-base-url "https://api.dataforsyningen.dk/replikering")

(declare parse-response)
(declare handle-streaming-response)

(defn get-latest-transaction-id
  "Gets the latest transaction id from DAWA."
  []
  (let [result
        (-> (client/get (str dawa-base-url "/senestetransaktion"))
            :body
            (json/parse-string true))]
    {:transaction-id (:txid result)
     :timestamp (->> result :tidspunkt instant/read-instant-timestamp)}))

(defn get-all-unit-addresses
  "Retrieves all unit-addresses from DAWA."
  [transaction-id f]
  (let [url (str dawa-base-url "/udtraek?entitet=adresse&ndjson&txid=" transaction-id)
        response (client/get url {:as :reader})]
    (handle-streaming-response response f)))

(defn get-all-access-addresses
  "Retrieves all access-addresses from DAWA."
  [transaction-id f]
  (let [url (str dawa-base-url "/udtraek?entitet=adgangsadresse&ndjson&txid=" transaction-id)
        response (client/get url {:as :reader})]
    (handle-streaming-response response f)))

(defn get-post-codes
  "Retrieves address post-codes from DAWA."
  [transaction-id]
  (->> (client/get (str dawa-base-url "/udtraek?entitet=postnummer&ndjson&txid=" transaction-id))
       parse-response))

(defn get-roads
  "Gets roads from DAWA."
  [transaction-id]
  (->> (client/get (str dawa-base-url "/udtraek?entitet=navngivenvej&ndjson&txid=", transaction-id))
       parse-response))

(defn- handle-streaming-response
  [response f]
  (with-open [reader (:body response)]
    (doseq [line (line-seq reader)]
      (f (json/parse-string line true)))))

(defn- parse-response [http-response]
  (->> http-response
       :body
       (string/split-lines)
       (map #(json/parse-string % true))))
