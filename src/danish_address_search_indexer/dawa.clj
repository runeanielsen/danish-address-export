(ns danish-address-search-indexer.dawa
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.instant :as instant]))

(def dawa-base-url "https://api.dataforsyningen.dk/replikering")

(declare parse-jsonline-response)
(declare stream-jsonline-response)

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
  (let [url (str dawa-base-url "/udtraek?entitet=adresse&ndjson&txid=" transaction-id)]
    (stream-jsonline-response url f)))

(defn get-all-access-addresses
  "Retrieves all access-addresses from DAWA."
  [transaction-id f]
  (let [url (str dawa-base-url "/udtraek?entitet=adgangsadresse&ndjson&txid=" transaction-id)]
    (stream-jsonline-response url f)))

(defn get-post-codes
  "Retrieves address post-codes from DAWA."
  [transaction-id]
  (->> (client/get (str dawa-base-url "/udtraek?entitet=postnummer&ndjson&txid=" transaction-id))
       parse-jsonline-response))

(defn get-roads
  "Gets roads from DAWA."
  [transaction-id]
  (->> (client/get (str dawa-base-url "/udtraek?entitet=navngivenvej&ndjson&txid=" transaction-id))
       parse-jsonline-response))

(defn map-status
  "Maps DAWA status code."
  [status-code]
  (case status-code
    1 :active
    2 :canceled
    3 :pending
    4 :discontinued))

(defn map-unit-address
  "Maps DAWA unit-address to domain representation."
  [unit-address]
  {:access-address-id (:adgangsadresseid unit-address)
   :status (map-status (:status unit-address))
   :floor (:etage unit-address)
   :door (:dør unit-address)
   :id (:id unit-address)
   :created (:oprettet unit-address)
   :updated (:ændret unit-address)})

(defn map-access-address
  "Maps DAWA access-address to domain representation."
  [access-address]
  {:access-address-id (:adgangsadresseid access-address)
   :status (map-status (:status access-address))
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
   :updated (:ændret access-address)})

(defn- stream-jsonline-response
  [url f]
  (let [response (client/get url {:as :reader})]
    (with-open [reader (:body response)]
      (doseq [line (line-seq reader)]
        (f (json/parse-string line true))))))

(defn- parse-jsonline-response [http-response]
  (->> http-response
       :body
       (string/split-lines)
       (map #(json/parse-string % true))))
