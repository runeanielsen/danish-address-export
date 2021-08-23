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
  (let [result (-> (client/get (str dawa-base-url "/senestetransaktion"))
                   :body
                   (json/parse-string true))]
    {:id (:txid result)
     :timestamp (->> result :tidspunkt instant/read-instant-timestamp)}))

(defn get-all-unit-addresses
  "Retrieves all unit-addresses from DAWA."
  [transaction-id callback]
  (let [url (str dawa-base-url "/udtraek?entitet=adresse&ndjson&txid=" transaction-id)]
    (stream-jsonline-response url callback)))

(defn get-all-access-addresses
  "Retrieves all access-addresses from DAWA."
  [transaction-id callback]
  (let [url (str dawa-base-url "/udtraek?entitet=adgangsadresse&ndjson&txid=" transaction-id)]
    (stream-jsonline-response url callback)))

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

(defn map-post-code
  "Maps DAWA post-code to domain representation."
  [dawa-post-code]
  {:id (:id dawa-post-code)
   :name (:navn dawa-post-code)})

(defn map-road
  "Maps DAWA road to domain representation."
  [dawa-road]
  {:id (:id dawa-road)
   :name (:navn dawa-road)})

(defn map-unit-address
  "Maps DAWA unit-address to domain representation."
  [dawa-unit-address]
  {:access-address-id (:adgangsadresseid dawa-unit-address)
   :status (map-status (:status dawa-unit-address))
   :floor (:etage dawa-unit-address)
   :door (:dør dawa-unit-address)
   :id (:id dawa-unit-address)
   :created (:oprettet dawa-unit-address)
   :updated (:ændret dawa-unit-address)})

(defn map-access-address
  "Maps DAWA access-address to domain representation."
  [dawa-access-address]
  {:access-address-id (:adgangsadresseid dawa-access-address)
   :status (map-status (:status dawa-access-address))
   :id (:id dawa-access-address)
   :road-code (:vejkode dawa-access-address)
   :house-number (:husnr dawa-access-address)
   :post-district-code (:postnr dawa-access-address)
   :east-coordinate (:etrs89koordinat_øst dawa-access-address)
   :north-coordinate (:etrs89koordinat_nord dawa-access-address)
   :location-updated (:adressepunktændringsdato dawa-access-address)
   :town-name (:supplerendebynavn dawa-access-address)
   :plot-id (:matrikelnr dawa-access-address)
   :road-id (:navngivenvej_id dawa-access-address)
   :municipal-code (:kommunekode dawa-access-address)
   :created (:oprettet dawa-access-address)
   :updated (:ændret dawa-access-address)})

(defn- stream-jsonline-response
  [url callback]
  (let [response (client/get url {:as :reader})]
    (with-open [reader (:body response)]
      (doseq [line (line-seq reader)]
        (callback (json/parse-string line true))))))

(defn- parse-jsonline-response [http-response]
  (->> http-response
       :body
       (string/split-lines)
       (map #(json/parse-string % true))))
