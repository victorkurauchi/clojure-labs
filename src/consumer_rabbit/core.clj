(ns consumer-rabbit.core
  (:gen-class)
  (:require [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [monger.core :as mg]
            [monger.collection :as mc]
            [clojure.data.generators :as gen]
            [langohr.basic     :as lb])
  (:import [com.mongodb MongoOptions ServerAddress]
           [org.bson.types ObjectId]
           [com.mongodb DB WriteConcern]
           )
  )

;; localhost, default port
(defn get-db []
(let [conn (mg/connect)
      db   (mg/get-db conn "monger-test")
      coll "documents"] db)
  )

(def ^{:const true}
  default-exchange-name "")

; (mc/insert-and-return (get-db) "documents" {:_id (ObjectId.) :message (apply message (generate-string))})

;use java's random generator
(def random (java.util.Random.))

;define characters list to use to generate string
(def chars
  (map char (concat (range 48 58) (range 66 92) (range 97 123))))

;generates 1 random character
(defn random-char []
  (nth chars (.nextInt random (count chars))))

; generates random string of length characters
(defn random-string [length]
  (apply str (take length (repeatedly random-char))))

(defn get-message [str]
   (concat str (random-string 10) ))

(defn save
  [message]
  (
   (mc/insert-and-return (get-db) "documents" {:_id (ObjectId.) :message message})
  ;  (mc/insert-and-return (get-db) "documents" {:_id (ObjectId.) :message (random-string 10)})
   )
)

(defn message-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                   (String. payload "UTF-8") delivery-tag content-type type))
  (save {:message (String. payload "UTF-8")})
  )

(defn -main
  [& args]
  (let [conn  (rmq/connect)
        ch    (lch/open conn)
        qname "langohr.examples.hello-world"]
    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    (lq/declare ch qname {:exclusive false :auto-delete false :durable true :arguments { "x-queue-type" "classic" }})
    (lc/subscribe ch qname message-handler {:auto-ack true})
    (lb/publish ch default-exchange-name qname "Hello!" {:content-type "text/plain" :type "greetings.hi"})
    (Thread/sleep 200000)
    (println "[main] Disconnecting...")
    (rmq/close ch)
    (rmq/close conn)))
