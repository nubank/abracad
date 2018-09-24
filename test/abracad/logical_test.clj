(ns abracad.logical-test
  (:require [abracad.avro :as avro]
            [midje.sweet :refer :all])
  (:import java.nio.ByteBuffer
           java.util.UUID))

(defmethod avro/write-logical "uuid" [_ _ ^UUID datum]
  (let [b (ByteBuffer/wrap (byte-array 16))]
    (.putLong b (.getMostSignificantBits datum))
    (.putLong b (.getLeastSignificantBits datum))
    (.array b)))

(defmethod avro/read-logical "uuid" [_ _ ^bytes datum]
  (let [b (ByteBuffer/wrap datum)]
    (UUID. (.getLong b) (.getLong b))))

(fact "we can encode/decode data using logical types"
   (let [schema (avro/parse-schema {:type :fixed
                                    :size 16
                                    :name :UUID
                                    :logicalType :uuid})
         uuid (UUID/randomUUID)]
     (->> (avro/binary-encoded schema uuid)
          (avro/decode-seq schema)
          (first)) => uuid))
