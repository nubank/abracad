(ns abracad.avro.write
  "Generic data writing implementation."
  {:private true}
  (:require [abracad.avro.util :as util])
  (:import [abracad.avro ArrayAccessor ClojureDatumWriter]
           [clojure.lang Indexed IRecord Named Sequential]
           [java.nio ByteBuffer]
           [java.util Collection Map]
           [org.apache.avro AvroTypeException Schema Schema$Field Schema$Type]
           [org.apache.avro.generic GenericRecord]
           [org.apache.avro.io Encoder]))

(def ^:dynamic *unchecked*
  "When `true`, do not perform schema field validation checks during
record serialization."
  false)

(defn element-union?
  [^Schema schema]
  (= (.getType schema) Schema$Type/UNION))

(defn edn-schema?
  [^Schema schema]
  (= "abracad.avro.edn" (.getNamespace schema)))

(defn namespaced?
  [x] (instance? Named x))

(defn named?
  [x] (or (symbol? x) (keyword? x) (string? x)))

(def ^:const bytes-class
  (Class/forName "[B"))

(defprotocol HandleBytes
  (count-bytes [this])
  (emit-bytes [this encoder])
  (emit-fixed [this encoder]))

(extend-type (Class/forName "[B")
  HandleBytes
  (count-bytes [bytes] (alength ^bytes bytes))
  (emit-bytes [bytes encoder]
    (.writeBytes ^Encoder encoder ^bytes bytes))
  (emit-fixed [bytes encoder]
    (.writeFixed ^Encoder encoder ^bytes bytes)))

(extend-type ByteBuffer
  HandleBytes
  (count-bytes [^ByteBuffer bytes] (.remaining bytes))
  (emit-bytes [^ByteBuffer bytes ^Encoder encoder]
    (.writeBytes encoder bytes))
  (emit-fixed [^ByteBuffer bytes ^Encoder encoder]
    (.writeFixed encoder bytes)))

(defn schema-error!
  [^Schema schema datum]
  (throw (ex-info "Cannot write datum as schema"
                  {:datum datum, :schema (.getFullName schema)})))

(defn wr-named
  [^ClojureDatumWriter writer ^Schema schema datum ^Encoder out]
  (doseq [^Schema$Field f (.getFields schema)
          :let [key (util/field-keyword f), val (get datum key)]]
    (.write writer (.schema f) val out)))

(defn wr-named-checked
  [^ClojureDatumWriter writer ^Schema schema datum ^Encoder out]
  (let [fields (into #{} (map util/field-keyword (.getFields schema)))]
    (when (not-every? fields (keys datum))
      (schema-error! schema datum))
    (doseq [^Schema$Field f (.getFields schema)
            :let [key (util/field-keyword f), val (get datum key)]]
      (.write writer (.schema f) val out))))

(defn wr-positional
  [^ClojureDatumWriter writer ^Schema schema datum ^Encoder out]
  (let [fields (.getFields schema), nfields (count fields)]
    (when (not= nfields (count datum))
      (schema-error! schema datum))
    (dorun
     (map (fn [^Schema$Field f val] (.write writer (.schema f) val out))
          fields datum))))

(defn elide
  [^Schema schema]
  (.schema ^Schema$Field (first (.getFields schema))))

(defn write-record*
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (util/case-expr (.getFullName schema)
    (let [wrf (cond (instance? Indexed datum) wr-positional
                    *unchecked* wr-named
                    :else wr-named-checked)]
      (wrf writer schema datum out))))

(defn write-record
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (let [unchecked (-> datum meta (:avro/unchecked *unchecked*))]
    (if (= unchecked *unchecked*)
      (write-record* writer schema datum out)
      (binding [*unchecked* unchecked]
        (write-record* writer schema datum out)))))

(defn write-enum
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (.writeEnum out (.getEnumOrdinal schema (-> datum name util/mangle))))

(defn array-prim?
  [datum]
  (let [cls ^Class (class datum)]
    (and (-> cls .isArray)
         (-> cls .getComponentType .isPrimitive))))

(defn write-array-seq
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (.setItemCount out (count datum))
  (doseq [datum datum]
    (.startItem out)
    (.write writer schema datum out)))

(defn write-array
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (let [schema (.getElementType schema)]
    (.writeArrayStart out)
    (if (array-prim? datum)
      (ArrayAccessor/writeArray datum out)
      (write-array-seq writer schema datum out))
    (.writeArrayEnd out)))

(defn schema-name-type
  [datum]
  (let [t (type datum)]
    (cond (string? t) t
          (instance? Named t)
          , (let [ns (namespace t)
                  ns (if ns (util/mangle ns))
                  n (-> t name util/mangle)]
              (if ns (str ns "." n) n))
          (class? t) (.getName ^Class t))))

(defn field-name
  [^Schema$Field field] (keyword (.name field)))

(defn write-bytes
  [^ClojureDatumWriter writer datum ^Encoder out]
  (emit-bytes datum out))
