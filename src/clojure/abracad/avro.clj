(ns abracad.avro
  "Functions for de/serializing data with Avro."
  (:refer-clojure :exclude [compare spit slurp])
  (:require [abracad.avro.util :as util]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.walk :as walk])
  (:import [abracad.avro ClojureDatumReader ClojureDatumWriter]
           [clojure.lang Named]
           [java.io ByteArrayOutputStream EOFException File InputStream
            OutputStream]
           [org.apache.avro Schema Schema$Parser]
           [org.apache.avro.file CodecFactory DataFileReader DataFileStream
            DataFileWriter SeekableByteArrayInput SeekableFileInput
            SeekableInput]
           [org.apache.avro.io DatumReader DatumWriter Decoder DecoderFactory
            Encoder EncoderFactory]))

(defn schema?
  "True iff `schema` is an Avro `Schema` instance."
  [schema] (instance? Schema schema))

(defn ^:private named?
  "True iff `x` is something which may be provided as an argument to `name`."
  [x] (or (string? x) (instance? Named x)))

(defn ^:private schema-mangle
  "Mangle `named` forms and existing schemas."
  [form]
  (cond (named? form) (-> form name util/mangle)
        (schema? form) (json/parse-string (str form))
        :else form))

(defn ^:private clj->json
  "Parse Clojure data into a JSON schema."
  [schema] (json/generate-string (walk/postwalk schema-mangle schema)))

(defn ^:private ^CodecFactory codec-for
  "Return Avro codec factory for `codec`."
  [codec] (if-not (string? codec) codec (CodecFactory/fromString codec)))

(defprotocol PSeekableInput
  "Protocol for coercing to an Avro `SeekableInput`."
  (-seekable-input [x opts]
    "Attempt to util/coerce `x` to an Avro `SeekableInput`."))

(defn ^SeekableInput seekable-input
  "Attempt to util/coerce `x` to an Avro `SeekableInput`."
  ([x] (-seekable-input x nil))
  ([opts x] (-seekable-input x opts)))

(extend-protocol PSeekableInput
  (Class/forName "[B") (-seekable-input [x opts] (SeekableByteArrayInput. x))
  SeekableInput (-seekable-input [x opts] x)
  File (-seekable-input [x opts] (SeekableFileInput. x))
  String (-seekable-input [x opts] (seekable-input opts (io/file x))))

(defn ^:private raw-schema?
  "True if schema `source` should be parsed as-is."
  [source]
  (or (instance? InputStream source)
      (and (string? source)
           (.lookingAt (re-matcher #"[\[\{\"]" source)))))

(defn ^:private parse-schema-raw
  [^Schema$Parser parser source]
  (if (instance? String source)
    (.parse parser ^String source)
    (.parse parser ^InputStream source)))

(defn ^:private parse-schema* ^Schema
  [& sources]
  (let [parser (Schema$Parser.)]
    (reduce (fn [_ source]
              (->> (cond (schema? source) (str source)
                         (raw-schema? source) source
                         :else (clj->json source))
                   (parse-schema-raw parser)))
            nil
            sources)))

(defn ^Schema parse-schema
  "Parse Avro schemas in `source` and `sources`.  Each schema source may be a
JSON string, an input stream containing a JSON schema, a Clojure data structure
which may be converted to a JSON schema, or an already-parsed Avro schema
object.  The schema for each subsequent source may refer to the types defined in
the previous schemas.  The parsed schema from the final source is returned."
  ;;TODO why is this (schema? source) here?
  ([source] (if (schema? source) source (parse-schema* source)))
  ([source & sources] (apply parse-schema* source sources)))

(defn unparse-schema
  "Return Avro-normalized Clojure data version of `schema`.  If `schema` is not
already a parsed schema, will first normalize and parse it."
  [schema] (-> schema parse-schema str (json/parse-string true)))

(defn tuple-schema
  "Return Clojure-data Avro schema for record consisting of fields of the
provided `types`, and optionally named `name`."
  ([types] (-> "abracad.avro.tuple" gensym name (tuple-schema types)))
  ([name types]
     {:name name, :type "record",
      :abracad.reader "vector",
      :fields (vec (map-indexed (fn [i type]
                                  (merge {:name (str "field" i),
                                          :type type}
                                         (meta type)))
                                types))}))

(defn ^:private order-ignore
  "Update all but the first `n` record-field specifiers `fields` to have an
`:order` of \"ignore\"."
  [fields n]
  (vec (map-indexed (fn [i field]
                      (if (< i n)
                        field
                        (assoc field :order "ignore")))
                    fields)))

(defn grouping-schema
  "Produce a grouping schema version of record schema `schema` which ignores all
but the first `n` fields when sorting."
  [n schema] (-> schema unparse-schema (update :fields order-ignore n)))

(defn ^ClojureDatumReader datum-reader
  "Return an Avro DatumReader which produces Clojure data structures."
  ([] (ClojureDatumReader.))
  ([schema]
     (ClojureDatumReader.
      (if-not (nil? schema) (parse-schema schema))))
  ([expected actual]
     (ClojureDatumReader.
      (if-not (nil? expected) (parse-schema expected))
      (if-not (nil? actual) (parse-schema actual)))))

(defn ^DataFileReader data-file-reader
  "Return an Avro DataFileReader which produces Clojure data structures."
  ([source] (data-file-reader nil source))
  ([expected source]
     (DataFileReader/openReader
      ^SeekableInput (seekable-input source) ^DatumReader (datum-reader expected))))

(defn ^DataFileStream data-file-stream
  "Return an Avro DataFileStream which produces Clojure data structures."
  ([source] (data-file-stream nil source))
  ([expected source]
     (DataFileStream.
       (io/input-stream source) (datum-reader expected))))

(defmacro ^:private decoder-factory
  "Invoke static methods of default Avro Decoder factory."
  [method & args] `(. (DecoderFactory/get) ~method ~@args))

(defn ^Decoder binary-decoder
  "Return a binary-encoding decoder for `source`.  The `source` may be
an input stream, a byte array, or a vector of `[bytes off len]`."
  [source]
  (if (vector? source)
    (let [[source off len] source]
      (decoder-factory binaryDecoder source off len nil))
    (if (instance? InputStream source)
      (decoder-factory binaryDecoder ^InputStream source nil)
      (decoder-factory binaryDecoder ^bytes source nil))))

(defn ^Decoder direct-binary-decoder
  "Return a non-buffered binary-encoding decoder for `source`."
  [source] (decoder-factory directBinaryDecoder source nil))

(defn ^Decoder json-decoder
  "Return a JSON-encoding decoder for `source` using `schema`."
  [schema source]
  (let [schema ^Schema (parse-schema schema)]
    (if (instance? InputStream source)
      (decoder-factory jsonDecoder schema ^InputStream source)
      (decoder-factory jsonDecoder schema ^String source))))

(defn decode
  "Decode and return one object from `source` using `schema`.  The
`source` may be an existing Decoder object or anything on which
a (binary-encoding) Decoder may be opened."
  [schema source]
  (let [reader (util/coerce DatumReader datum-reader schema)
        decoder (util/coerce Decoder binary-decoder source)]
    (.read ^DatumReader reader nil decoder)))

(defn decode-seq
  "As per `decode`, but decode and return a sequence of all objects
decoded serially from `source`."
  [schema source]
  (let [reader (util/coerce DatumReader datum-reader schema)
        decoder (util/coerce Decoder binary-decoder source)]
    ((fn step []
       (lazy-seq
        (try
          (let [record (.read ^DatumReader reader nil decoder)]
            (cons record (step)))
          (catch EOFException _ nil)))))))

(defn ^ClojureDatumWriter datum-writer
  "Return an Avro DatumWriter which consumes Clojure data structures."
  ([] (ClojureDatumWriter.))
  ([schema]
     (ClojureDatumWriter.
      (if-not (nil? schema) (parse-schema schema)))))

(defn ^DataFileWriter data-file-writer
  "Return an Avro DataFileWriter which consumes Clojure data structures."
  ([] (DataFileWriter. (datum-writer)))
  ([sink]
     (let [^DataFileWriter dfw (data-file-writer)]
       (doto dfw (.appendTo (io/file sink)))))
  ([schema sink]
     (data-file-writer nil schema sink))
  ([codec schema sink]
   (let [^DataFileWriter dfw (data-file-writer)
         sink (util/coerce OutputStream io/output-stream sink)
         schema (parse-schema schema)]
     (when codec
       (.setCodec dfw (codec-for codec)))
     (.create dfw  ^Schema schema ^OutputStream sink)
     dfw)))

(defmacro ^:private encoder-factory
  "Invoke static methods of default Avro Encoder factory."
  [method & args] `(. (EncoderFactory/get) ~method ~@args))

(defn ^Encoder binary-encoder
  "Return a binary-encoding encoder for `sink`."
  [sink] (encoder-factory binaryEncoder sink nil))

(defn ^Encoder direct-binary-encoder
  "Return an unbuffered binary-encoding encoder for `sink`."
  [sink] (encoder-factory directBinaryEncoder sink nil))

(defn ^Encoder json-encoder
  "Return a JSON-encoding encoder for `sink` using `schema`."
  [schema sink]
  (let [schema (parse-schema schema)]
    (encoder-factory jsonEncoder ^Schema schema ^OutputStream sink)))

(defn encode
  "Serially encode each record in `records` to `sink` using `schema`.
The `sink` may be an existing Encoder object, or anything on which
a (binary-encoding) Encoder may be opened."
  [schema sink & records]
  (let [writer (util/coerce DatumWriter datum-writer schema)
        encoder (util/coerce Encoder binary-encoder sink)]
    (doseq [record records]
      (.write ^DatumWriter writer record encoder))
    (.flush ^Encoder encoder)))

(defn binary-encoded
  "Return bytes produced by binary-encoding `records` with `schema`
via `encode`."
  [schema & records]
  (with-open [out (ByteArrayOutputStream.)]
    (apply encode schema out records)
    (.toByteArray out)))

(defn json-encoded
  "Return string produced by JSON-encoding `records` with `schema`
via `encode`."
  [schema & records]
  (with-open [out (ByteArrayOutputStream.)]
    (apply encode schema (json-encoder schema out) records)
    (String. (.toByteArray out))))

(defn spit
  "Like core `spit`, but emits `content` to `f` as Avro with `schema`."
  [schema f content & opts]
  (let [codec (get opts :codec "snappy")]
    (with-open [dfw ^DataFileWriter (data-file-writer codec schema f)]
      (.append dfw content))))

(defn slurp
  "Like core `slurp`, but reads Avro content from `f`."
  [f & opts]
  (with-open [dfr ^DataFileReader (data-file-reader f)]
    (.next dfr)))

(defn mspit
  "Like Avro `spit`, but emits `content` as a sequence of records."
  [schema f content & opts]
  (let [codec (get opts :codec "snappy")]
    (with-open [dfw ^DataFileWriter (data-file-writer codec schema f)]
      (doseq [record content]
        (.append dfw record)))))

(defn mslurp
  "Like Avro `slurp`, but produces a sequence of records."
  [f & opts]
  (with-open [dfr ^DataFileReader (data-file-reader f)]
    (into [] dfr)))
