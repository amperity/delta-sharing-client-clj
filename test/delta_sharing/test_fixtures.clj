(ns delta-sharing.test-fixtures
  (:require
    [clojure.data.json :as json]
    [delta-sharing.util :as cu]))


(defn- edn->json
  ([m]
   (edn->json m cu/camelify-keys))
  ([m key-fn]
   (-> m (key-fn) (json/write-str))))


(def protocol-reader-v1
  {:protocol {:min-reader-version 1}})


(def json-protocol-reader-v1
  (edn->json {"protocol" {"minReaderVersion" 1}}))


(def protocol-reader-v3
  {:protocol {:min-reader-version 3
              :min-writer-version 7
              :reader-features ["deletionVectors"]
              :writer-features ["deletionVectors"]}})


(def json-protocol-reader-v3
  (edn->json {"protocol" {"deltaProtocol" {"minReaderVersion" 3
                                           "minWriterVersion" 7
                                           "readerFeatures" ["deletionVectors"]
                                           "writerFeatures" ["deletionVectors"]}}}))


(def metadata-response
  {:partition-columns []
   :format {:provider "parquet"}
   :schema {:type "struct"
            :fields [{:name "foo"
                      :type "string"
                      :nullable true}
                     {:name "date"
                      :type "date"
                      :nullable true}]}
   :id "abc123"
   :configuration {:enable-change-data-feed "true"}
   :version 0
   :size 123456
   :num-files 5})


(def json-parquet-metadata
  (edn->json
    {"metaData"
     {"partitionColumns" []
      "format" {"provider" "parquet"}
      "schemaString" (edn->json {:type "struct"
                                 :fields [{:name "foo"
                                           :type "string"
                                           :nullable true}
                                          {:name "date"
                                           :type "date"
                                           :nullable true}]})
      "id" "abc123"
      "configuration" {}
      "version" 0
      "size" 123456
      "numFiles" 5}}))


(def json-delta-metadata
  (edn->json {"metaData"
              {"deltaMetadata" {"partitionColumns" []
                                "format" {"provider" "parquet"}
                                "schemaString" (edn->json {:type "struct"
                                                           :fields [{:name "foo"
                                                                     :type "string"
                                                                     :nullable true}
                                                                    {:name "date"
                                                                     :type "date"
                                                                     :nullable true}]})
                                "id" "abc123"
                                "configuration" {"enableChangeDataFeed" "true"}}
               "version" 0
               "size" 123456
               "numFiles" 5}}))


;; ## Parquet file response


(def parsed-parquet-file
  {:url "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
   :id "8b0086f2"
   :partition-values {:date "2021-04-28"}
   :size 573
   :stats {:num-records 1
           :min-values {"date" "2021-04-28"}
           :max-values {"date" "2021-04-28"}
           :null-count 0}
   :expiration-timestamp 1652140800000})


(def json-parquet-file
  (edn->json
    {"file"
     {"url" "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
      "id" "8b0086f2"
      "partitionValues" {"date" "2021-04-28"}
      "size" 573
      "stats" (edn->json
                {"numRecords" 1
                 "minValues" {"date" "2021-04-28"}
                 "maxValues" {"date" "2021-04-28"}
                 "nullCount" 0})
      "expirationTimestamp" 1652140800000}}))


;; ## Delta table file repsonse 


(def parsed-delta-file
  {:url "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
   :id "file-1"
   :size 573
   :delta-single-action {:action :add
                         :data-change true
                         :partition-values {:date "2021-04-28"}
                         :modification-time 1715276581000
                         :stats {:num-records 1
                                 :min-values {"date" "2021-04-28"}
                                 :max-values {"date" "2021-04-28"}
                                 :null-count 0}
                         :path "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"}
   :expiration-timestamp 1652140800000})


(def json-delta-file
  (edn->json
    {"file"
     {"url" "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
      "id" "file-1"
      "size" 573
      "deltaSingleAction" {"add" {"dataChange" true
                                  "partitionValues" {"date" "2021-04-28"}
                                  "modificationTime" 1715276581000
                                  "stats" (edn->json {"numRecords" 1
                                                      "minValues" {"date" "2021-04-28"}
                                                      "maxValues" {"date" "2021-04-28"}
                                                      "nullCount" 0})
                                  "path" "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"}}
      "expirationTimestamp" 1652140800000}}))


;; ## CDF responses

(def parsed-parquet-add-action
  {:action :add
   :url "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
   :id "file-1"
   :partition-values {:date "2021-04-28"}
   :timestamp 1652140800000
   :size 573
   :stats {:num-records 1
           :min-values {"date" "2021-04-28"}
           :max-values {"date" "2021-04-28"}
           :null-count 0}
   :expiration-timestamp 1652140800000
   :version 0})


(def json-parquet-add-action
  (edn->json
    {"add"
     {"url" "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
      "id" "file-1"
      "partitionValues" {"date" "2021-04-28"}
      "size" 573
      "timestamp" 1652140800000
      "stats" (edn->json
                {"numRecords" 1
                 "minValues" {"date" "2021-04-28"}
                 "maxValues" {"date" "2021-04-28"}
                 "nullCount" 0})
      "expirationTimestamp" 1652140800000
      "version" 0}}))


(def parsed-parquet-metadata-change
  {:action :metadata
   :partition-columns []
   :format {:provider "parquet"}
   :schema {:type "struct"
            :fields [{:name "foo"
                      :type "string"
                      :nullable true}
                     {:name "date"
                      :type "date"
                      :nullable true}
                     {:name "new_column"
                      :type "string"
                      :nullable true}]}
   :id "abc123"
   :configuration {}
   :version 1
   :size 123456
   :num-files 5})


(def json-parquet-metadata-change
  (edn->json
    {"metaData"
     {"partitionColumns" []
      "format" {"provider" "parquet"}
      "schemaString" (edn->json {:type "struct"
                                 :fields [{:name "foo"
                                           :type "string"
                                           :nullable true}
                                          {:name "date"
                                           :type "date"
                                           :nullable true}
                                          {:name "new_column"
                                           :type "string"
                                           :nullable true}]})
      "id" "abc123"
      "configuration" {}
      "version" 1
      "size" 123456
      "numFiles" 5}}))


(def parsed-delta-add-action
  {:url "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
   :id "file-1"
   :delta-single-action {:action :add
                         :data-change true
                         :partition-values {:date "2021-04-28"}
                         :modification-time 1715276581000
                         :size 573
                         :stats {:num-records 1
                                 :min-values {"date" "2021-04-28"}
                                 :max-values {"date" "2021-04-28"}
                                 :null-count 0}
                         :path "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"}
   :expiration-timestamp 1652140800000
   :timestamp 1716594284000
   :version 0})


(def json-delta-add-action
  (edn->json
    {"file"
     {"url" "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"
      "id" "file-1"
      "deltaSingleAction" {"add" {"dataChange" true
                                  "partitionValues" {"date" "2021-04-28"}
                                  "modificationTime" 1715276581000
                                  "size" 573
                                  "stats" (edn->json
                                            {"numRecords" 1
                                             "minValues" {"date" "2021-04-28"}
                                             "maxValues" {"date" "2021-04-28"}
                                             "nullCount" 0})
                                  "path" "https://<some-s3-bucket>.s3.us-west-2.amazonaws.com/file-1/part-00.parquet"}}
      "expirationTimestamp" 1652140800000
      "timestamp" 1716594284000
      "version" 0}}))


(def parsed-delta-metadata-change
  {:schema {:type "struct"
            :fields [{:name "foo"
                      :type "string"
                      :nullable true}
                     {:name "date"
                      :type "date"
                      :nullable true}
                     {:name "new_column"
                      :type "string"
                      :nullable true}]}
   :format {:provider "parquet"}
   :num-files 5
   :size 123456
   :id "abc123"
   :action :metadata
   :configuration {:enable-change-data-feed "true"}
   :version 1
   :partition-columns []})


(def json-delta-metadata-change
  (edn->json {"metaData"
              {"deltaMetadata" {"partitionColumns" []
                                "format" {"provider" "parquet"}
                                "schemaString" (edn->json {:type "struct"
                                                           :fields [{:name "foo"
                                                                     :type "string"
                                                                     :nullable true}
                                                                    {:name "date"
                                                                     :type "date"
                                                                     :nullable true}
                                                                    {:name "new_column"
                                                                     :type "string"
                                                                     :nullable true}]})
                                "id" "abc123"
                                "configuration" {"enableChangeDataFeed" "true"}}
               "version" 1
               "size" 123456
               "numFiles" 5}}))
