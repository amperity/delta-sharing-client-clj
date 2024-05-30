(ns delta-sharing.client.http-test
  (:require
    [clojure.data.json :as json]
    [clojure.string :as str]
    [clojure.test :refer [are deftest is testing]]
    [delta-sharing.client.flow :as f]
    [delta-sharing.client.http :as dsc.http]
    [delta-sharing.client.proto :as dsc.proto]
    [delta-sharing.test-fixtures :as t.fixture]
    [delta-sharing.util :as cu]
    [org.httpkit.client :as http])
  (:import
    clojure.lang.ExceptionInfo))


;; ## Munged opts

(deftest delta-sharing-capabilities
  (testing "reader version options"
    (is (= {"delta-sharing-capabilities" "responseformat=parquet"}
           (#'dsc.http/select-reader-opts {}))
        "default minimum reader version should be 1")

    (is (= {"delta-sharing-capabilities"
            "responseformat=delta;readerfeatures=columnMapping,deletionVectors,timestampNtz,v2Checkpoint,vacuumProtocolCheck"}
           (#'dsc.http/select-reader-opts {:min-reader-version 3}))
        "supported minimum reader version should be respected")

    (is (= {"delta-sharing-capabilities" "responseformat=parquet"}
           (#'dsc.http/select-reader-opts {:min-reader-version 42}))
        "unsupported minimum reader version should be ignored")))


;; ## API call methods

(deftest prepare-request
  (let [mock-client {:auth (atom (cu/veil "token"))
                     :endpoint "https://example.com/delta-sharing/"}]
    (testing "base request payload"
      (is (= {:method :get
              :url (str (:endpoint mock-client) "shares")
              :headers {"authorization" "Bearer token"}}
             (#'dsc.http/prepare-request mock-client :get "shares" {}))))
    (testing "with query-params"
      (is (= {:method :get
              :url (str (:endpoint mock-client) "shares")
              :headers {"authorization" "Bearer token"}
              :query-params {:max-results 10
                             :next-token "abc123"}}
             (#'dsc.http/prepare-request mock-client :get "shares"
                                         {:query-params {:max-results 10
                                                         :next-token "abc123"}}))))
    (testing "with JSON body"
      (is (= {:method :get
              :url (str (:endpoint mock-client) "shares")
              :headers {"authorization" "Bearer token"
                        "Content-Type" "application/json; charset=utf-8"}
              :body "{\"limitHint\":100}"}
             (#'dsc.http/prepare-request mock-client :get "shares"
                                         {:headers {"Content-Type" "application/json; charset=utf-8"}
                                          :body {"limitHint" 100}}))))))


(deftest form-success
  (let [headers {:delta-table-version "1"
                 :server "databricks"}
        sample-get-share-body (json/write-str {:share {:id "abc123",
                                                       :name "ram_bugbash_inbound"}})]
    (testing "header response only"
      (let [path "shares/delta_sharing/schemas/default/tables/boston-housing/version"]
        (is (= [{:method :get
                 :path path
                 :api ::dsc.http/query-table-version
                 :delta-sharing.client/status 200
                 :delta-sharing.client/headers {:delta-table-version "1"
                                                :server "databricks"},
                 :delta-sharing.client/request-headers {"startingTimestamp" "2022-01-01T00:00:00Z"}}
                {}]
               (#'dsc.http/form-success 200 headers
                                        nil {:info {:headers {"startingTimestamp" "2022-01-01T00:00:00Z"}}
                                             :method :get
                                             :path path
                                             :api ::dsc.http/query-table-version}
                                        nil nil)))

        (testing "moves specified header keys into response data"
          (is (= [{:method :get
                   :path path
                   :api ::dsc.http/query-table-version
                   :delta-sharing.client/status 200
                   :delta-sharing.client/headers {:delta-table-version "1"
                                                  :server "databricks"},
                   :delta-sharing.client/request-headers {"startingTimestamp" "2022-01-01T00:00:00Z"}}
                  {:delta-table-version "1"}]
                 (#'dsc.http/form-success 200 headers
                                          nil {:info {:headers {"startingTimestamp" "2022-01-01T00:00:00Z"}}
                                               :method :get
                                               :path path
                                               :api ::dsc.http/query-table-version}
                                          [:delta-table-version] nil))))))

    (testing "default body response parsing"
      (is (= [{:method :get
               :api ::dsc.http/get-share
               :delta-sharing.client/status 200
               :delta-sharing.client/headers {:server "databricks"},
               :delta-sharing.client/request-headers nil}
              (json/read-str sample-get-share-body {:key-fn cu/->kebab-keyword})]
             (#'dsc.http/form-success 200 {:server "databricks"}
                                      sample-get-share-body
                                      {:info {}
                                       :method :get
                                       :api ::dsc.http/get-share}
                                      nil nil))))

    (testing "custom body response parsing"
      (is (= [{:method :get
               :api ::dsc.http/get-share
               :delta-sharing.client/status 200
               :delta-sharing.client/headers {:server "databricks"},
               :delta-sharing.client/request-headers nil}
              {:test-share {:test-id "abc123", :test-name "ram_bugbash_inbound"}}]
             (#'dsc.http/form-success 200 {:server "databricks"}
                                      sample-get-share-body
                                      {:info {}
                                       :method :get
                                       :api ::dsc.http/get-share}
                                      nil
                                      (fn [body]
                                        (json/read-str body {:key-fn (comp cu/->kebab-keyword
                                                                           (partial str "test-"))}))))))))


(deftest form-failure
  (let [path "shares/delta_sharing/schemas/default/tables/boston-housing/metadata"
        sample-header {:server "databricks"
                       :content-type "application/json"}]
    (testing "classic error types"
      (let [[res-info ex] (#'dsc.http/form-failure path 401 sample-header
                                                   (json/write-str {"error_code" "EXPIRED_CREDENTIALS"
                                                                    "message" "Expired credentials!"})
                                                   {:info {:headers {"delta-sharing-capabilities" "responseformat=parquet"}}})]
        (is (= res-info (ex-data ex))
            "The data on the exception should be the same as the res-info.")
        (is (= (format "Delta sharing API error on %s (401: unauthenticated - EXPIRED_CREDENTIALS): Expired credentials!"
                       path)
               (ex-message ex))
            "The message should match the expected formatting."))

      (testing "statuses"
        (are [result status] (= result (-> (#'dsc.http/form-failure path status sample-header
                                                                    (json/write-str {"error_code" "ERROR"
                                                                                     "message" "BOOM!"})
                                                                    {})
                                           (second)
                                           (ex-data)
                                           (:delta-sharing.client/error-type)))
          :bad-request 400
          :unauthenticated 401
          :forbidden 403
          :not-found 404
          :busy 429
          :internal-server-error 500
          :unknown 666)))
    (testing "known error sub-types"
      (is (= :unsupported-delta-table-features
             (-> (#'dsc.http/form-failure path 401 sample-header
                                          (json/write-str {"error_code" "INVALID_PARAMETER_VALUE"
                                                           "message" "DS_UNSUPPORTED_DELTA_TABLE_FEATURES: Table features delta.enableDeletionVectors are found in table version: 0."})
                                          {:info {:headers {"delta-sharing-capabilities" "responseformat=parquet"}}})
                 (second)
                 (ex-data)
                 (:delta-sharing.client/error-type)))))
    (testing "non-JSON error response"
      (is (= [401 nil nil]
             (->> (#'dsc.http/form-failure path 401 {:content-type "text/html"}
                                           (json/write-str {"error_code" "EXPIRED_CREDENTIALS"
                                                            "message" "Expired credentials!"})
                                           {:info {:headers {"delta-sharing-capabilities" "responseformat=parquet"}}})
                  (first)
                  ((juxt :delta-sharing.client/status
                         :delta-sharing.client/error-code
                         :delta-sharing.client/error-message))))
          "Error responses not in JSON should return without a parsed response"))))


(deftest call-api
  (testing "sanity checking errors"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot make API call on nil client."
          (#'dsc.http/call-api nil nil nil nil nil)))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot make API call without keyword label."
          (#'dsc.http/call-api {} nil nil nil nil)))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot make API call without keyword method."
          (#'dsc.http/call-api {} ::dsc.http/list-shares nil nil nil)))
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot make API call on blank path."
          (#'dsc.http/call-api {} ::dsc.http/list-shares :get nil nil))))
  (let [test-client {:flow (f/sync-handler)
                     :auth (atom (cu/veil "token"))
                     :endpoint "https://example.com/delta_sharing/"}]
    (testing "successful responses"
      (with-redefs [http/request (fn [request callback-fn]
                                   (callback-fn {:opts (select-keys request [::dsc.http/state])
                                                 :status 200
                                                 :headers {}
                                                 :body (json/write-str {"items" [{"id" "abc123",
                                                                                  "name" "delta_sharing"}]})}))]
        (is (= {:items [{:id "abc123", :name "delta_sharing"}]}
               (#'dsc.http/call-api test-client ::dsc.http/list-shares :get "shares" {:query-params {:max-results 5}}))
            "Should return a successful response")))
    (testing "error responses from server"
      (with-redefs [http/request (fn [request callback-fn]
                                   (callback-fn {:opts (select-keys request [::dsc.http/state])
                                                 :status 404
                                                 :headers {:content-type "application/json"}
                                                 :body (json/write-str {"error_code" "SHARE_DOES_NOT_EXIST"
                                                                        "message" "Share 'dne' does not exist."})}))]
        (is (thrown-with-msg? ExceptionInfo
                              #"Delta sharing API error on shares/dne .* Share 'dne' does not exist."
              (#'dsc.http/call-api test-client ::dsc.http/get-share :get "shares/dne" {}))
            "Should throw an error response")))
    (testing "client error"
      (with-redefs [http/request (fn [request callback-fn]
                                   (callback-fn {:opts (select-keys request [::dsc.http/state])
                                                 :error (ex-info "BOOM!" {})}))]
        (is (thrown-with-msg? ExceptionInfo
                              #"BOOM!"
              (#'dsc.http/call-api test-client ::dsc.http/list-shares :get "shares" {}))
            "Should throw the client error")))
    (testing "unexpected error in response parsing"
      (with-redefs [dsc.http/form-success (fn [_ _ _ _ _ _] (throw (RuntimeException. "BOOM!")))
                    http/request (fn [request callback-fn]
                                   (callback-fn {:opts (select-keys request [::dsc.http/state])
                                                 :status 200
                                                 :headers {}
                                                 :body (json/write-str {"items" [{"id" "abc123",
                                                                                  "name" "delta_sharing"}]})}))]
        (is (thrown-with-msg? Exception
                              #"Failed to parse success response: BOOM!"
              (#'dsc.http/call-api test-client ::dsc.http/list-shares :get "shares" {}))
            "Should throw the client error")))))


;; ## Client method response formatting

(deftest list-response-formatting
  (is (= {:data [{:id "abc123",
                  :name "delta_sharing"}]}
         (#'dsc.http/->list-response "{\"items\":[{\"name\":\"delta_sharing\",\"id\":\"abc123\"}]}"))
      "Should replace top level 'items' key with `:data`"))


(deftest common-table-json-response-foramtting
  (testing "parsing specific metadata keys and stringified JSON objects"
    (is (= {:metadata {:schema [{:metadata {}
                                 :name "field_foo"
                                 :nullable true
                                 :type "long"}]
                       :num-files 1}}
           (json/read-str (json/write-str {"metaData" {"schemaString" (json/write-str [{"metadata" {}
                                                                                        "name" "field_foo"
                                                                                        "nullable" true
                                                                                        "type" "long"}])
                                                       :num-files 1}})
                          {:key-fn #'dsc.http/parse-json-metadata-keys
                           :value-fn #'dsc.http/parse-wrapped-json-objects})))))


(deftest metadata-response-parsing
  (testing "parsing delta response format"
    (is (= t.fixture/metadata-response
           (#'dsc.http/->table-metadata-response
            {:delta-metadata (dissoc t.fixture/metadata-response :version :size :num-files)
             :version 0
             :size 123456
             :num-files 5}))
        "Should collapse the delta metadata key into the top level map")
    (is (= (merge t.fixture/metadata-response t.fixture/protocol-reader-v3)
           (#'dsc.http/parse-metadata-response (str/join "\n" [t.fixture/json-protocol-reader-v3
                                                               t.fixture/json-delta-metadata])))
        "Should parse protocol and metadata JSON responses into expected shapes"))

  (testing "parsing parquet response format"
    (is (= t.fixture/metadata-response
           (#'dsc.http/->table-metadata-response t.fixture/metadata-response))
        "Should collapse the delta metadata key into the top level map.")
    (is (= (merge (assoc t.fixture/metadata-response :configuration {})
                  t.fixture/protocol-reader-v1)
           (#'dsc.http/parse-metadata-response (str/join "\n" [t.fixture/json-protocol-reader-v1
                                                               t.fixture/json-parquet-metadata])))
        "Should parse protocol and metadata JSON responses into expected shapes")))


(deftest table-read-response-parsing
  (testing "catching parse errors from streamed responses"
    (is (thrown-with-msg? ExceptionInfo #"'Forced rpc error .*' is not a JSON object."
          (#'dsc.http/parse-table-read-response
           false
           (str/join "\n" [t.fixture/json-protocol-reader-v1
                           t.fixture/json-delta-metadata
                           "Forced rpc error that hides the real error message from the server"])))
        "Need to catch parse errors for startingVersion option support"))
  (testing "parsing delta response format"
    (is (= (assoc t.fixture/protocol-reader-v3
                  :metadata t.fixture/metadata-response
                  :files [t.fixture/parsed-delta-file])
           (#'dsc.http/parse-table-read-response
            false
            (str/join "\n" [t.fixture/json-protocol-reader-v3
                            t.fixture/json-delta-metadata
                            t.fixture/json-delta-file])))
        "Can parse delta file response with only one file")

    (is (= (assoc t.fixture/protocol-reader-v3
                  :metadata t.fixture/metadata-response
                  :files (into [] (repeat 2 t.fixture/parsed-delta-file)))
           (#'dsc.http/parse-table-read-response
            false
            (str/join "\n" [t.fixture/json-protocol-reader-v3
                            t.fixture/json-delta-metadata
                            t.fixture/json-delta-file
                            t.fixture/json-delta-file])))
        "Can parse delta file response with more than one file")

    (testing "response with change data and historical metadata"
      (is (= (assoc t.fixture/protocol-reader-v3
                    :metadata t.fixture/metadata-response
                    :change-data-actions [t.fixture/parsed-delta-add-action
                                          t.fixture/parsed-delta-metadata-change])
             (#'dsc.http/parse-table-read-response
              true
              (str/join "\n" [t.fixture/json-protocol-reader-v3
                              t.fixture/json-delta-metadata
                              t.fixture/json-delta-add-action
                              t.fixture/json-delta-metadata-change])))
          "Can parse delta file response with change data and historical metadata")))

  (testing "parsing parquet response format"
    (is (= (assoc t.fixture/protocol-reader-v1
                  :metadata (assoc t.fixture/metadata-response :configuration {})
                  :files [t.fixture/parsed-parquet-file])
           (#'dsc.http/parse-table-read-response
            false
            (str/join "\n" [t.fixture/json-protocol-reader-v1
                            t.fixture/json-parquet-metadata
                            t.fixture/json-parquet-file])))
        "Can parse parquet file response with only one file")

    (is (= (assoc t.fixture/protocol-reader-v1
                  :metadata (assoc t.fixture/metadata-response :configuration {})
                  :files (into [] (repeat 2 t.fixture/parsed-parquet-file)))
           (#'dsc.http/parse-table-read-response
            false
            (str/join "\n" [t.fixture/json-protocol-reader-v1
                            t.fixture/json-parquet-metadata
                            t.fixture/json-parquet-file
                            t.fixture/json-parquet-file])))
        "Can parse parquet file response with more than one file")

    (testing "response with change data files and historical metadata"
      (is (= (assoc t.fixture/protocol-reader-v1
                    :metadata (assoc t.fixture/metadata-response :configuration {})
                    :change-data-actions [t.fixture/parsed-parquet-add-action])
             (#'dsc.http/parse-table-read-response
              true
              (str/join "\n" [t.fixture/json-protocol-reader-v1
                              t.fixture/json-parquet-metadata
                              t.fixture/json-parquet-add-action])))
          "Can parse parquet file response with change data response")

      (is (= (assoc t.fixture/protocol-reader-v1
                    :metadata (assoc t.fixture/metadata-response :configuration {})
                    :change-data-actions [t.fixture/parsed-parquet-add-action
                                          t.fixture/parsed-parquet-metadata-change])
             (#'dsc.http/parse-table-read-response
              true
              (str/join "\n" [t.fixture/json-protocol-reader-v1
                              t.fixture/json-parquet-metadata
                              t.fixture/json-parquet-add-action
                              t.fixture/json-parquet-metadata-change])))
          "Can parse parquet file response with change data response with historical metadata"))))


;; ## Client method setup

(deftest http-client
  (let [client (dsc.http/http-client "https://example.com/delta-sharing" "token" {:flow (f/sync-handler)})]
    (with-redefs [dsc.http/call-api (fn [_ api-label method path params]
                                      {:api-label api-label
                                       :method method
                                       :path path
                                       :params (dissoc params :handle-body-response)})]
      (testing "list-shares method args"
        (is (= {:api-label ::dsc.http/list-shares
                :method :get
                :path "shares"
                :params {}}
               (dsc.proto/list-shares client {}))
            "Optionless version returns without query-params")
        (is (= {:api-label ::dsc.http/list-shares
                :method :get
                :path "shares"
                :params {:query-params {"maxResults" 5, "nextToken" "abc-123"}}}
               (dsc.proto/list-shares client {:max-results 5, :next-token "abc-123", :foo 1}))
            "Returns with respected query-params"))

      (testing "get-share method args"
        (is (= {:api-label ::dsc.http/get-share
                :method :get
                :path "shares/delta_sharing"
                :params {}}
               (dsc.proto/get-share client "delta_sharing"))
            "Returns without query-params"))

      (testing "list-share-schemas method args"
        (is (= {:api-label ::dsc.http/list-share-schemas
                :method :get
                :path "shares/delta_sharing/schemas"
                :params {}}
               (dsc.proto/list-share-schemas client "delta_sharing" {}))
            "Optionless version returns without query-params")
        (is (= {:api-label ::dsc.http/list-share-schemas
                :method :get
                :path "shares/delta_sharing/schemas"
                :params {:query-params {"maxResults" 5, "nextToken" "abc-123"}}}
               (dsc.proto/list-share-schemas client "delta_sharing" {:max-results 5, :next-token "abc-123", :foo 1}))
            "Returns with respected query-params"))

      (testing "list-schema-tables method args"
        (is (= {:api-label ::dsc.http/list-schema-tables
                :method :get
                :path "shares/delta_sharing/schemas/default/tables"
                :params {}}
               (dsc.proto/list-schema-tables client "delta_sharing" "default" {}))
            "Optionless version returns without query-params")
        (is (= {:api-label ::dsc.http/list-schema-tables
                :method :get
                :path "shares/delta_sharing/schemas/default/tables"
                :params {:query-params {"maxResults" 5, "nextToken" "abc-123"}}}
               (dsc.proto/list-schema-tables client "delta_sharing" "default" {:max-results 5, :next-token "abc-123", :foo 1}))
            "Returns with respected query-params"))

      (testing "list-share-tables method args"
        (is (= {:api-label ::dsc.http/list-share-tables
                :method :get
                :path "shares/delta_sharing/all-tables"
                :params {}}
               (dsc.proto/list-share-tables client "delta_sharing" {}))
            "Optionless version returns without query-params")
        (is (= {:api-label ::dsc.http/list-share-tables
                :method :get
                :path "shares/delta_sharing/all-tables"
                :params {:query-params {"maxResults" 5, "nextToken" "abc-123"}}}
               (dsc.proto/list-share-tables client "delta_sharing" {:max-results 5, :next-token "abc-123", :foo 1}))
            "Returns with respected query-params"))

      (testing "query-table-version method args"
        (is (= {:api-label ::dsc.http/query-table-version
                :method :get
                :path "shares/delta_sharing/schemas/default/tables/table_foo/version"
                :params {:include-header-keys [:delta-table-version]}}
               (dsc.proto/query-table-version client "delta_sharing" "default" "table_foo" {}))
            "Optionless version returns without query-params")
        (is (= {:api-label ::dsc.http/query-table-version
                :method :get
                :path "shares/delta_sharing/schemas/default/tables/table_foo/version"
                :params {:include-header-keys [:delta-table-version]
                         :query-params {"startingTimestamp" 0}}}
               (dsc.proto/query-table-version client "delta_sharing" "default" "table_foo" {:starting-timestamp 0}))
            "Returns with respected query-params"))

      (testing "query-table-metadata method args"
        (is (= {:api-label ::dsc.http/query-table-metadata
                :method :get
                :path "shares/delta_sharing/schemas/default/tables/table_foo/metadata"
                :params {:headers {"delta-sharing-capabilities" "responseformat=parquet"}
                         :include-header-keys [:delta-table-version]}}
               (dsc.proto/query-table-metadata client "delta_sharing" "default" "table_foo" {}))
            "Optionless version returns with reader version 1 equivalent delta sharing capabilities")
        (is (= {:api-label ::dsc.http/query-table-metadata
                :method :get
                :path "shares/delta_sharing/schemas/default/tables/table_foo/metadata"
                :params {:headers {"delta-sharing-capabilities"
                                   "responseformat=delta;readerfeatures=columnMapping,deletionVectors,timestampNtz,v2Checkpoint,vacuumProtocolCheck"}
                         :include-header-keys [:delta-table-version]}}
               (dsc.proto/query-table-metadata client "delta_sharing" "default" "table_foo" {:min-reader-version 3, :ignored-key :foo}))
            "Respects min-reader-version key")
        (is (= {:api-label ::dsc.http/query-table-metadata
                :method :get
                :path "shares/delta_sharing/schemas/default/tables/table_foo/metadata"
                :params {:headers {"delta-sharing-capabilities" "responseformat=delta;readerfeatures=deletionVectors"}
                         :include-header-keys [:delta-table-version]}}
               (dsc.proto/query-table-metadata client "delta_sharing" "default" "table_foo" {:response-format "delta"
                                                                                             :reader-features "deletionVectors"}))
            "Passing respose-format and/or reader-features overrides the default reader version capabilities"))

      (testing "read-table-data method args"
        (is (= {:api-label ::dsc.http/read-table-data
                :method :post
                :path "shares/delta_sharing/schemas/default/tables/table_foo/query"
                :params {:headers {"delta-sharing-capabilities" "responseformat=parquet"
                                   "Content-Type" "application/json; charset=utf-8"}
                         :include-header-keys [:delta-table-version]
                         :body {}}}
               (dsc.proto/read-table-data client "delta_sharing" "default" "table_foo" {}))
            "Optionless version returns without query-params")
        ;; all these options passed simultaneously aren't actually valid to a delta sharing server
        ;; but here for testing purposes only
        (let [json-predicate-hints (json/write-str
                                     {"op" "equal",
                                      "children" [{"op" "column"
                                                   "name" "date"
                                                   "valueType" "date"}
                                                  {"op" "literal"
                                                   "value" "2021-04-29"
                                                   "valueType" "date"}]})]
          (is (= {:api-label ::dsc.http/read-table-data
                  :method :post
                  :path "shares/delta_sharing/schemas/default/tables/table_foo/query"
                  :params {:headers {"delta-sharing-capabilities" "responseformat=parquet"
                                     "Content-Type" "application/json; charset=utf-8"}
                           :include-header-keys [:delta-table-version]
                           :body {"predicateHints" ["date >= '2021-01-01'"]
                                  "jsonPredicateHints" json-predicate-hints
                                  "limitHint" 100
                                  "version" 123
                                  "timestamp" "2023-03-22T12:45:50.00Z"
                                  "startingVersion" 100
                                  "endingVersion" 123}}}
                 (dsc.proto/read-table-data client "delta_sharing" "default" "table_foo" {:predicate-hints ["date >= '2021-01-01'"]
                                                                                          :json-predicate-hints json-predicate-hints
                                                                                          :limit-hint 100
                                                                                          :version 123
                                                                                          :timestamp "2023-03-22T12:45:50.00Z"
                                                                                          :starting-version 100
                                                                                          :ending-version 123
                                                                                          :ignored-key "should be ignored"}))
              "Respects options supported by the delta sharing protocol method")))

      (testing "read-change-data-feed method args"
        (is (= {:api-label ::dsc.http/read-change-data-feed
                :method :get
                :path "shares/delta_sharing/schemas/default/tables/table_foo/changes"
                :params {:headers {"delta-sharing-capabilities" "responseformat=parquet"}
                         :include-header-keys [:delta-table-version]}}
               (dsc.proto/read-change-data-feed client "delta_sharing" "default" "table_foo" {}))
            "Optionless version returns without query-params")
        (is (= {:api-label ::dsc.http/read-change-data-feed
                :method :get
                :path "shares/delta_sharing/schemas/default/tables/table_foo/changes"
                :params {:headers {"delta-sharing-capabilities" "responseformat=parquet"}
                         :include-header-keys [:delta-table-version]
                         :query-params {"startingTimestamp" "2023-03-22T12:45:50.00Z"
                                        "endingTimestamp" "2023-03-25T15:00:00.00Z"
                                        "startingVersion" 100
                                        "endingVersion" 123
                                        "includeHistoricalMetadata" true}}}
               (dsc.proto/read-change-data-feed client "delta_sharing" "default" "table_foo" {:starting-timestamp "2023-03-22T12:45:50.00Z"
                                                                                              :ending-timestamp "2023-03-25T15:00:00.00Z"
                                                                                              :starting-version 100
                                                                                              :ending-version 123
                                                                                              :include-historical-metadata? true
                                                                                              :ignored-key "should be ignored"}))
            "Respects options supported by the delta sharing protocola method")))))


;; ## Client creation

(deftest client-creation
  (testing "endpoint validation"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Endpoint must be a URL with scheme 'http' or 'https':.*"
          (dsc.http/http-client "mock://" "" {}))))

  (testing "auth token validation"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Cannot create client with no auth token."
          (dsc.http/http-client "https://example.com/delta-sharing/" "" {}))))

  (testing "client construction"
    (is (= "token"
           (->>
             (dsc.http/http-client "https://example.com/delta-sharing/" "token" {})
             (:auth)
             (deref)
             (cu/unveil)))
        "auth token should be veiled")

    (is (= "https://example.com/delta-sharing/"
           (:endpoint
             (dsc.http/http-client "https://example.com/delta-sharing" "token" {})))
        "ensure a trailing slash to the endpoint")

    (testing "flow and http opts"
      (is (= {:timeout-ms 120}
             (:http-opts
               (dsc.http/http-client "https://example.com/delta-sharing"
                                     "token"
                                     {:timeout-ms 120})))
          "sets the passed http opts")
      (let [not-a-flow-handler (fn test-flow [x] x)]
        (is (= not-a-flow-handler
               (:flow
                 (dsc.http/http-client "https://example.com/delta-sharing"
                                       "token"
                                       {:flow not-a-flow-handler})))
            "uses the custom flow handler passed into the client")))))
