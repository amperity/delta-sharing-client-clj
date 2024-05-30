(ns delta-sharing.client.http
  (:require
    [clojure.data.json :as json]
    [clojure.set :as set]
    [clojure.string :as str]
    [delta-sharing.client.flow :as f]
    [delta-sharing.client.proto :as proto]
    [delta-sharing.util :as cu]
    [org.httpkit.client :as http]))


;; ## Defs and request/response utils

;; from https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features
;; all metadata, samples and CDF requests need to respect the reader features of the table being requested
;; e.g. if a table has a minReaderVersion of 2, columnMapping must be passed as a reader feature
(def ^:private delta-reader-version-1
  {:response-format "parquet"})


(def ^:private delta-reader-version-2
  {:response-format "parquet,delta"
   :reader-features "columnMapping"})


;; note: versions are supersets of each other so a reader version of 3 is fine to read a table of
;; minReaderVersion 2 but not vice versa
(def ^:private delta-reader-version-3
  {:response-format "delta"
   :reader-features "columnMapping,deletionVectors,timestampNtz,v2Checkpoint,vacuumProtocolCheck"})


(defn- select-reader-opts
  "Select, from the passed options, the minimum reader version the client will support for querying 
  or reading the table. Will override the default `:response-format` and `:reader-features` if set.
  This will allow the caller to read table in the specified response format with the corresponding 
  delta table features."
  [opts]
  (let [capability-opts (merge (case (:min-reader-version opts)
                                 1 delta-reader-version-1
                                 2 delta-reader-version-2
                                 3 delta-reader-version-3
                                 delta-reader-version-1)
                               (select-keys opts [:response-format :reader-features]))]
    {"delta-sharing-capabilities"
     (->> capability-opts
          (mapv (fn form-opts
                  [[opt-key opt-val]]
                  (str (-> opt-key (name) (str/replace "-" ""))
                       "=" opt-val)))
          (str/join ";"))}))


(defn- known-error-type
  "Returns an enriched error type key if known."
  [err-message]
  (cond
    (str/starts-with? err-message "DS_UNSUPPORTED_DELTA_TABLE_FEATURES")
    :unsupported-delta-table-features))


;; ## API functions

(defn- prepare-request
  "Produces a map of options to pass to the HTTP client for the provided
  method, API path, and other request parameters."
  [client method path params]
  (let [token (cu/unveil @(:auth client))
        content-type (get-in params [:headers "Content-Type"])]
    (cond-> (assoc params
                   :method method
                   :url (str (:endpoint client) path))
      token
      (assoc-in [:headers "authorization"] (str "Bearer " token))

      (and content-type
           (str/starts-with? content-type "application/json")
           (:body params))
      (update :body json/write-str))))


(defn- form-success
  "Handle a successful response from the delta sharing API. Returns a pair of the collected
  response information and the parsed result data."
  [status header body info include-header-keys handle-body-response]
  ;; in case the response defaults to a ByteInputStream in the absence of a body or content-type in response header
  (let [parsed (when (and (string? body)
                          (not (str/blank? body)))
                 (if handle-body-response
                   (handle-body-response body)
                   (json/read-str body {:key-fn cu/->kebab-keyword})))
        move-keys (select-keys header include-header-keys)
        res-info (-> info
                     (assoc :delta-sharing.client/status status
                            :delta-sharing.client/headers header
                            :delta-sharing.client/request-headers (get-in info [:info :headers]))
                     (dissoc :info))
        data (-> (or parsed {})
                 (merge move-keys)
                 (vary-meta merge res-info))]
    [res-info data]))


(defn- form-failure
  "Handle an error response from the delta sharing API. Returns a pair of the collected 
  response information and the exception to be yeilded."
  [path status header body info]
  (let [parsed (when (and (not (str/blank? body))
                          (:content-type header)
                          (str/starts-with? (:content-type header)
                                            "application/json"))
                 (json/read-str body))
        ;; note: supporting both here bc databricks actually returns the `error_code`
        ;; but documents returning `errorCode` in the protocol.
        err-code (or (get parsed "error_code")
                     (get parsed "errorCode"))
        err-msg (get parsed "message")
        err-type (case (int status)
                   400 :bad-request
                   401 :unauthenticated
                   403 :forbidden
                   404 :not-found
                   429 :busy
                   500 :internal-server-error
                   :unknown)
        err-subtype (when err-msg (known-error-type err-msg))
        res-info (-> info
                     (assoc :delta-sharing.client/status status
                            :delta-sharing.client/header header
                            :delta-sharing.client/error-type (or err-subtype err-type)
                            :delta-sharing.client/request-header (get-in info [:info :headers]))
                     (dissoc :info)
                     (cond->
                       err-code
                       (assoc :delta-sharing.client/error-code err-code)

                       err-msg
                       (assoc :delta-sharing.client/error-message err-msg)))
        message (format "Delta sharing API error on %s (%s: %s): %s"
                        path
                        status
                        (if err-code
                          (str (name err-type) " - " err-code)
                          (name err-type))
                        err-msg)]
    [res-info (ex-info message res-info)]))


(defn- call-api
  "Make an HTTP call to the databricks delta sharing API."
  [client api-label method path params]
  (when-not client
    (throw (IllegalArgumentException. "Cannot make API call on nil client.")))
  (when-not (keyword? api-label)
    (throw (IllegalArgumentException. "Cannot make API call without keyword label.")))
  (when-not (keyword? method)
    (throw (IllegalArgumentException. "Cannot make API call without keyword method.")))
  (when (str/blank? path)
    (throw (IllegalArgumentException. "Cannot make API call on blank path.")))
  (let [handler (:flow client)
        request (prepare-request client method path params)
        info (merge {:info params}
                    {:delta-sharing.client/api api-label
                     :delta-sharing.client/method method
                     :delta-sharing.client/path path}
                    (when-let [query (:query-params params)]
                      {:delta-sharing.client/query query}))]
    (letfn [(make-request
              [extra]
              (http/request
                (merge request extra)
                callback))

            (callback
              [{:keys [opts status headers body error]}]
              (let [{::keys [state]} opts]
                (try
                  (if error
                    (f/on-error! handler state info error)
                    (if (<= 200 status 299)
                      (let [{:keys [handle-body-response
                                    include-header-keys]} params
                            [res-info data] (try
                                              (form-success status headers body info
                                                            include-header-keys
                                                            handle-body-response)
                                              (catch Exception ex
                                                [(ex-data ex) (ex-info (str "Failed to parse success response: "
                                                                            (ex-message ex))
                                                                       (merge {} (ex-data ex)))]))]
                        (if (instance? Throwable data)
                          (f/on-error! handler state res-info data)
                          (f/on-success! handler state res-info data)))
                      (let [[res-info ex] (form-failure path status headers body info)]
                        (f/on-error! handler state res-info ex))))
                  (catch Exception ex
                    ;; Unexpected error
                    (f/on-error! handler state info ex)))))]
      ;; Kick off the HTTP request
      (f/call!
        handler info
        (fn call
          [state]
          (make-request {::state state}))))))


;; ## Client Utils



(defn- ->list-response
  "Renames the response of a list endpoint that has been pre-parsed
  into kebab-cased keywords."
  [body]
  (-> body
      (json/read-str  {:key-fn cu/->kebab-keyword})
      (set/rename-keys  {:items :data})))


(defn- parse-json-metadata-keys
  "Helper to handle formating of the keys of the returned metadata and table data responses.
  Example: metaData -> :metadata, schemaString -> :schema (bc it'll be parsed)."
  [k]
  (case k
    "schemaString"
    :schema

    "metaData"
    :metadata

    (cu/->kebab-keyword k)))


(defn- parse-wrapped-json-objects
  "Generic helper for parsing JSON wrapper objects returned by the metadata and table data
  endpoints."
  [k v]
  (cond
    (and (= :schema k) (string? v))
    (json/read-str v {:key-fn cu/->kebab-keyword})

    (and (= :stats k) (string? v))
    (-> v (json/read-str) (update-keys cu/->kebab-keyword))

    :else
    v))


(defn- ->table-metadata-response
  "Returns a flat map of table metadata."
  [{:keys [num-files size version] :as table-metadata}]
  (if-let [metadata (:delta-metadata table-metadata)]
    (cu/assoc-some metadata
                   :num-files num-files
                   :size size
                   :version version)
    table-metadata))


(defn- parse-metadata-response
  [body]
  (let [[{protocol :protocol}
         {metadata :metadata}] (map #(json/read-str % {:key-fn parse-json-metadata-keys
                                                       :value-fn parse-wrapped-json-objects})
                                    (str/split body #"\n"))]
    ;; simplify return shapes to remove wrapped datatypes (especially for the delta format responses)
    (assoc (->table-metadata-response metadata)
           :protocol (or (:delta-protocol protocol) protocol))))


(defn- flatten-action
  "Returns a version of the table action object where the action type is embedded within the action info."
  [action-obj]
  (let [[action-kw action-info] (first action-obj)
        [delta-action table-action] (some->> action-info
                                             (:delta-single-action)
                                             (first))]
    (cond
      (= :metadata action-kw)
      (assoc (->table-metadata-response action-info) :action action-kw)

      (not= :file action-kw)
      (assoc action-info :action action-kw)

      delta-action
      (assoc action-info
             :delta-single-action
             (assoc table-action :action delta-action))

      :else
      action-info)))


(defn- parse-table-read-response
  "Returns information about a version of the table or about the change data files if 
  returning a cdf response."
  [cdf-response? body]
  (let [[{protocol :protocol}
         {metadata :metadata}
         & responses] (map (fn parse-json-object
                             [obj]
                             (try
                               (json/read-str obj {:key-fn parse-json-metadata-keys
                                                   :value-fn parse-wrapped-json-objects})
                               (catch Exception _
                                 (throw (ex-info (format "'%s' is not a JSON object." obj)
                                                 {:failed-string obj})))))
                           (str/split body #"\n"))
        files (into [] (map flatten-action) (if (map? responses)
                                              [responses]
                                              responses))]
    ;; return a CDF response when `startingVersion` or `startingTimestamp` are set
    (assoc {:metadata (->table-metadata-response metadata)
            :protocol (or (:delta-protocol protocol) protocol)}
           (if cdf-response? :change-data-actions :files)
           files)))


;; ## HTTP Client

(defrecord HTTPClient
  [flow endpoint auth http-opts]

  proto/Client

  (list-shares
    [client opts]
    (call-api
      client ::list-shares
      :get "shares"
      (cu/assoc-some {:handle-body-response ->list-response}
                     :query-params (-> opts
                                       (select-keys [:max-results
                                                     :next-token])
                                       (cu/camelify-keys)
                                       (not-empty)))))


  (get-share
    [client share]
    (call-api
      client ::get-share
      :get (str "shares/" share)
      {}))


  (list-share-schemas
    [client share opts]
    (call-api
      client ::list-share-schemas
      :get (str "shares/" share "/schemas")
      (cu/assoc-some
        {:handle-body-response ->list-response}
        :query-params (-> opts
                          (select-keys [:max-results
                                        :next-token])
                          (cu/camelify-keys)
                          (not-empty)))))


  (list-schema-tables
    [client share schema opts]
    (call-api
      client ::list-schema-tables
      :get (str "shares/" share
                "/schemas/" schema "/tables")
      (cu/assoc-some
        {:handle-body-response ->list-response}
        :query-params (-> opts
                          (select-keys [:max-results
                                        :next-token])
                          (cu/camelify-keys)
                          (not-empty)))))


  (list-share-tables
    [client share opts]
    (call-api
      client ::list-share-tables
      :get (str "shares/" share "/all-tables")
      (cu/assoc-some
        {:handle-body-response ->list-response}
        :query-params (-> opts
                          (select-keys [:max-results
                                        :next-token])
                          (cu/camelify-keys)
                          (not-empty)))))


  (query-table-version
    [client share schema table opts]
    (call-api
      client ::query-table-version
      :get (str "shares/" share
                "/schemas/" schema
                "/tables/" table "/version")
      (cu/assoc-some
        {:include-header-keys [:delta-table-version]}
        :query-params (-> opts
                          (select-keys [:starting-timestamp])
                          (cu/camelify-keys)
                          (not-empty)))))


  (query-table-metadata
    [client share schema table opts]
    (call-api
      client ::query-table-metadata
      :get (str "shares/" share
                "/schemas/" schema
                "/tables/" table "/metadata")
      {:headers (select-reader-opts opts)
       :include-header-keys [:delta-table-version]
       :handle-body-response parse-metadata-response}))


  (read-table-data
    [client share schema table opts]
    (call-api
      client ::read-table-data
      :post (str "shares/" share
                 "/schemas/" schema
                 "/tables/" table "/query")
      {:headers (assoc (select-reader-opts opts)
                       "Content-Type" "application/json; charset=utf-8")

       :include-header-keys [:delta-table-version]
       :handle-body-response (partial parse-table-read-response
                                      (boolean (:starting-version opts)))
       :body (-> (or opts {})
                 (select-keys [:predicate-hints
                               :json-predicate-hints
                               :limit-hint
                               :version
                               :timestamp
                               :starting-version
                               :ending-version])
                 (cu/camelify-keys))}))


  (read-change-data-feed
    [client share schema table opts]
    (call-api
      client ::read-change-data-feed
      :get (str "shares/" share
                "/schemas/" schema
                "/tables/" table "/changes")
      (cu/assoc-some
        {:headers (select-reader-opts opts)
         :include-header-keys [:delta-table-version]
         :handle-body-response (partial parse-table-read-response
                                        (boolean (or (:starting-version opts)
                                                     (:starting-timestamp opts))))}
        :query-params (-> (or opts {})
                          (select-keys [:starting-timestamp
                                        :ending-timestamp
                                        :starting-version
                                        :ending-version
                                        :include-historical-metadata?])
                          (set/rename-keys {:include-historical-metadata? :include-historical-metadata})
                          (cu/camelify-keys)
                          (not-empty))))))


;; ## Constructors

;; Privatize automatic constructors.
(alter-meta! #'->HTTPClient assoc :private true)
(alter-meta! #'map->HTTPClient assoc :private true)


(defn- ensure-trailing-slash
  [endpoint]
  (if-not (str/ends-with? endpoint "/")
    (str endpoint "/")
    endpoint))


(defn http-client
  "Create a new HTTP delta-sharing client. The endpoint should start with
  `http://` or `https://`."
  [endpoint auth http-opts]
  (when-not (and (string? endpoint)
                 (or (str/starts-with? endpoint "http://")
                     (str/starts-with? endpoint "https://")))
    (throw (IllegalArgumentException.
             (str "Endpoint must be a URL with scheme 'http' or 'https': "
                  (pr-str endpoint)))))
  (when (str/blank? auth)
    (throw (IllegalArgumentException.
             "Cannot create client with no auth token.")))
  (map->HTTPClient
    (merge {:flow (f/sync-handler)}
           http-opts
           {:endpoint (ensure-trailing-slash endpoint)
            :auth (atom (cu/veil auth))
            :http-opts (merge {:timeout-ms 120000}
                              http-opts)})))
