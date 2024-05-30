(ns delta-sharing.client
  "Main Delta Sharing client namespace. Contains functions for generic client
  operations, constructing new clients, and using clients as components in a
  larger system."
  (:require
    [delta-sharing.client.http :as http]
    [delta-sharing.client.proto :as proto])
  (:import
    java.net.URI))


(def ^:private client-min-reader-version
  1)


;; ## Protocol methods


(defn list-shares
  "Returns a list of all shares accessible to the recipient.

   Options:
    - `max-results` (int) maximum results per page. If there are more available results,
    the response will return a `:next-page-token` that can be used to get the next page.

    - `next-token` (string) a page token to use for the next page of results. See `max-results`."
  ([client]
   (list-shares client {}))
  ([client opts]
   (proto/list-shares client opts)))


(defn get-share
  "Returns information about a share that's accessible to the recipient."
  [client share]
  (proto/get-share client share))


(defn list-share-schemas
  "Returns a list of all the shemas in a share.
  
   Options:
    - `max-results` (int) maximum results per page. If there are more available results,
    the response will return a `:next-page-token` that can be used to get the next page.

    - `next-token` (string) a page token to use for the next page of results. See `max-results`."
  ([client share]
   (list-share-schemas client share {}))
  ([client share opts]
   (proto/list-share-schemas client share opts)))


(defn list-schema-tables
  "Returns a list of tables in a schema.

   Options:
    - `max-results` (int) maximum results per page. If there are more available results,
    the response will return a `:next-page-token` that can be used to get the next page.

    - `next-token` (string) a page token to use for the next page of results. See `max-results`."
  ([client share schema]
   (list-schema-tables client share schema {}))
  ([client share schema opts]
   (proto/list-schema-tables client share schema opts)))


(defn list-share-tables
  "Returns a list of tables within all schemas of a share.
  
   Options:
    - `max-results` (int) maximum results per page. If there are more available results,
    the response will return a `:next-page-token` that can be used to get the next page.

    - `next-token` (string) a page token to use for the next page of results. See `max-results`."
  ([client share]
   (list-share-tables client share {}))
  ([client share opts]
   (proto/list-share-tables client share opts)))


(defn query-table-version
  "Returns information about the version of a table.

  Optionally accepts a `:starting-timestamp` from which to return the earliest matching 
  version for."
  ([client share schema table]
   (query-table-version client share schema table {}))
  ([client share schema table opts]
   (proto/query-table-version client share schema table opts)))


(defn query-table-metadata
  "Returns a map of information about a table's metadata and protocol.

  See https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#query-table-metadata for more.
  
  Options:
   - `:min-reader-version` (int - default 1) Minimum reader version to support. See 
   https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features for more.
   Supported reader versions include:
   1 {:response-format 'parquet'}
   2 {:response-format 'delta' :reader-features 'columnMapping'}
   3 {:response-format 'delta' :reader-features 'columnMapping,deletionVectors,timestampNtz,v2Checkpoint,vacuumProtocolCheck'}
   - `:response-format` (string - default 'parquet') The default table format to respond with. Overrides the one from
   `min-reader-version` when set.
   - `:reader-features` (string - default none) Advanced table features on the requested table. Requires a
   response format in delta. Overrides the one from `min-reader-version` when set."
  ([client share schema table]
   (query-table-metadata client share schema table {}))
  ([client share schema table opts]
   (proto/query-table-metadata client share schema table opts)))


(defn read-table-data
  "Returns a map with information about the latest version of the table including its `:protocol`, `:metadata`.
  Provides the downloadable data files under `:files` for the latest version (or the requested version when `:version` is set).
  Otherwise, a collection of change data files is returned under `:change-data-actions` along with any historical metadata.

  See https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#read-data-from-a-table for more. And
  https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#request-body for more about the request parameters.
  
  Delta sharing capability options:
   - `:min-reader-version` (int - default 1) Minimum reader version to support. See 
   https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features for more.
   Supported reader versions include:
   1 {:response-format 'parquet'}
   2 {:response-format 'delta' :reader-features 'columnMapping'}
   3 {:response-format 'delta' :reader-features 'columnMapping,deletionVectors,timestampNtz,v2Checkpoint,vacuumProtocolCheck'}
   - `:response-format` (string - default 'parquet') The default table format to respond with. Overrides the one from
   `min-reader-version` when set.
   - `:reader-features` (string - default none) Advanced table features on the requested table. Requires a
   response format in delta. Overrides the one from `min-reader-version` when set.

   Supported delta sharing options:
   [`:predicate-hints`,`:json-predicate-hints`,`:limit-hint`,`:version`,`:timestamp`,`:starting-version`,`:ending-version`]"
  ([client share schema table]
   (read-table-data client share schema table))
  ([client share schema table opts]
   (proto/read-table-data client share schema table opts)))


(defn read-change-data-feed
  "Returns a map with information about the table (protocol and metadata) along with its change data files since `starting-version`
  or `starting-timestamp` inclusively. The top-level `:metadata` returned is the first version in the requested range.

  One of either the version or timestamp parameters must be passed.

  The change data feed represents row-level changes between versions of a Delta table. It records change data
  for UPDATE, DELETE, and MERGE operations. See https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#read-change-data-feed-from-a-table 
  for more. Only valid for tables with `delta.enableChangeDataFeed` enabled.

  NOTE: Synthetic table columns `_change_type`, `_commit_version`, and `_commit_timestamp` are not added to the response. They can be derived
  by passing the response to a reader that reads the change file and table actions from the version respectively. 
  
  Delta sharing capability options:
   - `:min-reader-version` (int - default 1) Minimum reader version to support. See 
   https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features for more.
   Supported reader versions include:
   1 {:response-format 'parquet'}
   2 {:response-format 'delta' :reader-features 'columnMapping'}
   3 {:response-format 'delta' :reader-features 'columnMapping,deletionVectors,timestampNtz,v2Checkpoint,vacuumProtocolCheck'}
   - `:response-format` (string - default 'parquet') The default table format to respond with. Overrides the one from
   `min-reader-version` when set.
   - `:reader-features` (string - default none) Advanced table features on the requested table. Requires a
   response format in delta. Overrides the one from `min-reader-version` when set.

   Delta sharing parameters: 
   [`:starting-timestamp`,`:ending-timestamp`,`:starting-version`,`:ending-version`,`:include-historical-metadata?`]"
  ([client share schema table]
   (read-change-data-feed client share schema table))
  ([client share schema table opts]
   (proto/read-change-data-feed client share schema table opts)))


;; ## Client Construction

(defn new-client
  "Contructs a new delta-sharing client from a URI address endpoint by dispatching on the scheme."
  [endpoint auth reader-version & {:as http-opts}]
  (let [uri (URI/create endpoint)]
    (when-not (>= reader-version client-min-reader-version)
      (throw (IllegalArgumentException. (format "Client does not support reader version %d. Reader version should be >= %d"
                                                reader-version
                                                client-min-reader-version))))
    (if (contains? #{"http" "https"} (.getScheme uri))
      (http/http-client endpoint auth http-opts)
      (throw (IllegalArgumentException.
               (str "Unsupported delta-sharing address scheme: " (pr-str endpoint)))))))
