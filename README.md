Amperity Delta Sharing Library
==============================

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/amperity/delta-sharing-client-clj/tree/main.svg?style=shield)](https://dl.circleci.com/status-badge/redirect/gh/amperity/delta-sharing-client-clj/tree/main)
[![codecov](https://codecov.io/gh/amperity/delta-sharing-client-clj/branch/main/graph/badge.svg)](https://codecov.io/gh/amperity/delta-sharing-client-clj)
[![Clojars Project](https://img.shields.io/clojars/v/com.amperity/delta-sharing-client-clj.svg)](https://clojars.org/com.amperity/delta-sharing-client-clj)
[![cljdoc](https://cljdoc.org/badge/com.amperity/delta-sharing-client-clj)](https://cljdoc.org/d/com.amperity/delta-sharing-client-clj/CURRENT)

A native clojure implementation of a delta sharing client. More details about the delta sharing protocol can be found at the [official docs page](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md).

## Client construction

The following is an example of how to construct an instance of the client against Databricks' open delta sharing server.

The client is constructed an endpoint URL, a bearer token & share credentials version. More details about these profile credentials can be found under the ["Profile File Format" section of the official docs page](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md#profile-file-format).

```clojure
;; Pull in namespace
user=> (require '[amperity.delta-sharing.client :as dsc])
nil

;; Create an instance of the client using the endpoint, token and version
user=> (def client (dsc/new-client "https://sharing.delta.io/delta-sharing/" "my auth token" 1 {}))
#'user/client

;; Invoke the method(s) you want using the client
user=> (dsc/list-shares client {:max-results 50})
=> {:data [{:name "delta_sharing"}]}

user=> (dsc/list-share-tables client "delta_sharing" {:max-results 2})
=> {:next-page-token "CgE1Eg1kZWx0YV9zaGFyaW5n",
    :data [{:name "COVID_19_NYT"
            :schema "default",
            :share "delta_sharing"}
           {:name "boston-housing",
            :schema "default",
            :share "delta_sharing"}]]}

user=> (dsc/read-table-data client "delta_sharing" "default" "COVID_19_NYT" {:min-reader-version 1})
{:delta-table-version "1710371505697",
 :files [{:expiration-timestamp 1713812921776,
          :id "5ds7z3rjK3qFxy54zRre1GSXiy9fap",
          :partition-values {},
          :size 550,
          :url "https://open-delta-sharing.s3.us-west-2.amazonaws.com/samples/COVID-19_NYT/some-signature"}],
 :metadata {:format {:provider "parquet"},
            :id "7245fd1d-8a6d-4988-af72-92a95b646511",
            :schema {
                :fields [{:metadata {}, :name "date", :nullable true, :type "string"}
                         {:metadata {}, :name "county", :nullable true, :type "string"}
                         {:metadata {}, :name "state", :nullable true, :type "string"}
                         {:metadata {}, :name "fips", :nullable true, :type "integer"}
                         {:metadata {}, :name "cases", :nullable true, :type "integer"}
                         {:metadata {}, :name "deaths", :nullable true, :type "integer"}],
                     :type "struct"}},
 :protocol {:min-reader-version 1}}
```

## Reader version options

Delta tables support these [features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features).

Supported reader versions include:

| Reader Version |          Feature map         |
| -------------- | ---------------------------- |
| 1 (default)    | `{:response-format 'parquet'}` |
| 2              | `{:response-format 'delta' :reader-features 'columnMapping'}` |
| 3              | `{:response-format 'delta' :reader-features 'columnMapping, deletionVectors, timestampNtz, v2Checkpoint, vacuumProtocolCheck'}` |

The default `min-reader-version` is 1, but you can pass a `:response-format` and/or `:reader-features` to override the default or selected reader version as well.

## Using a custom flow handler

You may want to Implement your own custom flow handler for (a)sync calls, retries, observability etc. For these cases, you can pass a handler that implements the protocol [here](src/amperity/delta_sharing/client/flow.clj) as an optional argument during client construction. 

Here's an example of how to construct a client using the [provided](src/amperity/delta_sharing/client/flow.clj) `CompletableFuture` flow handler included with this library.

```clojure
;; Pull in relevant namespaces
user=> (require '[amperity.delta-sharing.client :as dsc])
nil

user=> (require '[amperity.delta-sharing.client.flow :as f])
nil

;; Construct the client leveraging the optional parameters to inject a custom flow handler
user=> (def client (dsc/new-client "https://sharing.delta.io/delta-sharing/" "my auth token" 1 {:flow (f/completable-future-handler)}))
#'user/client

user=> (dsc/list-shares client {:max-results 50})
=> #object[java.util.concurrent.CompletableFuture 0x27210a69 "java.util.concurrent.CompletableFuture@27210a69[Not completed]"]

```

## Local Development

The client code can all be exercised in a REPL this is as simple as running
`lein repl` .
