(defproject com.amperity/delta-sharing-client "MONOLITH-SNAPSHOT"
  :description "Clojure delta-sharing client implementation"

  :dependencies
  [[org.clojure/clojure "1.11.3"]
   [org.clojure/data.json "2.5.0"]
   [http-kit "2.7.0"]]

  :profiles
  {:repl
   {:source-paths ["dev"]
    :dependencies [[org.clojure/tools.namespace "1.5.0"]]}})
