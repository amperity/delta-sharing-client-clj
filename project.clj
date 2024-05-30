(defproject com.amperity/delta-sharing-client "0.1.0"
  :description "Clojure delta-sharing client implementation"
  :url "https://github.com/amperity/delta-sharing-client-clj"
  :license {:name "MIT License"
            :url "https://mit-license.org/"}

  :plugins
  [[lein-cloverage "1.2.2"]]

  :dependencies
  [[org.clojure/clojure "1.11.3"]
   [org.clojure/data.json "2.5.0"]
   [http-kit "2.7.0"]]

  :profiles
  {:repl
   {:source-paths ["dev"]
    :dependencies [[org.clojure/tools.namespace "1.5.0"]]}})
