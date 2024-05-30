(ns delta-sharing.util
  "Delta-sharing API utilities"
  (:require
    [clojure.string :as str]
    [clojure.walk :as walk]))


;; ## Misc utils


(defn assoc-some
  "Associates one or more key-value pairs to the given map as long as
  value is non-nil."
  [m & kvs]
  (when-not (even? (count kvs))
    (throw (ex-info "assoc-some expects an even number of kv pairs" {})))
  (into (or m {})
        (comp
          (map vec)
          (filter (comp some? second)))
        (partition 2 kvs)))


;; ## Key conversion

(defn walk-keys
  "Update the provided data structure by calling `f` on each map key."
  [data f]
  (walk/postwalk
    (fn xform
      [x]
      (if (map? x)
        (into (empty x)
              (map (juxt (comp f key) val))
              x)
        x))
    data))


(defn- not-single
  "Returns the coll when there's more than one item or nil otherwise."
  [coll]
  (when (< 1 (count coll))
    coll))


(defn kebab->camelCase
  "Return a camelcased version of the kebab-cased input string where all the words
  separated by `-` are combined into a camelCased format.
  Examples: 
  1. 'foo-bar' => 'fooBar'
  2. 'foo-barBaz' => 'foo-Barbaz'"
  [s]
  (if-let [[first & rest] (not-single (str/split s #"-"))]
    (str/join "" (cons (str/lower-case first) (map str/capitalize rest)))
    s))


(defn camelify-keys
  "Converts kebab cased clojure keywords or strings into camelcased strings."
  [data]
  (walk-keys data (comp kebab->camelCase name)))


(defn ->kebab-keyword
  "Return a kebab cased keyword."
  [s]
  (when-let [[first & rest] (or (not-single (str/split s #"_"))
                                (not-single (str/split s #"(?=[A-Z])"))
                                (vector s))]
    (keyword
      (str/join "-" (cons (str/lower-case first) (map str/lower-case rest))))))


;; ## Secret protection

(deftype Veil
  [value])


(defn veil
  [x]
  (->Veil x))


(defn unveil
  [x]
  (if (instance? Veil x)
    (.-value ^Veil x)
    x))
