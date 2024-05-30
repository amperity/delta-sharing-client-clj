(ns delta-sharing.client.flow
  "A collection of functions which determines how requests and responses
  are handled via the delta-sharing client"
  (:refer-clojure :exclude [await])
  (:import
    (java.util.concurrent
      CompletableFuture
      ExecutionException
      ScheduledExecutorService
      ScheduledThreadPoolExecutor
      TimeUnit
      TimeoutException)))


;; ## Utilities


(defn- throttling?
  "True if the exception returned is a 429 response from Databricks."
  [status]
  (= 429 status))


(defn- server-fault?
  "True if the exception represents a 5xx HTTP response from the delta-sharing API."
  [status]
  (<= 500 status 599))


(defn- retryable-error?
  "True if the exception is a retryable delta-sharing error."
  [ex]
  (let [status (->> ex (ex-data) (:delta-sharing.client/status))]
    (when (integer? status)
      (or (throttling? status)
          (server-fault? status)))))


(defn- throwing-deref
  "A variant of `deref` which will throw if the pending value yields an
  exception."
  ([pending]
   (throwing-deref pending nil nil))
  ([pending timeout-ms timeout-val]
   (let [x (if timeout-ms
             (deref pending timeout-ms timeout-val)
             @pending)]
     (if (instance? Throwable x)
       (throw x)
       x))))


;; ## Protocol


(defprotocol Handler
  "Protocol for a handler which controls how client requests should be exposed
  to the consumer of the delta-sharing APIs."

  (call!
    [handler info f]
    "Create a new state container and invoke the function on it to initiate a
    request. Returns the result object the client should see. The `info` map
    may contain additional observability information.")

  (on-success!
    [handler state info data]
    "Callback indicating a successful response with the given response data.
    Should modify the state; the result of this call is not used.")

  (on-error!
    [handler state info ex]
    "Callback indicating a failure response with the given exception. Should
    modify the state; the result of this call is not used.")

  (await
    [handler result]
    [handler result timeout-ms timeout-val]
    "Wait for the given call to complete, blocking the current thread if
    necessary. Returns the response value on success, throws an exception on
    failure, or returns `timeout-val` if supplied and `timeout-ms` milliseconds
    pass while waiting.

    This will be invoked on the value returned by the `call` method, not the
    internal state object."))


;; ## Default flow handler


(deftype SyncHandler
  [retry-interval retry-duration]

  Handler

  (call!
    [_ info f]
    (let [start (System/nanoTime)
          deadline (+ start (* retry-duration 1000 1000))
          info (atom info)
          result (promise)
          state {:fn f
                 :start start
                 :deadline deadline
                 :info (atom info)
                 :result result}]
      (f state)
      (throwing-deref result)))


  (on-success!
    [_ state _ data]
    (deliver (:result state) data))


  (on-error!
    [_ state _ ex]
    (if (and (retryable-error? ex)
             (< (+ (System/nanoTime) (* retry-interval 1000 1000))
                (:deadline state)))
      (let [f (:fn state)]
        ;; retry request
        (Thread/sleep retry-interval)
        (f state))
      ;; terminal error or out of retries
      (deliver (:result state) ex)))


  (await
    [_ result]
    result)


  (await
    [_ result _ _]
    result))


(alter-meta! #'->SyncHandler assoc :private true)


(defn sync-handler
  "The synchronous handler will block the thread calling the API and will
  return either the response data (on success) or throw an exception (on
  error)."
  [& {:keys [retry-interval retry-duration]
      :or {retry-interval 1000
           retry-duration 30000}}]
  (->SyncHandler retry-interval retry-duration))


;; ## Completable future handler


(defn- retry-in
  "Schedules a function to be run in `interval-millis` and returns a CompletableFuture."
  [^ScheduledExecutorService scheduler interval-millis f]
  (.schedule scheduler f interval-millis TimeUnit/MILLISECONDS))


(deftype CompletableFutureHandler
  [retry-interval retry-duration]

  Handler

  (call!
    [_ info f]
    (let [start (System/nanoTime)
          deadline (+ start (* retry-duration 1000 1000))
          info (atom info)
          result (CompletableFuture.)
          state {:fn f
                 :info info
                 :start start
                 :deadline deadline
                 :executor (doto
                             (ScheduledThreadPoolExecutor. 1)
                             (.setRemoveOnCancelPolicy true))
                 :result result}]
      (f state)
      result))


  (on-success!
    [_ state _ data]
    (.complete ^CompletableFuture (:result state) data))


  (on-error!
    [_ state _ ex]
    (if (and (retryable-error? ex)
             (< (+ (System/nanoTime) (* retry-interval 1000 1000))
                (:deadline state)))
      ;; retry request
      (let [f (:fn state)]
        (retry-in (:executor state) retry-interval (fn retry [] (f state))))
      ;; terminal error or out of retries
      (.completeExceptionally ^CompletableFuture (:result state) ex)))


  (await
    [_ result]
    (try
      (.get ^CompletableFuture result)
      (catch ExecutionException ex
        (throw (or (ex-cause ex) ex)))))


  (await
    [_ result timeout-ms timeout-val]
    (try
      (.get ^CompletableFuture result timeout-ms TimeUnit/MILLISECONDS)
      (catch ExecutionException ex
        (throw (or (ex-cause ex) ex)))
      (catch TimeoutException _
        timeout-val))))


(alter-meta! #'->CompletableFutureHandler assoc :private true)


(defn completable-future-handler
  "The asynchronous handler will return a CompletableFuture that either 
  yields the response data (on success) or throw an exception (on error).
  When an exception is thrown, the returned exception is a
  `java.util.concurrent.ExecutionException` where the original exception is its cause."
  [& {:keys [retry-interval retry-duration]
      :or {retry-interval 1000
           retry-duration 30000}}]
  (->CompletableFutureHandler retry-interval retry-duration))
