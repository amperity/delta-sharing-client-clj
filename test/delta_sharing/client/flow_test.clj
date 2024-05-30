(ns delta-sharing.client.flow-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [delta-sharing.client.flow :as f])
  (:import
    (clojure.lang
      ExceptionInfo
      IPending)
    (java.util.concurrent
      CompletableFuture)))


(deftest sync-handler
  (let [handler (f/sync-handler {:retry-duration 2000})]
    (testing "success"
      (let [result (f/call!
                     handler nil
                     (fn request
                       [{:keys [result] :as state}]
                       (is (instance? IPending result))
                       (is (not (realized? result)))
                       (is (any? (f/on-success! handler state nil :ok)))
                       (is (realized? result))))]
        (is (= :ok result))
        (is (= :ok (f/await handler result)))
        (is (= :ok (f/await handler result 1 :still-waiting)))))

    (testing "error"
      (is (thrown-with-msg?
            ExceptionInfo
            #"BOOM!"
            (f/call!
              handler nil
              (fn request
                [{:keys [result] :as state}]
                (is (instance? IPending result))
                (is (not (realized? result)))
                (is (any? (f/on-error! handler state nil (ex-info "BOOM!" {}))))
                (is (realized? result)))))))

    (testing "retryable error responses"
      (testing "timed out"
        (let [state-ref (volatile! nil)]
          (is (thrown-with-msg?
                ExceptionInfo
                #"INTERNAL ERROR"
                (f/call!
                  handler nil
                  (fn request
                    [{:keys [result] :as state}]
                    (is (instance? IPending result))
                    (is (not (realized? result)))
                    (if-not (:retries @state-ref)
                      (do
                        (is (any? (f/on-error! handler (vreset! state-ref (update state :retries (fnil inc 0)))
                                               nil (ex-info "SLOW DOWN!" {:delta-sharing.client/status 429}))))
                        (is (realized? result))
                        (is (= 2 (:retries @state-ref))))
                      (do
                        (is (not (realized? result)))
                        (is (= 1 (:retries @state-ref)))
                        (is (any? (f/on-error! handler (vreset! state-ref (update state :retries (fnil inc 0)))
                                               nil (ex-info "INTERNAL ERROR" {:delta-sharing.client/status 500}))))
                        (is (realized? result))))))))))

      (testing "successful retry"
        (let [state-ref (volatile! nil)]
          (is (= :ok
                 (f/call!
                   handler nil
                   (fn request
                     [{:keys [result] :as state}]
                     (is (instance? IPending result))
                     (is (not (realized? result)))
                     (if-not (:retries @state-ref)
                       (do
                         (is (any? (f/on-error! handler (vreset! state-ref (update state :retries (fnil inc 0)))
                                                nil (ex-info "SLOW DOWN!" {:delta-sharing.client/status 429}))))
                         (is (realized? result))
                         (is (= 1 (:retries @state-ref))))
                       (do
                         (is (not (realized? result)))
                         (is (= 1 (:retries @state-ref)))
                         (is (any? (f/on-success! handler @state-ref nil :ok)))
                         (is (realized? result)))))))))))))


(deftest completable-future-handler
  (let [handler (f/completable-future-handler {:retry-duration 2000})]
    (testing "success repsonses"
      (let [state-ref (volatile! nil)
            result (f/call!
                     handler nil
                     (fn request
                       [{:keys [result] :as state}]
                       (vreset! state-ref state)
                       (is (instance? CompletableFuture result))
                       (is (not (.isDone result)))))]
        (is (instance? CompletableFuture result))
        (is (not (.isDone result)))
        (is (= :still-waiting (f/await handler result 1 :still-waiting)))
        (is (any? (f/on-success! handler @state-ref nil :ok)))
        (is (.isDone result))
        (is (= :ok @result))
        (is (= :ok (f/await handler result)))
        (is (= :ok (f/await handler result 1 :still-waiting)))))

    (testing "error responses"
      (let [state-ref (volatile! nil)
            result (f/call!
                     handler nil
                     (fn request
                       [{:keys [result] :as state}]
                       (vreset! state-ref state)
                       (is (instance? CompletableFuture result))
                       (is (not (.isDone result)))))]
        (is (instance? CompletableFuture result))
        (is (not (.isDone result)))
        (is (= :still-waiting (f/await handler result 1 :still-waiting)))
        (is (any? (f/on-error! handler @state-ref nil (ex-info "BOOM!" {}))))
        (is (.isDone result))
        (is (thrown-with-msg? Exception #"BOOM!" @result))
        (is (= "BOOM!" (try @result
                            (catch Exception e
                              (ex-message (ex-cause e))))))
        (is (thrown-with-msg? Exception #"BOOM!" (f/await handler result)))
        (is (thrown-with-msg? Exception #"BOOM!" (f/await handler result 1 :still-waiting)))))

    (testing "retryable error responses"
      (testing "timed out"
        (let [state-ref (volatile! nil)
              result (f/call!
                       handler nil
                       (fn request
                         [{:keys [result] :as state}]
                         (vreset! state-ref (update state
                                                    :retries
                                                    (fnil inc 0)))
                         (is (instance? CompletableFuture result))
                         (is (not (.isDone result)))))]
          (is (instance? CompletableFuture result))
          (is (not (.isDone result)))
          (is (= :still-waiting (f/await handler result 1 :still-waiting)))
          (is (any? (f/on-error! handler @state-ref nil (ex-info "SLOW DOWN!"
                                                                 {:delta-sharing.client/status 429}))))
          (is (not (.isDone result)))
          (is (= 1 (:retries @state-ref)))
          (Thread/sleep 1000)
          (is (any? (f/on-error! handler @state-ref nil (ex-info "INTERNAL ERROR"
                                                                 {:delta-sharing.client/status 500}))))
          (is (.isDone result))
          (is (thrown-with-msg? Exception #"INTERNAL ERROR" @result)
              "Dereffing the result yields a thrown wrapped concurrent exception.")
          (is (= "INTERNAL ERROR" (try @result
                                       (catch Exception e
                                         (ex-message (ex-cause e))))))
          (is (thrown-with-msg? ExceptionInfo #"INTERNAL ERROR" (f/await handler result))
              "Awaiting the result yeilds the original exception.")
          (is (thrown-with-msg? ExceptionInfo #"INTERNAL ERROR" (f/await handler result 1 :still-waiting))
              "Awaiting the result yeilds the original exception.")))

      (testing "successful retry"
        (let [state-ref (volatile! nil)
              result (f/call!
                       handler nil
                       (fn request
                         [{:keys [result] :as state}]
                         (vreset! state-ref (update state
                                                    :retries
                                                    (fnil inc 0)))
                         (is (instance? CompletableFuture result))
                         (is (not (.isDone result)))))]
          (is (instance? CompletableFuture result))
          (is (not (.isDone result)))
          (is (= :still-waiting (f/await handler result 1 :still-waiting)))
          (is (any? (f/on-error! handler @state-ref nil (ex-info "SLOW DOWN!"
                                                                 {:delta-sharing.client/status 429}))))
          (is (not (.isDone result)))
          (is (= 1 (:retries @state-ref)))
          (Thread/sleep 1000)
          (is (any? (f/on-success! handler @state-ref nil :ok)))
          (is (.isDone result))
          (is (= :ok @result))
          (is (= :ok (f/await handler result)))
          (is (= :ok (f/await handler result 1 :still-waiting))))))))
