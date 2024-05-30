(ns delta-sharing.client-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [delta-sharing.client :as dsc]))


(deftest new-client-construction

  (testing "incompatible minimum client reader version"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Client does not support reader version 0. Reader version should be >= 1"
          (dsc/new-client "" "" 0))))

  (testing "incompatible scheme"
    (is (thrown-with-msg? IllegalArgumentException
                          #"Unsupported delta-sharing address scheme: \"mock://endpoint\""
          (dsc/new-client "mock://endpoint" "" 1))))

  (is (some? (dsc/new-client "https://example.com/delta_sharing" "token" 1))))
