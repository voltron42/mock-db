(ns mock-db.core-test
  (:require [clojure.test :refer :all]
            [mock-db.core :refer :all]
            [clojure.java.jdbc :as jdbc]))

(defonce run-times (atom 0))

(defmethod mock-jdbc [:query "select * from my_table"] [_ _ _]
  (swap! run-times inc)
  [{:id 12345 :message "This is fake data."}])

(deftest test-with-mock-db
  (with-mock-db "This is the mock db atom"
                (reset! run-times 0)
                (let [db "this is the db object that I don't care about"
                      results (jdbc/query db ["select * from my_table"])]
                  (is (= results [{:id 12345 :message "This is fake data."}]))
                  (is (= @run-times 1))
                  (try
                    (jdbc/update! db :another_table {:name "Dan"} ["where id = ?" 543201])
                    (catch Throwable e
                      (is (= (type e) UnsupportedOperationException))
                      (is (= (.getMessage e) "(:update! :another_table #{:name} \"where id = ?\")")))))))

