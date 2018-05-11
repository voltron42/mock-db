(ns mock-db.core
  (require [clojure.java.jdbc :as jdbc]))

(defmulti mock-jdbc (fn [id-args _ _] id-args))

(defmethod mock-jdbc :default [id-args _ _]
  (throw (UnsupportedOperationException. (pr-str id-args))))

(defmulti ^:private apply-options (fn [fn-name _ _] fn-name))

(defmethod ^:private apply-options :default [_ _ results] results)

(defmethod ^:private apply-options :query
  [_ {:keys [result-set-fn row-fn]
      :or {result-set-fn identity
           row-fn identity}}
   results]
  (result-set-fn (map row-fn results)))

(defn- apply-mock [fn-name db opts [& id-args] args]
  (apply-options fn-name opts (mock-jdbc (cons fn-name id-args) db args)))

(defn build-mock-query [db-atom]
  (fn mock-query
    ([db sql+args] (mock-query db sql+args {}))
    ([_ [sql & args] opts]
     (apply-mock :query db-atom opts [sql] args))))

(defn build-mock-find-by-keys [db-atom]
  (fn mock-find-by-keys
    ([db table columns] (mock-find-by-keys db table columns {}))
    ([_ table columns opts]
     (apply-mock :find-by-keys db-atom opts [table (set (keys columns))] columns))))

(defn build-mock-get-by-id [db-atom]
  (fn mock-get-by-id
    ([db table pk-value] (mock-get-by-id db table pk-value {}))
    ([db table pk-value pk-name-or-opts]
     (if (map? pk-name-or-opts)
       (mock-get-by-id db table pk-value :id pk-name-or-opts)
       (mock-get-by-id db table pk-value pk-name-or-opts {})))
    ([_ table pk-value pk-name opts]
     (apply-mock :get-by-id db-atom opts [table] [pk-value pk-name]))))

(defn build-mock-execute [db-atom]
  (fn mock-execute
    ([db sql+args] (mock-execute db sql+args {}))
    ([_ [sql & args] opts]
     (apply-mock :execute! db-atom opts [sql] args))))

(defn build-mock-update [db-atom]
  (fn mock-update
    ([db table set-map where-clause] (mock-update db table set-map where-clause {}))
    ([_ table set-map [where & params] opts]
     (apply-mock :update! db-atom opts [table (set (keys set-map)) where] [set-map params]))))

(defn build-mock-delete [db-atom]
  (fn mock-delete
    ([db table where-clause] (mock-delete db table where-clause {}))
    ([_ table [where & params] opts]
     (apply-mock :delete! db-atom opts [table where] params))))

(defn build-mock-insert [db-atom]
  (fn mock-insert
    ([db table rows] (mock-insert db table rows {}))
    ([db table cols-or-row values-or-opts]
     (if (map? values-or-opts)
       (let [cols (keys cols-or-row)
             values (map (partial get cols-or-row) cols)]
         (mock-insert db table cols values values-or-opts))
       (mock-insert db table cols-or-row values-or-opts {})))
    ([_ table cols values opts]
     (apply-mock :insert! db-atom opts [table cols] values))))

(defn build-mock-insert-multi [db-atom]
  (fn mock-insert-multi
    ([db table rows] (mock-insert-multi db table rows {}))
    ([db table cols-or-row values-or-opts]
     (if (map? values-or-opts)
       (let [cols (keys cols-or-row)
             values (map (partial get cols-or-row) cols)]
         (mock-insert-multi db table cols values values-or-opts))
       (mock-insert-multi db table cols-or-row values-or-opts {})))
    ([_ table cols values opts]
     (apply-mock :insert-multi! db-atom opts [table cols] values))))

(defmacro with-mock-db [db-atom & body]
  `(with-redefs [jdbc/query (build-mock-query ~db-atom)
                 jdbc/find-by-keys (build-mock-find-by-keys ~db-atom)
                 jdbc/execute! (build-mock-execute ~db-atom)
                 jdbc/update! (build-mock-update ~db-atom)
                 jdbc/delete! (build-mock-delete ~db-atom)
                 jdbc/insert! (build-mock-insert ~db-atom)
                 jdbc/insert-multi! (build-mock-insert-multi ~db-atom)
                 jdbc/get-by-id (build-mock-get-by-id ~db-atom)]
     ~@body))
