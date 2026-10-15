(ns cronus.store
  "The jobs and their attempts in D1. Every function takes the D1
  binding (`env.DB`) first and answers a promise. The schema is applied
  once per isolate with CREATE IF NOT EXISTS, so a fresh database works
  without a manual step; `schema.sql` is the one source, embedded at
  build time as `schema.mjs`."
  (:require ["./schema.mjs" :refer [schema]]
            [clojure.string :as str]))

(def ^:private ready #js {:done false})

(defn statements
  "The statements of the schema, comment lines removed."
  []
  (->> (str/split-lines schema)
       (remove #(str/starts-with? (str/trim %) "--"))
       (str/join "\n")
       (#(str/split % ";"))
       (map str/trim)
       (remove str/blank?)))

(defn ^:async ensure!
  "Applies the schema once per isolate."
  [db]
  (when-not (.-done ready)
    (js-await (.batch db (clj->js (map #(.prepare db %) (statements)))))
    (set! (.-done ready) true)))

(def job-columns
  ["id" "cron" "tz" "url" "method" "headers_json" "body" "timeout_ms" "attempts"
   "state" "next_run" "pending_scheduled" "pending_attempt" "created" "updated"
   "last_run" "last_status" "failures"])

(defn ^:async put-job!
  "Inserts the job or replaces its definition; a replaced job keeps its
  creation time and loses any pending retry."
  [db {:keys [id cron tz url method headers_json body timeout_ms attempts next_run now]}]
  (js-await
    (-> (.prepare db "INSERT INTO jobs (id, cron, tz, url, method, headers_json, body, timeout_ms, attempts, state, next_run, pending_scheduled, pending_attempt, created, updated, failures)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'active', ?10, NULL, 0, ?11, ?11, 0)
                      ON CONFLICT(id) DO UPDATE SET cron = ?2, tz = ?3, url = ?4, method = ?5, headers_json = ?6, body = ?7,
                        timeout_ms = ?8, attempts = ?9, state = 'active', next_run = ?10, pending_scheduled = NULL,
                        pending_attempt = 0, updated = ?11")
        (.bind id cron tz url method headers_json (if (nil? body) nil body) timeout_ms attempts next_run now)
        (.run)))
  nil)

(defn ^:async get-job
  "The job row, or nil."
  [db id]
  (let [row (js-await (-> (.prepare db "SELECT * FROM jobs WHERE id = ?1") (.bind id) (.first)))]
    (if (nil? row) nil row)))

(defn ^:async list-jobs
  "Jobs whose id starts with `prefix`, ordered by id, after `after`, at
  most `limit`."
  [db prefix limit after]
  (let [result (js-await (-> (.prepare db "SELECT * FROM jobs WHERE substr(id, 1, ?1) = ?2 AND id > ?3 ORDER BY id LIMIT ?4")
                             (.bind (count prefix) prefix (or after "") limit)
                             (.all)))]
    (.-results result)))

(defn ^:async delete-job!
  "Removes the job and its attempts; answers whether it existed."
  [db id]
  (let [results (js-await (.batch db #js [(-> (.prepare db "DELETE FROM attempts WHERE job_id = ?1") (.bind id))
                                           (-> (.prepare db "DELETE FROM jobs WHERE id = ?1") (.bind id))]))
        deleted (aget results 1)]
    (pos? (or (.-changes (.-meta deleted)) 0))))

(defn ^:async due-jobs
  "Active jobs that are due or have a retry waiting, oldest first."
  [db now]
  (let [result (js-await (-> (.prepare db "SELECT * FROM jobs WHERE state = 'active' AND (next_run <= ?1 OR pending_scheduled IS NOT NULL) ORDER BY next_run LIMIT 2000")
                             (.bind now)
                             (.all)))]
    (.-results result)))

(defn ^:async update-job!
  "Sets the given columns (a map of column name to value) and `updated`."
  [db id fields now]
  (let [names (keys fields)
        sets (str/join ", " (map-indexed (fn [i n] (str n " = ?" (+ i 2))) names))
        values (map #(get fields %) names)
        sql (str "UPDATE jobs SET " sets ", updated = ?" (+ (count names) 2) " WHERE id = ?1")]
    (js-await (.run (.apply (.-bind (.prepare db sql)) (.prepare db sql) (clj->js (concat [id] values [now])))))
    nil))

(defn ^:async record-attempt!
  "Appends the attempt and keeps the last 20 of the job."
  [db {:keys [job_id scheduled attempt mode at status duration_ms answer ok]}]
  (js-await (.batch db #js [(-> (.prepare db "INSERT INTO attempts (job_id, scheduled, attempt, mode, at, status, duration_ms, answer, ok) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)")
                                (.bind job_id scheduled attempt mode at (if (nil? status) nil status) duration_ms (if (nil? answer) nil answer) (if ok 1 0)))
                            (-> (.prepare db "DELETE FROM attempts WHERE job_id = ?1 AND rowid NOT IN (SELECT rowid FROM attempts WHERE job_id = ?1 ORDER BY rowid DESC LIMIT 20)")
                                (.bind job_id))]))
  nil)

(defn ^:async attempts-of
  "The last 20 attempts of the job, the latest recorded first."
  [db id]
  (let [result (js-await (-> (.prepare db "SELECT scheduled, attempt, mode, at, status, duration_ms, answer, ok FROM attempts WHERE job_id = ?1 ORDER BY rowid DESC LIMIT 20")
                             (.bind id)
                             (.all)))]
    (.-results result)))
