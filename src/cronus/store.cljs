(ns cronus.store
  "The jobs and their attempts in D1. Every function takes the D1
  binding (`env.DB`) first and answers a promise. The schema lives in
  `migrations/` and is applied with `wrangler d1 migrations apply`.

  A firing claims its job with a lease before the call and writes its
  outcome only while the row is still at the version the claim wrote
  (`updated`), so two overlapping minutes never fire the same scheduled
  time, and an edit or a delete during a call wins over the stale write."
  (:require [clojure.string :as str]))

(def batch-limit
  "Due jobs taken per minute; each costs two D1 statements, well inside
  D1's budget per invocation."
  300)

(defn ^:async put-job!
  "Inserts the job or replaces its definition; a replaced job keeps its
  creation time and loses any pending retry and any lease."
  [db {:keys [id cron tz url method headers_json body timeout_ms attempts next_run now]}]
  (js-await
    (-> (.prepare db "INSERT INTO jobs (id, cron, tz, url, method, headers_json, body, timeout_ms, attempts, next_run, pending_scheduled, pending_attempt, lease_until, created, updated, failures)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, 0, NULL, ?11, ?11, 0)
                      ON CONFLICT(id) DO UPDATE SET cron = ?2, tz = ?3, url = ?4, method = ?5, headers_json = ?6, body = ?7,
                        timeout_ms = ?8, attempts = ?9, next_run = ?10, pending_scheduled = NULL, pending_attempt = 0,
                        lease_until = NULL, updated = ?11")
        (.bind id cron tz url method headers_json (if (nil? body) nil body) timeout_ms attempts next_run now)
        (.run)))
  nil)

(defn ^:async get-job
  "The job row, or nil."
  [db id]
  (let [row (js-await (-> (.prepare db "SELECT * FROM jobs WHERE id = ?1") (.bind id) (.first)))]
    (if (nil? row) nil row)))

(def ^:private after-everything
  "The highest code point; `prefix` + it bounds the ids with that prefix
  from above, so the listing is a range on the primary key."
  (js/String.fromCodePoint 0x10FFFF))

(defn ^:async list-jobs
  "Jobs whose id starts with `prefix`, ordered by id, after `after`, at
  most `limit`."
  [db prefix limit after]
  (let [result (js-await (-> (.prepare db "SELECT * FROM jobs WHERE id >= ?1 AND id < ?2 AND id > ?3 ORDER BY id LIMIT ?4")
                             (.bind prefix (str prefix after-everything) (or after "") limit)
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
  "Jobs that are due at `now` (the minute) or have a retry waiting, and
  are not leased at `wall-now`, oldest first, at most `batch-limit`."
  [db now wall-now]
  (let [result (js-await (-> (.prepare db "SELECT * FROM jobs WHERE (next_run <= ?1 OR pending_scheduled IS NOT NULL) AND (lease_until IS NULL OR lease_until < ?2) ORDER BY next_run LIMIT ?3")
                             (.bind now wall-now batch-limit)
                             (.all)))]
    (.-results result)))

(defn ^:async claim!
  "Leases the job until `lease-until` when it is still at `version` and
  not leased at `wall-now`; answers the new version, or nil when another
  minute holds the job or the row changed since it was read."
  [db id version lease-until wall-now]
  (let [new-version (max (inc version) wall-now)
        result (js-await (-> (.prepare db "UPDATE jobs SET lease_until = ?2, updated = ?3 WHERE id = ?1 AND updated = ?4 AND (lease_until IS NULL OR lease_until < ?5)")
                             (.bind id lease-until new-version version wall-now)
                             (.run)))]
    (when (pos? (or (.-changes (.-meta result)) 0))
      new-version)))

(defn ^:async finish!
  "The outcome of a firing in one batch: the attempt row (the job keeps
  its last 20), and the job's `fields` (column name to value), the lease
  released and `updated` set to `now`, written only while the job is
  still at `version`. A job deleted during the call gets no attempt row.
  Answers whether the job was written."
  [db {:keys [job_id version now attempt fields]}]
  (let [{:keys [scheduled attempt mode at status duration_ms answer ok]} attempt
        names (keys fields)
        sets (str/join ", " (map-indexed (fn [i n] (str n " = ?" (+ i 4))) names))
        sql (str "UPDATE jobs SET lease_until = NULL, updated = ?2" (when (seq names) ", ") sets " WHERE id = ?1 AND updated = ?3")
        update (.prepare db sql)
        results (js-await (.batch db #js [(-> (.prepare db "INSERT INTO attempts (job_id, scheduled, attempt, mode, at, status, duration_ms, answer, ok)
                                                              SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9 WHERE EXISTS (SELECT 1 FROM jobs WHERE id = ?1)")
                                              (.bind job_id scheduled attempt mode at (if (nil? status) nil status) duration_ms (if (nil? answer) nil answer) (if ok 1 0)))
                                          (-> (.prepare db "DELETE FROM attempts WHERE job_id = ?1 AND rowid NOT IN (SELECT rowid FROM attempts WHERE job_id = ?1 ORDER BY rowid DESC LIMIT 20)")
                                              (.bind job_id))
                                          (.apply (.-bind update) update (clj->js (concat [job_id now version] (map #(get fields %) names))))]))]
    (pos? (or (.-changes (.-meta (aget results 2))) 0))))

(defn ^:async attempts-of
  "The last 20 attempts of the job, the latest recorded first."
  [db id]
  (let [result (js-await (-> (.prepare db "SELECT scheduled, attempt, mode, at, status, duration_ms, answer, ok FROM attempts WHERE job_id = ?1 ORDER BY rowid DESC LIMIT 20")
                             (.bind id)
                             (.all)))]
    (.-results result)))
