(ns cronus.store
  "The jobs in D1, each row with its last attempt. Every function takes
  the D1 binding (`env.DB`) first and answers a promise. The schema lives in
  `migrations/` and is applied with `wrangler d1 migrations apply`.

  A job row has a `version`, a counter that every PUT and every claim
  increments. A firing claims its job with a lease before the call and
  records its outcome only while the row is still at the version the
  claim wrote, so two overlapping minutes never fire the same scheduled
  time, a stale firing writes nothing, and an edit or a delete during a
  call wins."
  (:require [clojure.string :as str]))

(def batch-limit
  "Due jobs taken per minute; each costs one D1 statement, well inside
  D1's budget per invocation."
  300)

(defn- bind
  "`stmt.bind` with every nil (or undefined, which D1 refuses) as NULL."
  [stmt & values]
  (.apply (.-bind stmt) stmt (clj->js (mapv (fn [v] (if (nil? v) nil v)) values))))

(defn- changed?
  "Whether a run statement changed a row."
  [result]
  (pos? (or (.-changes (.-meta result)) 0)))

(defn ^:async put-job!
  "Inserts the job or replaces its definition; a replaced job keeps its
  creation time, gets a new version and loses any pending retry and any
  lease."
  [db {:keys [id cron tz url timeout_ms attempts next_run now]}]
  (js-await
    (-> (.prepare db "INSERT INTO jobs (id, cron, tz, url, timeout_ms, attempts, version, next_run, pending_scheduled, pending_attempt, lease_until, created, updated, failures)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, NULL, 0, NULL, ?8, ?8, 0)
                      ON CONFLICT(id) DO UPDATE SET cron = ?2, tz = ?3, url = ?4, timeout_ms = ?5, attempts = ?6,
                        version = version + 1, next_run = ?7, pending_scheduled = NULL, pending_attempt = 0,
                        lease_until = NULL, updated = ?8")
        (bind id cron tz url timeout_ms attempts next_run now)
        (.run)))
  nil)

(defn ^:async get-job
  "The job row, or nil."
  [db id]
  (let [row (js-await (-> (.prepare db "SELECT * FROM jobs WHERE id = ?1") (bind id) (.first)))]
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
                             (bind prefix (str prefix after-everything) (or after "") limit)
                             (.all)))]
    (.-results result)))

(defn ^:async delete-job!
  "Removes the job; answers whether it existed."
  [db id]
  (changed? (js-await (-> (.prepare db "DELETE FROM jobs WHERE id = ?1") (bind id) (.run)))))

(defn ^:async due-jobs
  "Jobs that are due at `now` (the minute) or have a retry waiting, and
  are not leased at `wall-now`, oldest first, at most `batch-limit`."
  [db now wall-now]
  (let [result (js-await (-> (.prepare db "SELECT * FROM jobs WHERE (next_run <= ?1 OR pending_scheduled IS NOT NULL) AND (lease_until IS NULL OR lease_until < ?2) ORDER BY next_run LIMIT ?3")
                             (bind now wall-now batch-limit)
                             (.all)))]
    (.-results result)))

(defn ^:async claim!
  "Leases the job until `lease-until` and counts the attempt: the
  firing in progress becomes `pending_scheduled` (the retry waiting, or
  the next run) at `pending_attempt` + 1, the version one higher. Only
  while the row is still at `version` and not leased at `wall-now`;
  answers the new version, or nil when another minute holds the job or
  the row changed since it was read."
  [db id version lease-until wall-now]
  (let [result (js-await (-> (.prepare db "UPDATE jobs SET lease_until = ?2, version = version + 1,
                                                           pending_scheduled = COALESCE(pending_scheduled, next_run),
                                                           pending_attempt = pending_attempt + 1
                                             WHERE id = ?1 AND version = ?3 AND (lease_until IS NULL OR lease_until < ?4)")
                             (bind id lease-until version wall-now)
                             (.run)))]
    (when (changed? result)
      (inc version))))

(defn ^:async finish!
  "The outcome of a firing: the job's `fields` (column name to value),
  the lease released and `updated` set to `now`, written only while the
  job is still at `version`; a stale firing writes nothing. Answers
  whether the job was written."
  [db {:keys [job_id version now fields]}]
  (let [names (keys fields)
        sets (str/join ", " (map-indexed (fn [i n] (str n " = ?" (+ i 4))) names))
        sql (str "UPDATE jobs SET lease_until = NULL, updated = ?2" (when (seq names) ", ") sets " WHERE id = ?1 AND version = ?3")]
    (changed? (js-await (.run (apply bind (.prepare db sql) job_id now version (map #(get fields %) names)))))))
