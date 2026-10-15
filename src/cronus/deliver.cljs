(ns cronus.deliver
  "One firing of a job: the HTTP call with the idempotency headers, the
  verdict (ok, retry, fail), the attempt row and the job's new state.

  At least once: a firing is attempted until the target answers 2xx, up
  to the job's `attempts`, one attempt per minute. A network error, a
  timeout, a 5xx and a 429 are retried; any other status ends the
  firing as failed at once. A firing that is more than an hour overdue
  is skipped, so a long outage does not replay a day of runs."
  (:require [cronus.cron :as cron]
            [cronus.store :as store]))

(def catch-up-ms
  "How long a missed scheduled time is still fired."
  (* 60 60 1000))

(defn verdict
  "`:ok`, `:retry` or `:fail` for a status (nil = no answer)."
  [status]
  (cond
    (nil? status) :retry
    (and (>= status 200) (< status 300)) :ok
    (= status 429) :retry
    (>= status 500) :retry
    :else :fail))

(defn- iso [millis]
  (.toISOString (js/Date. millis)))

(defn request-of
  "The Request for one attempt: the job's method, headers and body plus
  `X-Cronus-Job`, `X-Cronus-Scheduled` and `X-Cronus-Attempt`; without a
  body of its own the job sends those three as JSON."
  [{:keys [id url method headers_json body]} scheduled attempt signal]
  (let [headers (js/Headers. (js/JSON.parse (or headers_json "{}")))
        method (or method "POST")
        with-body? (not (contains? #{"GET" "HEAD"} method))]
    (.set headers "X-Cronus-Job" id)
    (.set headers "X-Cronus-Scheduled" (iso scheduled))
    (.set headers "X-Cronus-Attempt" (str attempt))
    (when (and with-body? (nil? body) (not (.has headers "content-type")))
      (.set headers "content-type" "application/json"))
    (js/Request. url {:method method
                      :headers headers
                      :body (when with-body?
                              (if (nil? body)
                                (js/JSON.stringify {:job id :scheduled (iso scheduled) :attempt attempt})
                                body))
                      :signal signal})))

(defn ^:async attempt!
  "Performs one attempt; answers `{:status :ok? :duration_ms :answer :verdict}`."
  [job scheduled attempt]
  (let [controller (js/AbortController.)
        timer (js/setTimeout #(.abort controller) (or (:timeout_ms job) 30000))
        started (js/Date.now)
        result (try
                 (let [response (js-await (js/fetch (request-of job scheduled attempt (.-signal controller))))
                       text (try (js-await (.text response)) (catch :default _ ""))]
                   {:status (.-status response)
                    :answer (.slice (str text) 0 200)})
                 (catch :default e
                   {:status nil
                    :answer (.slice (str (if (= (.-name e) "AbortError") "timeout" (or (.-message e) e))) 0 200)}))]
    (js/clearTimeout timer)
    (let [v (verdict (:status result))]
      (assoc result
             :duration_ms (- (js/Date.now) started)
             :ok? (= v :ok)
             :verdict v))))

(defn- next-after
  [{:keys [cron tz]} millis]
  (cron/next-run-of cron tz millis))

(defn ^:async fire!
  "The due firing of the job at `now`: the attempt that is next (a
  first one, or the retry waiting), its row, and the job's new state.
  `mode` is \"scheduled\" or \"manual\"; a manual firing never moves the
  schedule. Answers the attempt result, or `{:skipped true}`."
  [db job now mode]
  (let [{:keys [id attempts pending_scheduled pending_attempt next_run failures]} job
        manual? (= mode "manual")
        retrying? (and (not manual?) (some? pending_scheduled))
        scheduled (cond manual? now
                        retrying? pending_scheduled
                        :else next_run)
        attempt (if retrying? (inc (or pending_attempt 0)) 1)]
    (if (and (not manual?) (not retrying?) (> (- now scheduled) catch-up-ms))
      (do (js-await (store/record-attempt! db {:job_id id :scheduled scheduled :attempt 0 :mode "skipped" :at now
                                               :status nil :duration_ms 0 :answer "skipped: more than an hour overdue" :ok false}))
          (js-await (store/update-job! db id {"next_run" (next-after job now)} now))
          {:skipped true})
      (let [{:keys [status ok? duration_ms answer verdict] :as result} (js-await (attempt! job scheduled attempt))
            final-failure? (or (= verdict :fail) (and (= verdict :retry) (>= attempt (or attempts 3))))
            fields (cond
                     ok? {"pending_scheduled" nil "pending_attempt" 0 "failures" 0
                          "last_run" now "last_status" "ok"}
                     final-failure? {"pending_scheduled" nil "pending_attempt" 0 "failures" (inc (or failures 0))
                                     "last_run" now "last_status" "failed"}
                     :else {"pending_scheduled" scheduled "pending_attempt" attempt "last_status" "retrying"})
            fields (if (and (not manual?) (or ok? final-failure?))
                     (assoc fields "next_run" (next-after job (max scheduled now)))
                     fields)]
        (js-await (store/record-attempt! db {:job_id id :scheduled scheduled :attempt attempt :mode mode :at now
                                             :status status :duration_ms duration_ms :answer answer :ok ok?}))
        (js-await (store/update-job! db id fields now))
        result))))

(defn ^:async run-all!
  "Runs the thunks (each answering a promise) with at most `limit` in
  flight; answers when all are done."
  [thunks limit]
  (let [queue (vec thunks)
        cursor #js {:i 0}
        worker (fn ^:async worker []
                 (loop []
                   (let [i (.-i cursor)]
                     (when (< i (.-length queue))
                       (set! (.-i cursor) (inc i))
                       (try (js-await ((aget queue i)))
                            (catch :default e (js/console.error "cronus: a firing threw" (or (.-message e) e))))
                       (recur)))))]
    (js-await (js/Promise.all (mapv (fn [_] (worker)) (range (min limit (.-length queue))))))
    nil))

(defn ^:async tick!
  "The minute: every due job fired, six at a time."
  [db now]
  (js-await (store/ensure! db))
  (let [jobs (js-await (store/due-jobs db now))]
    (js-await (run-all! (mapv (fn [job] (fn [] (fire! db job now "scheduled"))) jobs) 6))
    (count jobs)))
