(ns cronus.deliver
  "One firing of a job: the claim, the HTTP call with the idempotency
  headers, the verdict (ok, retry, fail), and the attempt row with the
  job's new state, written together.

  At least once: a firing is attempted until the target answers 2xx, up
  to the job's `attempts`, one attempt per minute. A network error, a
  timeout, a 5xx and a 429 are retried; any other status, a redirect
  among them, ends the firing as failed at once. A firing that is more
  than an hour overdue is skipped, so a long outage does not replay a
  day of runs."
  (:require [cronus.cron :as cron]
            [cronus.store :as store]))

(def catch-up-ms
  "How long a missed scheduled time is still fired."
  (* 60 60 1000))

(def answer-limit
  "How much of a target's answer is read, in bytes; the rest is
  cancelled, so a huge answer cannot exhaust the invocation."
  65536)

(def lease-margin-ms
  "A lease lasts the job's timeout plus this, for the writes after the
  call."
  15000)

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
  body of its own the job sends those three as JSON. Redirects are not
  followed: a 3xx is an answer like any other."
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
                      :redirect "manual"
                      :body (when with-body?
                              (if (nil? body)
                                (js/JSON.stringify {:job id :scheduled (iso scheduled) :attempt attempt})
                                body))
                      :signal signal})))

(defn ^:async read-head
  "Up to `answer-limit` bytes of the response body as text; the rest is
  cancelled."
  [response]
  (let [body (.-body response)]
    (if (nil? body)
      ""
      (let [reader (.getReader body)
            decoder (js/TextDecoder.)]
        (loop [acc "" taken 0]
          (let [chunk (js-await (.read reader))]
            (cond
              (.-done chunk) acc
              (>= taken answer-limit) (do (try (js-await (.cancel reader)) (catch :default _ nil))
                                          acc)
              :else (recur (str acc (.decode decoder (.-value chunk) {:stream true}))
                           (+ taken (.-byteLength (.-value chunk)))))))))))

(defn- clip [text]
  (.slice (str text) 0 200))

(defn ^:async attempt!
  "Performs one attempt; answers `{:status :ok? :duration_ms :answer :verdict}`.
  A request that cannot even be built (a stored URL or header that the
  runtime refuses) fails without a retry."
  [job scheduled attempt]
  (let [controller (js/AbortController.)
        timer (js/setTimeout #(.abort controller) (or (:timeout_ms job) 30000))
        started (js/Date.now)
        built (try {:request (request-of job scheduled attempt (.-signal controller))}
                   (catch :default e {:invalid (clip (str "invalid request: " (or (.-message e) e)))}))
        result (if (:invalid built)
                 {:status nil :answer (:invalid built) :verdict :fail}
                 (try
                   (let [response (js-await (js/fetch (:request built)))
                         text (try (js-await (read-head response)) (catch :default _ ""))]
                     {:status (.-status response) :answer (clip text)})
                   (catch :default e
                     {:status nil
                      :answer (clip (if (= (.-name e) "AbortError") "timeout" (or (.-message e) e)))})))]
    (js/clearTimeout timer)
    (let [v (or (:verdict result) (verdict (:status result)))]
      (assoc result
             :duration_ms (- (js/Date.now) started)
             :ok? (= v :ok)
             :verdict v))))

(defn- next-after
  [{:keys [cron tz]} millis]
  (cron/next-run-of cron tz millis))

(defn ^:async fire!
  "The due firing of the job at `now` (the minute): the claim, the
  attempt that is next (a first one, or the retry waiting), and its row
  with the job's new state. Answers the attempt result, `{:skipped true}`
  for a scheduled time too old, or `{:claimed false}` when another
  minute holds the job or the row changed since it was read."
  [db job now]
  (let [{:keys [id attempts pending_scheduled pending_attempt next_run failures updated timeout_ms]} job
        retrying? (some? pending_scheduled)
        scheduled (if retrying? pending_scheduled next_run)
        attempt (if retrying? (inc (or pending_attempt 0)) 1)
        wall-now (js/Date.now)
        version (js-await (store/claim! db id updated (+ wall-now (or timeout_ms 30000) lease-margin-ms) wall-now))]
    (cond
      (nil? version)
      {:claimed false}

      (and (not retrying?) (> (- now scheduled) catch-up-ms))
      (do (js-await (store/finish! db {:job_id id :version version :now (js/Date.now)
                                       :attempt {:scheduled scheduled :attempt 0 :mode "skipped" :at now :status nil
                                                 :duration_ms 0 :answer "skipped: more than an hour overdue" :ok false}
                                       :fields {"next_run" (next-after job now)}}))
          {:skipped true})

      :else
      (let [{:keys [status ok? duration_ms answer verdict] :as result} (js-await (attempt! job scheduled attempt))
            final-failure? (or (= verdict :fail) (and (= verdict :retry) (>= attempt (or attempts 3))))
            fields (cond
                     ok? {"pending_scheduled" nil "pending_attempt" 0 "failures" 0
                          "last_run" now "last_status" "ok"}
                     final-failure? {"pending_scheduled" nil "pending_attempt" 0 "failures" (inc (or failures 0))
                                     "last_run" now "last_status" "failed"}
                     :else {"pending_scheduled" scheduled "pending_attempt" attempt "last_status" "retrying"})
            fields (if (or ok? final-failure?)
                     (assoc fields "next_run" (next-after job (max scheduled now)))
                     fields)]
        (js-await (store/finish! db {:job_id id :version version :now (js/Date.now)
                                     :attempt {:scheduled scheduled :attempt attempt :mode "scheduled" :at now
                                               :status status :duration_ms duration_ms :answer answer :ok ok?}
                                     :fields fields}))
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
  (let [jobs (js-await (store/due-jobs db now (js/Date.now)))]
    (js-await (run-all! (mapv (fn [job] (fn [] (fire! db job now))) jobs) 6))
    (count jobs)))
