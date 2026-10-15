(ns cronus.deliver
  "The two halves of a firing. The minute (`tick!`) claims every due job
  and puts one message per job on the queue; the consumer (`deliver!`)
  makes the call and records the outcome. They share only the job row.

  Every call is a POST with the JSON body `{job, scheduled, attempt}`;
  the URL is the job's credential. At least once: a firing is attempted
  until the target answers 2xx, up to the job's `attempts`, one attempt
  per minute. A network error, a timeout, a 5xx and a 429 are retried;
  any other status, a redirect among them, ends the firing as failed at
  once. A firing that is more than an hour overdue is skipped, so a
  long outage does not replay a day of runs."
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
  "A lease lasts the job's timeout plus this: the wait in the queue and
  the writes after the call."
  60000)

(def send-batch-size
  "Messages per `sendBatch`, the queue's maximum."
  100)

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
  "The Request of one attempt: a POST to the job's URL with the JSON
  body `{job, scheduled, attempt}`, which the target can use to be
  idempotent. Redirects are not followed: a 3xx is an answer like any
  other, so the URL, the credential, never travels on."
  [{:keys [id url]} scheduled attempt signal]
  (js/Request. url {:method "POST"
                    :headers {"content-type" "application/json"}
                    :redirect "manual"
                    :body (js/JSON.stringify {:job id :scheduled (iso scheduled) :attempt attempt})
                    :signal signal}))

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
  A request that cannot even be built (a stored URL the runtime
  refuses) fails without a retry."
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

(defn ^:async tick!
  "The minute: every due job claimed (lease, attempt, version) and put
  on the queue as `{id, version, minute}`; answers how many. A job
  another minute holds, or whose row changed since it was read, is left
  alone."
  [db queue now]
  (let [jobs (js-await (store/due-jobs db now (js/Date.now)))
        messages #js []]
    (doseq [job jobs]
      (let [wall-now (js/Date.now)
            lease-until (+ wall-now (or (:timeout_ms job) 30000) lease-margin-ms)
            version (js-await (store/claim! db (:id job) (:version job) lease-until wall-now))]
        (when (some? version)
          (.push messages {:body {:id (:id job) :version version :minute now}}))))
    (loop [i 0]
      (when (< i (.-length messages))
        (js-await (.sendBatch queue (.slice messages i (+ i send-batch-size))))
        (recur (+ i send-batch-size))))
    (.-length messages)))

(defn ^:async deliver!
  "One message from the queue. The job is read again: a version other
  than the claim's, or no lease, means the message is stale (the lease
  expired and a later minute claimed again, a PUT replaced the job, or
  the outcome is already written) and nothing happens. Otherwise a
  first attempt more than an hour overdue is skipped; an attempt beyond
  the budget fails without a call, since the earlier outcomes were
  never written; else the call is made and the outcome written. Answers
  what happened as a keyword."
  [db {:keys [id version minute]}]
  (let [job (js-await (store/get-job db id))]
    (cond
      (nil? job) :gone
      (or (not= (:version job) version) (nil? (:lease_until job))) :stale
      :else
      (let [{:keys [attempts pending_scheduled pending_attempt failures]} job
            scheduled pending_scheduled
            attempt pending_attempt
            budget (or attempts 3)
            done! (fn [fields]
                    (store/finish! db {:job_id id :version version :now (js/Date.now) :fields fields}))]
        (cond
          (and (= attempt 1) (> (- minute scheduled) catch-up-ms))
          (do (js-await (done! {"pending_scheduled" nil "pending_attempt" 0 "next_run" (next-after job minute)
                                "last_status" "skipped" "last_http_status" nil "last_duration_ms" 0
                                "last_answer" (str "skipped: " (iso scheduled) " was more than an hour overdue")}))
              :skipped)

          (> attempt budget)
          (do (js-await (done! {"pending_scheduled" nil "pending_attempt" 0 "failures" (inc (or failures 0))
                                "last_run" minute "last_status" "failed" "last_http_status" nil "last_duration_ms" 0
                                "last_answer" "no attempts left; an earlier outcome was never written"
                                "next_run" (next-after job (max scheduled minute))}))
              :failed)

          :else
          (let [{:keys [status ok? duration_ms answer verdict]} (js-await (attempt! job scheduled attempt))
                final-failure? (or (= verdict :fail) (and (= verdict :retry) (>= attempt budget)))
                fields (cond
                         ok? {"pending_scheduled" nil "pending_attempt" 0 "failures" 0
                              "last_run" minute "last_status" "ok"
                              "next_run" (next-after job (max scheduled minute))}
                         final-failure? {"pending_scheduled" nil "pending_attempt" 0 "failures" (inc (or failures 0))
                                         "last_run" minute "last_status" "failed"
                                         "next_run" (next-after job (max scheduled minute))}
                         :else {"last_status" "retrying"})
                fields (assoc fields "last_http_status" status "last_duration_ms" duration_ms "last_answer" answer)]
            (js-await (done! fields))
            (cond ok? :ok final-failure? :failed :else :retrying)))))))
