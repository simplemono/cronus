(ns worker.api-test
  "The API, the minute and the consumer inside workerd: the migrations
  applied to the local D1 and the table emptied per test, the queue a
  fake that hands the minute's messages to the consumer, the targets
  answered by a stub of the global fetch."
  (:require ["cloudflare:test" :refer [env SELF applyD1Migrations createScheduledController createMessageBatch createExecutionContext waitOnExecutionContext getQueueResult]]
            ["vitest" :refer [test expect beforeEach afterEach vi]]
            ["../cronus/worker.mjs" :as worker]))

(def auth {"authorization" "Bearer test-secret" "content-type" "application/json"})

(def calls
  "What the targets received: {:url :method :headers :body :redirect}, in order."
  #js [])

(def replies
  "What the targets answer next, in order: {:status n :body s}, or
  {:status \"error\"} for a network error, {:status \"hang\"} for no
  answer until the timeout; `:delay` ms before the answer, `:before` a
  thunk run before it. Empty = 200 ok."
  #js [])

(def queued
  "The message bodies the minute put on the fake queue, not yet delivered."
  #js [])

(def fake-queue
  #js {:sendBatch (fn [messages]
                    (doseq [m messages] (.push queued (.-body m)))
                    (js/Promise.resolve nil))})

(defn worker-env
  "The Worker's env with the fake queue in place of the binding; built
  per call, since the plugin gives every test its own storage and a
  binding taken earlier points at another one."
  []
  #js {:DB (.-DB env) :CRONUS_SECRET (.-CRONUS_SECRET env) :QUEUE fake-queue})

(defn ^:async pause [ms]
  (js-await (js/Promise. (fn [resolve] (js/setTimeout resolve ms)))))

(defn ^:async stub-fetch [req]
  (let [body (js-await (.text req))
        headers {}]
    (.forEach (.-headers req) (fn [v k] (assoc! headers k v)))
    (.push calls {:url (.-url req) :method (.-method req) :headers headers :body body :redirect (.-redirect req)})
    (let [reply (or (.shift replies) {:status 200 :body "ok"})]
      (when (:delay reply) (js-await (pause (:delay reply))))
      (when (:before reply) (js-await ((:before reply))))
      (case (:status reply)
        "error" (throw (js/TypeError. "connection refused"))
        "hang" (js-await (js/Promise. (fn [_ reject]
                                        (.addEventListener (.-signal req) "abort"
                                                           (fn [] (let [e (js/Error. "aborted")]
                                                                    (set! (.-name e) "AbortError")
                                                                    (reject e)))))))
        (js/Response. (:body reply) {:status (:status reply)})))))

(defn ^:async fresh-table!
  "The migrations applied (once; they are tracked) and the table
  emptied; the plugin keeps the D1 state across the tests of a file."
  []
  (let [db (.-DB env)]
    (js-await (applyD1Migrations db (.-TEST_MIGRATIONS env)))
    (js-await (.run (.prepare db "DELETE FROM jobs")))))

(beforeEach (fn ^:async before []
              (set! (.-length calls) 0)
              (set! (.-length replies) 0)
              (set! (.-length queued) 0)
              (.stubGlobal vi "fetch" stub-fetch)
              (js-await (fresh-table!))))

(afterEach (fn []
             (.unstubAllGlobals vi)
             (.toBe (expect (.-length replies)) 0)
             (.toBe (expect (.-length queued)) 0)))

(defn ^:async call [method path body]
  (js-await (.fetch SELF (str "https://cronus.test" path)
                    (clj->js (cond-> {:method method :headers auth}
                               body (assoc :body (js/JSON.stringify (clj->js body))))))))

(defn ^:async body-of [response]
  (js-await (.json response)))

(defn ^:async show [id]
  (js-await (body-of (js-await (call "GET" (str "/jobs/" id) nil)))))

(defn ^:async put-job [id job]
  (let [r (js-await (call "PUT" (str "/jobs/" id) job))]
    [(.-status r) (js-await (body-of r))]))

(defn ^:async claim-minute!
  "The scheduled handler at `at-millis`: the due jobs claimed and their
  messages on the fake queue."
  [at-millis]
  (let [controller (createScheduledController {:scheduledTime (js/Date. at-millis) :cron "* * * * *"})
        ctx (createExecutionContext)]
    (js-await ((.-scheduled (.-default worker)) controller (worker-env) ctx))
    (js-await (waitOnExecutionContext ctx))))

(defn ^:async deliver-message!
  "One message through the consumer; answers the queue result."
  [body]
  (let [batch (createMessageBatch "cronus" #js [{:id (str (js/crypto.randomUUID)) :timestamp (js/Date.) :body body :attempts 1}])
        ctx (createExecutionContext)]
    (js-await ((.-queue (.-default worker)) batch (worker-env) ctx))
    (js-await (getQueueResult batch ctx))))

(defn ^:async deliver-queued!
  "Every queued message through the consumer, in order."
  []
  (let [messages (.slice queued 0)]
    (set! (.-length queued) 0)
    (doseq [m messages]
      (js-await (deliver-message! m)))))

(defn ^:async run-minute!
  "A whole minute: claim, then deliver."
  [at-millis]
  (js-await (claim-minute! at-millis))
  (js-await (deliver-queued!)))

(defn ^:async expire-lease!
  "As if the lease of the job had run out (the consumer never wrote)."
  [id]
  (js-await (.run (.bind (.prepare (.-DB env) "UPDATE jobs SET lease_until = 0 WHERE id = ?1") id))))

(defn ^:async auth-rules []
  (let [r (js-await (.fetch SELF "https://cronus.test/jobs"))]
    (.toBe (expect (.-status r)) 401))
  (let [r (js-await (.fetch SELF "https://cronus.test/jobs" {:headers {"authorization" "Bearer wrong"}}))]
    (.toBe (expect (.-status r)) 401))
  ;; the secret alone, without the Bearer scheme, is not a credential
  (let [r (js-await (.fetch SELF "https://cronus.test/jobs" {:headers {"authorization" "test-secret"}}))]
    (.toBe (expect (.-status r)) 401))
  (let [r (js-await (.fetch SELF "https://cronus.test/jobs" {:headers {"authorization" "Bearer test-secret"}}))]
    (.toBe (expect (.-status r)) 200))
  (let [r (js-await (.fetch SELF "https://cronus.test/health"))]
    (.toBe (expect (.-status r)) 200)))

(test "no secret, no entry; Bearer is required; health is open" auth-rules)

(defn ^:async put-get-list-delete []
  (let [[status job] (js-await (put-job "org1_agentA_job1" {:cron "0 9 * * 1-5" :tz "Europe/Berlin" :url "https://target.test/run?token=t1"}))]
    (.toBe (expect status) 200)
    (.toBe (expect (:tz job)) "Europe/Berlin")
    (.toBe (expect (:url job)) "https://target.test/run?token=t1")
    (.toMatch (expect (:next_run job)) #"T0[78]:00:00.000Z$")
    ;; unset values are null, never absent
    (.toBe (expect (:last_run job)) nil)
    (.toBe (expect (js/Object.hasOwn job "last_run")) true)
    (.toBe (expect (js/Object.hasOwn job "last_answer")) true)
    (.toBe (expect (:pending job)) nil))
  (js-await (put-job "org1_agentB_job2" {:cron "@hourly" :url "https://target.test/other"}))
  (js-await (put-job "org2_agentC_job3" {:cron "@daily" :url "https://target.test/x"}))
  (let [[status job] (js-await (put-job "org1%25agentD_job4" {:cron "@daily" :url "https://target.test/y"}))]
    (.toBe (expect status) 200)
    (.toBe (expect (:id job)) "org1%agentD_job4"))
  (let [{:keys [jobs next_after]} (js-await (body-of (js-await (call "GET" "/jobs?prefix=org1_" nil))))]
    (.toEqual (expect (mapv :id jobs)) #js ["org1_agentA_job1" "org1_agentB_job2"])
    (.toBe (expect next_after) nil))
  ;; % and _ are plain characters in a prefix
  (let [{:keys [jobs]} (js-await (body-of (js-await (call "GET" "/jobs?prefix=org1%25" nil))))]
    (.toEqual (expect (mapv :id jobs)) #js ["org1%agentD_job4"]))
  (let [{:keys [jobs next_after]} (js-await (body-of (js-await (call "GET" "/jobs?limit=2" nil))))]
    (.toEqual (expect (mapv :id jobs)) #js ["org1%agentD_job4" "org1_agentA_job1"])
    (.toBe (expect next_after) "org1_agentA_job1"))
  (let [{:keys [jobs]} (js-await (body-of (js-await (call "GET" "/jobs?limit=2&after=org1_agentA_job1" nil))))]
    (.toEqual (expect (mapv :id jobs)) #js ["org1_agentB_job2" "org2_agentC_job3"]))
  ;; a limit that is not a number falls back to the default, a zero to one
  (let [r (js-await (call "GET" "/jobs?limit=abc" nil))]
    (.toBe (expect (.-status r)) 200)
    (.toBe (expect (count (:jobs (js-await (body-of r))))) 4))
  (let [{:keys [jobs]} (js-await (body-of (js-await (call "GET" "/jobs?limit=0" nil))))]
    (.toBe (expect (count jobs)) 1))
  (let [job (js-await (show "org1_agentA_job1"))]
    (.toBe (expect (:cron job)) "0 9 * * 1-5"))
  ;; a second PUT replaces the definition
  (let [[_ job] (js-await (put-job "org1_agentA_job1" {:cron "@hourly" :url "https://target.test/run2"}))]
    (.toBe (expect (:cron job)) "@hourly")
    (.toBe (expect (:url job)) "https://target.test/run2"))
  (let [r (js-await (call "DELETE" "/jobs/org1_agentA_job1" nil))]
    (.toBe (expect (.-status r)) 200))
  (let [r (js-await (call "GET" "/jobs/org1_agentA_job1" nil))]
    (.toBe (expect (.-status r)) 404))
  (let [r (js-await (call "DELETE" "/jobs/org1_agentA_job1" nil))]
    (.toBe (expect (.-status r)) 404))
  ;; no other verbs on a job, none but GET on the collection
  (let [r (js-await (call "POST" "/jobs/org1_agentB_job2/run" nil))]
    (.toBe (expect (.-status r)) 404))
  (let [r (js-await (call "POST" "/jobs/org1_agentB_job2" nil))]
    (.toBe (expect (.-status r)) 405))
  (let [r (js-await (call "POST" "/jobs" nil))]
    (.toBe (expect (.-status r)) 405)))

(test "put, get, list by prefix, replace, delete" put-get-list-delete)

(defn ^:async refused [id job pattern]
  (let [[status body] (js-await (put-job id job))]
    (.toBe (expect status) 400)
    (.toMatch (expect (:error body)) pattern)))

(defn ^:async bad-definitions []
  (js-await (refused "x" {:cron "0 9 * *" :url "https://t.test/"} #"five"))
  (js-await (refused "x" {:cron "0 0 9 * * *" :url "https://t.test/"} #"five fields"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :tz "Mars/Olympus"} #"time zone"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "ftp://t.test/"} #"http"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://"} #"valid"))
  (js-await (refused "x" {:cron "0 9 * * *"} #"url"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :attempts 50} #"attempts"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :attempts 2.5} #"whole number"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :timeout_ms 1500.5} #"whole number"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :timeout_ms 0} #"timeout_ms"))
  (js-await (refused "x" {:cron "0 0 30 2 *" :url "https://t.test/"} #"never"))
  (let [r (js-await (call "PUT" "/jobs/x" nil))]
    (.toBe (expect (.-status r)) 400))
  (let [r (js-await (.fetch SELF "https://cronus.test/jobs/x" (clj->js {:method "PUT" :headers auth :body "[1,2]"})))]
    (.toBe (expect (.-status r)) 400))
  (let [r (js-await (call "PUT" "/jobs/bad%00id" {:cron "0 9 * * *" :url "https://t.test/"}))]
    (.toBe (expect (.-status r)) 400))
  (let [r (js-await (call "PUT" (str "/jobs/" (.repeat "a" 201)) {:cron "0 9 * * *" :url "https://t.test/"}))]
    (.toBe (expect (.-status r)) 400))
  ;; the nth weekday of a month is supported
  (let [[status job] (js-await (put-job "second-friday" {:cron "0 9 * * 5#2" :url "https://t.test/"}))]
    (.toBe (expect status) 200)
    (.toMatch (expect (:next_run job)) #"T09:00:00.000Z$"))
  (let [r (js-await (call "GET" "/jobs/x" nil))]
    (.toBe (expect (.-status r)) 404)))

(test "a bad definition is refused with the reason" bad-definitions)

(defn ^:async minute-fires []
  (let [[_ job] (js-await (put-job "j" {:cron "* * * * *" :url "https://target.test/run?token=secret-1"}))
        [_ _] (js-await (put-job "later" {:cron "0 0 1 1 *" :url "https://target.test/never"}))
        due (js/Date.parse (:next_run job))]
    (js-await (run-minute! (- due 60000)))
    (.toBe (expect (.-length calls)) 0)
    (js-await (run-minute! due))
    (.toBe (expect (.-length calls)) 1)
    (let [{:keys [url method headers body redirect]} (aget calls 0)]
      (.toBe (expect url) "https://target.test/run?token=secret-1")
      (.toBe (expect method) "POST")
      (.toBe (expect redirect) "manual")
      (.toBe (expect (:content-type headers)) "application/json")
      (.toEqual (expect (js/JSON.parse body)) #js {"job" "j" "scheduled" (:next_run job) "attempt" 1}))
    (let [after (js-await (show "j"))]
      (.toBe (expect (:last_status after)) "ok")
      (.toBe (expect (:last_http_status after)) 200)
      (.toBe (expect (:last_answer after)) "ok")
      (.toBe (expect (>= (:last_duration_ms after) 0)) true)
      (.toBe (expect (:last_run after)) (:next_run job))
      (.toBe (expect (:failures after)) 0)
      (.toBe (expect (:pending after)) nil)
      (.toBe (expect (js/Date.parse (:next_run after))) (+ due 60000)))
    ;; the same minute again does not fire again
    (js-await (run-minute! due))
    (.toBe (expect (.-length calls)) 1)))

(test "the minute fires a due job with the JSON body, once, then moves on" minute-fires)

(defn attempt-of [call]
  (:attempt (js/JSON.parse (:body call))))

(defn scheduled-of [call]
  (:scheduled (js/JSON.parse (:body call))))

(defn ^:async retries []
  (let [[_ job] (js-await (put-job "r" {:cron "* * * * *" :url "https://target.test/flaky" :attempts 3}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 503 :body "down"})
    (js-await (run-minute! due))
    (let [mid (js-await (show "r"))]
      (.toBe (expect (:last_status mid)) "retrying")
      (.toBe (expect (:last_http_status mid)) 503)
      (.toBe (expect (:last_answer mid)) "down")
      (.toBe (expect (:attempt (:pending mid))) 1)
      (.toBe (expect (:scheduled (:pending mid))) (:next_run job))
      ;; the schedule has not moved on while the firing is open
      (.toBe (expect (:next_run mid)) (:next_run job)))
    (.push replies {:status "error"})
    (js-await (run-minute! (+ due 60000)))
    (let [mid (js-await (show "r"))]
      (.toBe (expect (:attempt (:pending mid))) 2)
      (.toBe (expect (:last_http_status mid)) nil)
      (.toBe (expect (:last_answer mid)) "connection refused"))
    (js-await (run-minute! (+ due 120000)))
    (let [after (js-await (show "r"))]
      (.toBe (expect (:last_status after)) "ok")
      (.toBe (expect (:pending after)) nil)
      (.toBe (expect (:failures after)) 0)
      (.toBe (expect (attempt-of (aget calls 2))) 3)
      (.toBe (expect (scheduled-of (aget calls 2))) (:next_run job))
      (.toBe (expect (> (js/Date.parse (:next_run after)) (+ due 120000))) true))))

(test "a 5xx or a network error is retried next minute until 2xx, same scheduled time" retries)

(defn ^:async gives-up []
  (let [[_ job] (js-await (put-job "r2" {:cron "* * * * *" :url "https://target.test/dead" :attempts 2}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 500 :body "x"})
    (js-await (run-minute! due))
    (.push replies {:status 429 :body "slow down"})
    (js-await (run-minute! (+ due 60000)))
    (let [after (js-await (show "r2"))]
      (.toBe (expect (:last_status after)) "failed")
      (.toBe (expect (:last_http_status after)) 429)
      (.toBe (expect (:failures after)) 1)
      (.toBe (expect (:pending after)) nil)
      (.toBe (expect (js/Date.parse (:next_run after))) (+ due 120000)))
    ;; a definite refusal ends the firing at once
    (let [[_ job2] (js-await (put-job "f" {:cron "* * * * *" :url "https://target.test/gone"}))
          due2 (js/Date.parse (:next_run job2))]
      (.push replies {:status 404 :body "no"})
      (js-await (run-minute! due2))
      (let [after (js-await (show "f"))]
        (.toBe (expect (:last_status after)) "failed")
        (.toBe (expect (:failures after)) 1)
        (.toBe (expect (:pending after)) nil)
        (.toBe (expect (:last_http_status after)) 404)
        (.toBe (expect (js/Date.parse (:next_run after))) (+ due2 60000))))
    ;; a redirect is not followed and fails the firing at once
    (let [[_ job3] (js-await (put-job "moved" {:cron "* * * * *" :url "https://target.test/old"}))
          due3 (js/Date.parse (:next_run job3))]
      (.push replies {:status 302 :body ""})
      (js-await (run-minute! due3))
      (let [after (js-await (show "moved"))]
        (.toBe (expect (:last_status after)) "failed")
        (.toBe (expect (:pending after)) nil)
        (.toBe (expect (:last_http_status after)) 302)
        (.toBe (expect (.-length (.filter calls (fn [c] (= (:url c) "https://target.test/old"))))) 1)))))

(test "after the last attempt the firing fails; a 4xx or a 3xx fails at once" gives-up)

(defn ^:async timeout []
  (let [[_ job] (js-await (put-job "t" {:cron "* * * * *" :url "https://target.test/slow" :timeout_ms 1000 :attempts 1}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status "hang"})
    (js-await (run-minute! due))
    (let [after (js-await (show "t"))]
      (.toBe (expect (:last_status after)) "failed")
      (.toBe (expect (:last_http_status after)) nil)
      (.toBe (expect (:last_answer after)) "timeout")
      (.toBe (expect (>= (:last_duration_ms after) 900)) true))))

(test "no answer within timeout_ms counts as a failed attempt" timeout)

(defn ^:async overdue-is-skipped []
  (let [[_ job] (js-await (put-job "s" {:cron "* * * * *" :url "https://target.test/late"}))
        due (js/Date.parse (:next_run job))
        late (+ due (* 2 60 60 1000))]
    (js-await (run-minute! late))
    (.toBe (expect (.-length calls)) 0)
    (let [after (js-await (show "s"))]
      (.toBe (expect (:last_status after)) "skipped")
      (.toMatch (expect (:last_answer after)) #"more than an hour overdue")
      (.toBe (expect (:last_run after)) nil)
      (.toBe (expect (js/Date.parse (:next_run after))) (+ late 60000)))
    ;; 59 minutes late is still fired
    (let [after (js-await (show "s"))
          next (js/Date.parse (:next_run after))]
      (js-await (run-minute! (+ next (* 59 60 1000))))
      (.toBe (expect (.-length calls)) 1)
      (.toBe (expect (scheduled-of (aget calls 0))) (:next_run after)))))

(test "more than an hour overdue is skipped and noted; less is fired" overdue-is-skipped)

(defn ^:async many-at-once []
  (let [ids (mapv #(str "m" %) (range 15))]
    (doseq [id ids]
      (js-await (put-job id {:cron "* * * * *" :url (str "https://target.test/" id)})))
    (let [due (js/Date.parse (:next_run (js-await (show "m0"))))]
      (js-await (claim-minute! due))
      (.toBe (expect (.-length queued)) 15)
      (js-await (deliver-queued!))
      (.toBe (expect (.-length calls)) 15)
      (.toEqual (expect (sort (mapv #(.replace (:url %) "https://target.test/" "") (vec calls))))
                (clj->js (sort ids))))))

(test "many due jobs are all queued by one minute and delivered" many-at-once)

(defn ^:async overlapping-minutes []
  (let [[_ job] (js-await (put-job "o" {:cron "* * * * *" :url "https://target.test/o"}))
        due (js/Date.parse (:next_run job))]
    ;; the next minute starts while the first one's message is still in the queue
    (js-await (claim-minute! due))
    (.toBe (expect (.-length queued)) 1)
    (js-await (claim-minute! (+ due 60000)))
    (.toBe (expect (.-length queued)) 1)
    (js-await (deliver-queued!))
    (.toBe (expect (.-length calls)) 1)
    (let [after (js-await (show "o"))]
      (.toBe (expect (:last_status after)) "ok")
      ;; the lease is released and the job runs again at its next minute
      (js-await (run-minute! (+ due 120000)))
      (.toBe (expect (.-length calls)) 2))))

(test "a minute that overlaps the previous one does not queue the same job again" overlapping-minutes)

(defn ^:async expired-lease []
  (let [[_ job] (js-await (put-job "x" {:cron "* * * * *" :url "https://target.test/x"}))
        due (js/Date.parse (:next_run job))]
    ;; the consumer of minute one never ran: the lease runs out, minute two claims again
    (js-await (claim-minute! due))
    (let [stale (aget queued 0)]
      (set! (.-length queued) 0)
      (js-await (expire-lease! "x"))
      (js-await (claim-minute! (+ due 60000)))
      (.toBe (expect (.-length queued)) 1)
      ;; the old message is dropped, the new one calls as attempt 2 of the same scheduled time
      (let [result (js-await (deliver-message! stale))]
        (.toBe (expect (.-length (.-explicitAcks result))) 1))
      (.toBe (expect (.-length calls)) 0)
      (js-await (deliver-queued!))
      (.toBe (expect (.-length calls)) 1)
      (.toBe (expect (attempt-of (aget calls 0))) 2)
      (.toBe (expect (scheduled-of (aget calls 0))) (:next_run job))
      (.toBe (expect (:last_status (js-await (show "x")))) "ok"))))

(test "a lease that expired makes the next minute claim again; the stale message is dropped" expired-lease)

(defn ^:async budget-at-the-claim []
  (let [[_ job] (js-await (put-job "b" {:cron "* * * * *" :url "https://target.test/b" :attempts 2}))
        due (js/Date.parse (:next_run job))]
    ;; two claims whose outcomes were never written, then a third beyond the budget
    (js-await (claim-minute! due))
    (set! (.-length queued) 0)
    (js-await (expire-lease! "b"))
    (js-await (claim-minute! (+ due 60000)))
    (set! (.-length queued) 0)
    (js-await (expire-lease! "b"))
    (js-await (claim-minute! (+ due 120000)))
    (js-await (deliver-queued!))
    (.toBe (expect (.-length calls)) 0)
    (let [after (js-await (show "b"))]
      (.toBe (expect (:last_status after)) "failed")
      (.toMatch (expect (:last_answer after)) #"no attempts left")
      (.toBe (expect (:failures after)) 1)
      (.toBe (expect (:pending after)) nil)
      (.toBe (expect (js/Date.parse (:next_run after))) (+ due 180000)))))

(test "attempts are counted at the claim, so lost outcomes cannot fire forever" budget-at-the-claim)

(defn ^:async redelivered-message []
  (let [[_ job] (js-await (put-job "rd" {:cron "* * * * *" :url "https://target.test/rd"}))
        due (js/Date.parse (:next_run job))]
    (js-await (claim-minute! due))
    (let [message (aget queued 0)]
      (set! (.-length queued) 0)
      (js-await (deliver-message! message))
      ;; the queue delivers the same message again after a lost ack
      (js-await (deliver-message! message))
      (.toBe (expect (.-length calls)) 1))))

(test "a message delivered twice calls once" redelivered-message)

(defn ^:async deleted-during-a-call []
  (let [[_ job] (js-await (put-job "d" {:cron "* * * * *" :url "https://target.test/d"}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 200 :body "ok"
                    :before (fn [] (call "DELETE" "/jobs/d" nil))})
    (js-await (run-minute! due))
    (.toBe (expect (.-length calls)) 1)
    (let [r (js-await (call "GET" "/jobs/d" nil))]
      (.toBe (expect (.-status r)) 404))))

(test "a job deleted during its call stays deleted" deleted-during-a-call)

(defn ^:async replaced-during-a-call []
  (let [[_ job] (js-await (put-job "e" {:cron "* * * * *" :url "https://target.test/e"}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 503 :body "down"
                    :before (fn [] (call "PUT" "/jobs/e" {:cron "0 12 * * *" :url "https://target.test/e2"}))})
    (js-await (run-minute! due))
    (let [after (js-await (show "e"))]
      ;; the edit won: no retry of the old definition, the new schedule stands, no stale outcome
      (.toBe (expect (:cron after)) "0 12 * * *")
      (.toBe (expect (:pending after)) nil)
      (.toBe (expect (:last_status after)) nil)
      (.toMatch (expect (:next_run after)) #"T12:00:00.000Z$"))))

(test "a job replaced during its call keeps the new definition, not the stale outcome" replaced-during-a-call)

(defn ^:async big-answer []
  (let [[_ job] (js-await (put-job "big" {:cron "* * * * *" :url "https://target.test/big"}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 200 :body (.repeat "y" 300000)})
    (js-await (run-minute! due))
    (let [after (js-await (show "big"))]
      (.toBe (expect (:last_status after)) "ok")
      (.toBe (expect (:last_http_status after)) 200)
      (.toBe (expect (.-length (:last_answer after))) 200))))

(test "a huge answer is read only in part and recorded clipped" big-answer)
