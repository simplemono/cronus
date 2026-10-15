(ns worker.api-test
  "The API and the minute inside workerd: a fresh local D1 per test, the
  targets answered by a stub of the global fetch."
  (:require ["cloudflare:test" :refer [env SELF createScheduledController createExecutionContext waitOnExecutionContext]]
            ["vitest" :refer [test expect beforeEach afterEach vi]]
            ["../cronus/worker.mjs" :as worker]
            [cronus.store :as store]))

(def auth {"authorization" "Bearer test-secret" "content-type" "application/json"})

(def calls
  "What the targets received: {:url :method :headers :body}, in order."
  #js [])

(def replies
  "What the targets answer next, in order: {:status n :body s}, or
  {:status \"error\"} for a network error, {:status \"hang\"} for no
  answer until the timeout. Empty = 200 ok."
  #js [])

(defn ^:async stub-fetch [req]
  (let [body (js-await (.text req))
        headers {}]
    (.forEach (.-headers req) (fn [v k] (assoc! headers k v)))
    (.push calls {:url (.-url req) :method (.-method req) :headers headers :body body})
    (let [reply (or (.shift replies) {:status 200 :body "ok"})]
      (case (:status reply)
        "error" (throw (js/TypeError. "connection refused"))
        "hang" (js-await (js/Promise. (fn [_ reject]
                                        (.addEventListener (.-signal req) "abort"
                                                           (fn [] (let [e (js/Error. "aborted")]
                                                                    (set! (.-name e) "AbortError")
                                                                    (reject e)))))))
        (js/Response. (:body reply) {:status (:status reply)})))))

(defn ^:async fresh-tables!
  "The schema applied and the tables emptied; the plugin keeps the D1
  state across the tests of a file."
  []
  (let [db (.-DB env)]
    (js-await (.batch db (clj->js (mapv #(.prepare db %) (store/statements)))))
    (js-await (.batch db #js [(.prepare db "DELETE FROM attempts") (.prepare db "DELETE FROM jobs")]))))

(beforeEach (fn ^:async before []
              (set! (.-length calls) 0)
              (set! (.-length replies) 0)
              (.stubGlobal vi "fetch" stub-fetch)
              (js-await (fresh-tables!))))

(afterEach (fn []
             (.unstubAllGlobals vi)
             (.toBe (expect (.-length replies)) 0)))

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

(defn ^:async run-minute! [at-millis]
  (let [controller (createScheduledController {:scheduledTime (js/Date. at-millis) :cron "* * * * *"})
        ctx (createExecutionContext)]
    (js-await ((.-scheduled (.-default worker)) controller env ctx))
    (js-await (waitOnExecutionContext ctx))))

(defn ^:async no-secret-no-entry []
  (let [r (js-await (.fetch SELF "https://cronus.test/jobs"))]
    (.toBe (expect (.-status r)) 401))
  (let [r (js-await (.fetch SELF "https://cronus.test/jobs" {:headers {"authorization" "Bearer wrong"}}))]
    (.toBe (expect (.-status r)) 401))
  (let [r (js-await (.fetch SELF "https://cronus.test/health"))]
    (.toBe (expect (.-status r)) 200)))

(test "no secret, no entry; health is open" no-secret-no-entry)

(defn ^:async put-get-list-delete []
  (let [[status job] (js-await (put-job "org1_agentA_job1" {:cron "0 9 * * 1-5" :tz "Europe/Berlin" :url "https://target.test/run" :headers {"X-Job-Token" "t1"}}))]
    (.toBe (expect status) 200)
    (.toBe (expect (:state job)) "active")
    (.toBe (expect (:method job)) "POST")
    (.toBe (expect (:tz job)) "Europe/Berlin")
    (.toMatch (expect (:next_run job)) #"T0[78]:00:00.000Z$"))
  (js-await (put-job "org1_agentB_job2" {:cron "@hourly" :url "https://target.test/other"}))
  (js-await (put-job "org2_agentC_job3" {:cron "@daily" :url "https://target.test/x"}))
  (let [{:keys [jobs next_after]} (js-await (body-of (js-await (call "GET" "/jobs?prefix=org1_" nil))))]
    (.toEqual (expect (mapv :id jobs)) #js ["org1_agentA_job1" "org1_agentB_job2"])
    (.toBe (expect next_after) nil))
  (let [{:keys [jobs next_after]} (js-await (body-of (js-await (call "GET" "/jobs?limit=2" nil))))]
    (.toEqual (expect (mapv :id jobs)) #js ["org1_agentA_job1" "org1_agentB_job2"])
    (.toBe (expect next_after) "org1_agentB_job2"))
  (let [{:keys [jobs]} (js-await (body-of (js-await (call "GET" "/jobs?limit=2&after=org1_agentB_job2" nil))))]
    (.toEqual (expect (mapv :id jobs)) #js ["org2_agentC_job3"]))
  (let [job (js-await (show "org1_agentA_job1"))]
    (.toBe (expect (:cron job)) "0 9 * * 1-5")
    (.toEqual (expect (:headers job)) #js {"X-Job-Token" "t1"})
    (.toEqual (expect (:recent_attempts job)) #js []))
  ;; a second PUT replaces the definition
  (let [[_ job] (js-await (put-job "org1_agentA_job1" {:cron "@hourly" :url "https://target.test/run2"}))]
    (.toBe (expect (:cron job)) "@hourly")
    (.toBe (expect (:url job)) "https://target.test/run2"))
  (let [r (js-await (call "DELETE" "/jobs/org1_agentA_job1" nil))]
    (.toBe (expect (.-status r)) 200))
  (let [r (js-await (call "GET" "/jobs/org1_agentA_job1" nil))]
    (.toBe (expect (.-status r)) 404))
  (let [r (js-await (call "DELETE" "/jobs/org1_agentA_job1" nil))]
    (.toBe (expect (.-status r)) 404)))

(test "put, get, list by prefix, replace, delete" put-get-list-delete)

(defn ^:async bad-definitions []
  (let [[status body] (js-await (put-job "x" {:cron "0 9 * *" :url "https://t.test/"}))]
    (.toBe (expect status) 400)
    (.toMatch (expect (:error body)) #"five fields"))
  (let [[status body] (js-await (put-job "x" {:cron "0 9 * * *" :url "https://t.test/" :tz "Mars/Olympus"}))]
    (.toBe (expect status) 400)
    (.toMatch (expect (:error body)) #"time zone"))
  (let [[status body] (js-await (put-job "x" {:cron "0 9 * * *" :url "ftp://t.test/"}))]
    (.toBe (expect status) 400)
    (.toMatch (expect (:error body)) #"http"))
  (let [[status body] (js-await (put-job "x" {:cron "0 9 * * *" :url "https://t.test/" :attempts 50}))]
    (.toBe (expect status) 400)
    (.toMatch (expect (:error body)) #"attempts"))
  (let [r (js-await (call "GET" "/jobs/x" nil))]
    (.toBe (expect (.-status r)) 404)))

(test "a bad definition is refused with the reason" bad-definitions)

(defn ^:async minute-fires []
  (let [[_ job] (js-await (put-job "j" {:cron "* * * * *" :url "https://target.test/run" :headers {"X-Job-Token" "secret-1"} :body {:hello "world"}}))
        [_ _] (js-await (put-job "later" {:cron "0 0 1 1 *" :url "https://target.test/never"}))
        due (js/Date.parse (:next_run job))]
    (js-await (run-minute! (- due 60000)))
    (.toBe (expect (.-length calls)) 0)
    (js-await (run-minute! due))
    (.toBe (expect (.-length calls)) 1)
    (let [{:keys [url method headers body]} (aget calls 0)]
      (.toBe (expect url) "https://target.test/run")
      (.toBe (expect method) "POST")
      (.toBe (expect (:x-cronus-job headers)) "j")
      (.toBe (expect (:x-cronus-attempt headers)) "1")
      (.toBe (expect (:x-job-token headers)) "secret-1")
      (.toBe (expect (:x-cronus-scheduled headers)) (:next_run job))
      (.toBe (expect body) "{\"hello\":\"world\"}"))
    (let [after (js-await (show "j"))]
      (.toBe (expect (:last_status after)) "ok")
      (.toBe (expect (:failures after)) 0)
      (.toBe (expect (js/Date.parse (:next_run after))) (+ due 60000))
      (.toBe (expect (count (:recent_attempts after))) 1)
      (.toBe (expect (:ok (first (:recent_attempts after)))) true)
      (.toBe (expect (:answer (first (:recent_attempts after)))) "ok"))
    ;; the same minute again does not fire again
    (js-await (run-minute! due))
    (.toBe (expect (.-length calls)) 1)))

(test "the minute fires a due job with the idempotency headers, once, then moves on" minute-fires)

(defn ^:async no-body-sends-json []
  (let [[_ job] (js-await (put-job "g" {:cron "* * * * *" :url "https://target.test/get" :method "GET"}))
        [_ job2] (js-await (put-job "p" {:cron "* * * * *" :url "https://target.test/post"}))]
    (js-await (run-minute! (js/Date.parse (:next_run job))))
    (let [[get-call post-call] (sort-by :url (vec calls))]
      (.toBe (expect (:method get-call)) "GET")
      (.toBe (expect (:body get-call)) "")
      (.toBe (expect (:method post-call)) "POST")
      (.toBe (expect (:content-type (:headers post-call))) "application/json")
      (.toEqual (expect (js/JSON.parse (:body post-call)))
                #js {"job" "p" "scheduled" (:next_run job2) "attempt" 1}))))

(test "without a body of its own, a POST job sends the three fields as JSON" no-body-sends-json)

(defn ^:async retries []
  (let [[_ job] (js-await (put-job "r" {:cron "* * * * *" :url "https://target.test/flaky" :attempts 3}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 503 :body "down"})
    (js-await (run-minute! due))
    (let [mid (js-await (show "r"))]
      (.toBe (expect (:last_status mid)) "retrying")
      (.toBe (expect (:attempts_done (:pending mid))) 1)
      (.toBe (expect (:scheduled (:pending mid))) (:next_run job))
      ;; the schedule has not moved on while the firing is open
      (.toBe (expect (:next_run mid)) (:next_run job)))
    (.push replies {:status "error"})
    (js-await (run-minute! (+ due 60000)))
    (.toBe (expect (:attempts_done (:pending (js-await (show "r"))))) 2)
    (js-await (run-minute! (+ due 120000)))
    (let [after (js-await (show "r"))]
      (.toBe (expect (:last_status after)) "ok")
      (.toBe (expect (:pending after)) nil)
      (.toBe (expect (:failures after)) 0)
      (.toBe (expect (:x-cronus-attempt (:headers (aget calls 2)))) "3")
      (.toBe (expect (:x-cronus-scheduled (:headers (aget calls 2)))) (:next_run job))
      (.toEqual (expect (mapv :attempt (:recent_attempts after))) #js [3 2 1])
      (.toBe (expect (:answer (second (:recent_attempts after)))) "connection refused")
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
        (.toBe (expect (:status (first (:recent_attempts after)))) 404)
        (.toBe (expect (js/Date.parse (:next_run after))) (+ due2 60000))))))

(test "after the last attempt the firing fails; a 4xx fails at once" gives-up)

(defn ^:async timeout []
  (let [[_ job] (js-await (put-job "t" {:cron "* * * * *" :url "https://target.test/slow" :timeout_ms 1000 :attempts 1}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status "hang"})
    (js-await (run-minute! due))
    (let [after (js-await (show "t"))
          attempt (first (:recent_attempts after))]
      (.toBe (expect (:last_status after)) "failed")
      (.toBe (expect (:status attempt)) nil)
      (.toBe (expect (:answer attempt)) "timeout")
      (.toBe (expect (>= (:duration_ms attempt) 900)) true))))

(test "no answer within timeout_ms counts as a failed attempt" timeout)

(defn ^:async skip-run-pause []
  (let [[_ job] (js-await (put-job "s" {:cron "* * * * *" :url "https://target.test/late"}))
        due (js/Date.parse (:next_run job))
        late (+ due (* 2 60 60 1000))]
    (js-await (run-minute! late))
    (.toBe (expect (.-length calls)) 0)
    (let [after (js-await (show "s"))]
      (.toBe (expect (:mode (first (:recent_attempts after)))) "skipped")
      (.toBe (expect (:last_status after)) nil)
      (.toBe (expect (js/Date.parse (:next_run after))) (+ late 60000)))
    ;; 59 minutes late is still fired
    (let [after (js-await (show "s"))
          next (js/Date.parse (:next_run after))]
      (js-await (run-minute! (+ next (* 59 60 1000))))
      (.toBe (expect (.-length calls)) 1)
      (.toBe (expect (:x-cronus-scheduled (:headers (aget calls 0)))) (:next_run after)))
    (let [body (js-await (body-of (js-await (call "POST" "/jobs/s/run" nil))))]
      (.toBe (expect (:ok body)) true)
      (.toBe (expect (get-in body [:attempt :status])) 200)
      (.toBe (expect (.-length calls)) 2)
      (.toBe (expect (:mode (first (:recent_attempts (js-await (show "s")))))) "manual"))
    (let [body (js-await (body-of (js-await (call "POST" "/jobs/s/pause" nil))))]
      (.toBe (expect (:state body)) "paused"))
    (let [paused (js-await (show "s"))]
      (js-await (run-minute! (+ (js/Date.parse (:next_run paused)) 60000)))
      (.toBe (expect (.-length calls)) 2)
      (.toBe (expect (:next_run (js-await (show "s")))) (:next_run paused)))
    (let [body (js-await (body-of (js-await (call "POST" "/jobs/s/resume" nil))))]
      (.toBe (expect (:state body)) "active")
      (.toBe (expect (> (js/Date.parse (:next_run body)) (js/Date.now))) true))))

(test "more than an hour overdue is skipped; run fires now; pause stops the minute" skip-run-pause)

(defn ^:async many-at-once []
  (let [ids (mapv #(str "m" %) (range 15))]
    (doseq [id ids]
      (js-await (put-job id {:cron "* * * * *" :url (str "https://target.test/" id)})))
    (let [due (js/Date.parse (:next_run (js-await (show "m0"))))]
      (js-await (run-minute! due))
      (.toBe (expect (.-length calls)) 15)
      (.toEqual (expect (sort (mapv #(.replace (:url %) "https://target.test/" "") (vec calls))))
                (clj->js (sort ids))))))

(test "many due jobs are all fired in one minute" many-at-once)
