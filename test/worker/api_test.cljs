(ns worker.api-test
  "The API and the minute inside workerd: the migrations applied to the
  local D1 and the tables emptied per test, the targets answered by a
  stub of the global fetch."
  (:require ["cloudflare:test" :refer [env SELF applyD1Migrations createScheduledController createExecutionContext waitOnExecutionContext]]
            ["vitest" :refer [test expect beforeEach afterEach vi]]
            ["../cronus/worker.mjs" :as worker]
            [cronus.store :as store]))

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

(defn ^:async fresh-tables!
  "The migrations applied (once; they are tracked) and the tables
  emptied; the plugin keeps the D1 state across the tests of a file."
  []
  (let [db (.-DB env)]
    (js-await (applyD1Migrations db (.-TEST_MIGRATIONS env)))
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

(defn run-minute
  "The scheduled handler at `at-millis`; a promise, so that two minutes
  can overlap."
  [at-millis]
  (let [controller (createScheduledController {:scheduledTime (js/Date. at-millis) :cron "* * * * *"})
        ctx (createExecutionContext)]
    (.then ((.-scheduled (.-default worker)) controller env ctx)
           (fn [] (waitOnExecutionContext ctx)))))

(defn ^:async run-minute! [at-millis]
  (js-await (run-minute at-millis)))

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
  (let [[status job] (js-await (put-job "org1_agentA_job1" {:cron "0 9 * * 1-5" :tz "Europe/Berlin" :url "https://target.test/run" :headers {"X-Job-Token" "t1"}}))]
    (.toBe (expect status) 200)
    (.toBe (expect (:method job)) "POST")
    (.toBe (expect (:tz job)) "Europe/Berlin")
    (.toMatch (expect (:next_run job)) #"T0[78]:00:00.000Z$"))
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
  ;; a limit that is not a number falls back to the default
  (let [r (js-await (call "GET" "/jobs?limit=abc" nil))]
    (.toBe (expect (.-status r)) 200)
    (.toBe (expect (count (:jobs (js-await (body-of r))))) 4))
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
    (.toBe (expect (.-status r)) 404))
  ;; no other verbs on a job
  (let [r (js-await (call "POST" "/jobs/org1_agentB_job2/run" nil))]
    (.toBe (expect (.-status r)) 404))
  (let [r (js-await (call "POST" "/jobs/org1_agentB_job2" nil))]
    (.toBe (expect (.-status r)) 405)))

(test "put, get, list by prefix, replace, delete" put-get-list-delete)

(defn ^:async refused [id job pattern]
  (let [[status body] (js-await (put-job id job))]
    (.toBe (expect status) (if (= pattern 413) 413 400))
    (when (not= pattern 413)
      (.toMatch (expect (:error body)) pattern))))

(defn ^:async bad-definitions []
  (js-await (refused "x" {:cron "0 9 * *" :url "https://t.test/"} #"five"))
  (js-await (refused "x" {:cron "0 0 9 * * *" :url "https://t.test/"} #"five fields"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :tz "Mars/Olympus"} #"time zone"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "ftp://t.test/"} #"http"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://"} #"valid"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :attempts 50} #"attempts"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :attempts 2.5} #"whole number"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :timeout_ms 1500.5} #"whole number"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :headers {"X Bad Name" "v"}} #"header"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :headers {"X-Ok" "a\r\nb"}} #"header"))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :headers {"X-Big" (.repeat "x" 5000)}} 413))
  (js-await (refused "x" {:cron "0 9 * * *" :url "https://t.test/" :body (.repeat "x" 70000)} 413))
  (js-await (refused "x" {:cron "0 0 30 2 *" :url "https://t.test/"} #"never"))
  (let [r (js-await (call "PUT" "/jobs/bad%00id" {:cron "0 9 * * *" :url "https://t.test/"}))]
    (.toBe (expect (.-status r)) 400))
  (let [r (js-await (call "PUT" (str "/jobs/" (.repeat "a" 201)) {:cron "0 9 * * *" :url "https://t.test/"}))]
    (.toBe (expect (.-status r)) 400))
  (let [r (js-await (call "GET" "/jobs/x" nil))]
    (.toBe (expect (.-status r)) 404)))

(test "a bad definition is refused with the reason, a big one with 413" bad-definitions)

(defn ^:async minute-fires []
  (let [[_ job] (js-await (put-job "j" {:cron "* * * * *" :url "https://target.test/run" :headers {"X-Job-Token" "secret-1"} :body {:hello "world"}}))
        [_ _] (js-await (put-job "later" {:cron "0 0 1 1 *" :url "https://target.test/never"}))
        due (js/Date.parse (:next_run job))]
    (js-await (run-minute! (- due 60000)))
    (.toBe (expect (.-length calls)) 0)
    (js-await (run-minute! due))
    (.toBe (expect (.-length calls)) 1)
    (let [{:keys [url method headers body redirect]} (aget calls 0)]
      (.toBe (expect url) "https://target.test/run")
      (.toBe (expect method) "POST")
      (.toBe (expect redirect) "manual")
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
        (.toBe (expect (js/Date.parse (:next_run after))) (+ due2 60000))))
    ;; a redirect is not followed and fails the firing at once
    (let [[_ job3] (js-await (put-job "moved" {:cron "* * * * *" :url "https://target.test/old"}))
          due3 (js/Date.parse (:next_run job3))]
      (.push replies {:status 302 :body ""})
      (js-await (run-minute! due3))
      (let [after (js-await (show "moved"))]
        (.toBe (expect (:last_status after)) "failed")
        (.toBe (expect (:pending after)) nil)
        (.toBe (expect (:status (first (:recent_attempts after)))) 302)
        (.toBe (expect (.-length (.filter calls (fn [c] (= (:url c) "https://target.test/old"))))) 1)))))

(test "after the last attempt the firing fails; a 4xx or a 3xx fails at once" gives-up)

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

(defn ^:async overdue-is-skipped []
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
      (.toBe (expect (:x-cronus-scheduled (:headers (aget calls 0)))) (:next_run after)))))

(test "more than an hour overdue is skipped and logged; less is fired" overdue-is-skipped)

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

(defn ^:async overlapping-minutes []
  (let [[_ job] (js-await (put-job "o" {:cron "* * * * *" :url "https://target.test/slowish"}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 200 :body "ok" :delay 400})
    ;; the next minute starts while the first one is still waiting for the target
    (let [first-minute (run-minute due)]
      (js-await (pause 100))
      (js-await (run-minute! (+ due 60000)))
      (js-await first-minute))
    (.toBe (expect (.-length calls)) 1)
    (let [after (js-await (show "o"))]
      (.toBe (expect (:last_status after)) "ok")
      (.toBe (expect (count (:recent_attempts after))) 1)
      ;; the lease is released and the job runs again at its next minute
      (js-await (run-minute! (+ due 120000)))
      (.toBe (expect (.-length calls)) 2))))

(test "a minute that overlaps the previous one does not fire the same job again" overlapping-minutes)

(defn ^:async deleted-during-a-call []
  (let [[_ job] (js-await (put-job "d" {:cron "* * * * *" :url "https://target.test/d"}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 200 :body "ok"
                    :before (fn [] (store/delete-job! (.-DB env) "d"))})
    (js-await (run-minute! due))
    (.toBe (expect (.-length calls)) 1)
    (let [r (js-await (call "GET" "/jobs/d" nil))]
      (.toBe (expect (.-status r)) 404))
    (let [rows (js-await (.all (.prepare (.-DB env) "SELECT count(*) AS n FROM attempts")))]
      (.toBe (expect (:n (aget (.-results rows) 0))) 0))))

(test "a job deleted during its call leaves nothing behind" deleted-during-a-call)

(defn ^:async replaced-during-a-call []
  (let [[_ job] (js-await (put-job "e" {:cron "* * * * *" :url "https://target.test/e"}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 503 :body "down"
                    :before (fn [] (call "PUT" "/jobs/e" {:cron "0 12 * * *" :url "https://target.test/e2"}))})
    (js-await (run-minute! due))
    (let [after (js-await (show "e"))]
      ;; the edit won: no retry of the old definition, the new schedule stands
      (.toBe (expect (:cron after)) "0 12 * * *")
      (.toBe (expect (:pending after)) nil)
      (.toMatch (expect (:next_run after)) #"T12:00:00.000Z$")
      ;; the attempt itself is on record
      (.toBe (expect (count (:recent_attempts after))) 1))))

(test "a job replaced during its call keeps the new definition, not the stale outcome" replaced-during-a-call)

(defn ^:async big-answer []
  (let [[_ job] (js-await (put-job "big" {:cron "* * * * *" :url "https://target.test/big"}))
        due (js/Date.parse (:next_run job))]
    (.push replies {:status 200 :body (.repeat "y" 300000)})
    (js-await (run-minute! due))
    (let [after (js-await (show "big"))
          attempt (first (:recent_attempts after))]
      (.toBe (expect (:last_status after)) "ok")
      (.toBe (expect (:status attempt)) 200)
      (.toBe (expect (.-length (:answer attempt))) 200))))

(test "a huge answer is read only in part and recorded clipped" big-answer)

(defn ^:async attempts-are-trimmed []
  (let [[_ job] (js-await (put-job "tr" {:cron "* * * * *" :url "https://target.test/tr"}))
        due (js/Date.parse (:next_run job))]
    (doseq [i (range 25)]
      (js-await (run-minute! (+ due (* i 60000)))))
    (.toBe (expect (.-length calls)) 25)
    (let [after (js-await (show "tr"))]
      (.toBe (expect (count (:recent_attempts after))) 20)
      (.toBe (expect (:scheduled (first (:recent_attempts after)))) (.toISOString (js/Date. (+ due (* 24 60000))))))
    (let [rows (js-await (.all (.prepare (.-DB env) "SELECT count(*) AS n FROM attempts")))]
      (.toBe (expect (:n (aget (.-results rows) 0))) 20))))

(test "a job keeps its last 20 attempts" attempts-are-trimmed)
