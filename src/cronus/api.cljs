(ns cronus.api
  "The management API, behind one bearer secret (`CRONUS_SECRET`):

      PUT    /jobs/<id>          {cron tz url method headers body timeout_ms attempts}
      GET    /jobs/<id>          the job, its next run and its last attempts
      GET    /jobs?prefix=&limit=&after=
      DELETE /jobs/<id>
      POST   /jobs/<id>/run      one firing now, through the same path as a scheduled one
      POST   /jobs/<id>/pause
      POST   /jobs/<id>/resume
      GET    /health             no secret needed

  Ids are opaque strings, URL-encoded in the path. JSON in, JSON out."
  (:require [clojure.string :as str]
            [cronus.cron :as cron]
            [cronus.deliver :as deliver]
            [cronus.store :as store]))

(def version "0.1.0")

(defn- json [status data]
  (js/Response. (js/JSON.stringify (clj->js data))
                {:status status :headers {"content-type" "application/json"}}))

(defn- error [status message]
  (json status {:error message}))

(defn same-secret?
  "Constant-time comparison of two strings."
  [a b]
  (let [enc (js/TextEncoder.)
        x (.encode enc (str a))
        y (.encode enc (str b))]
    (and (= (.-length x) (.-length y))
         (loop [i 0 diff 0]
           (if (< i (.-length x))
             (recur (inc i) (bit-or diff (bit-xor (aget x i) (aget y i))))
             (= diff 0))))))

(defn- authorized? [req env]
  (let [secret (.-CRONUS_SECRET env)
        header (or (.get (.-headers req) "authorization") "")
        token (str/replace header #"^Bearer\s+" "")]
    (and (string? secret) (seq secret) (same-secret? token secret))))

(def ^:private methods #{"GET" "POST" "PUT" "PATCH" "DELETE"})

(defn- iso [millis]
  (when (some? millis) (.toISOString (js/Date. millis))))

(defn- present
  "A job row as the API shows it: times as ISO strings, headers parsed."
  [row attempts]
  {:id (:id row)
   :cron (:cron row)
   :tz (:tz row)
   :url (:url row)
   :method (:method row)
   :headers (js/JSON.parse (or (:headers_json row) "{}"))
   :body (:body row)
   :timeout_ms (:timeout_ms row)
   :attempts (:attempts row)
   :state (:state row)
   :next_run (iso (:next_run row))
   :pending (when (some? (:pending_scheduled row))
              {:scheduled (iso (:pending_scheduled row)) :attempts_done (:pending_attempt row)})
   :last_run (iso (:last_run row))
   :last_status (:last_status row)
   :failures (:failures row)
   :created (iso (:created row))
   :updated (iso (:updated row))
   :recent_attempts (when attempts
                      (mapv (fn [a] {:scheduled (iso (:scheduled a)) :attempt (:attempt a) :mode (:mode a)
                                    :at (iso (:at a)) :status (:status a) :duration_ms (:duration_ms a)
                                    :answer (:answer a) :ok (= 1 (:ok a))})
                           attempts))})

(defn validate
  "The job definition from the request body, or `{:error message}`."
  [id body now]
  (let [cron-text (:cron body)
        tz (or (:tz body) "UTC")
        url (:url body)
        method (str/upper-case (or (:method body) "POST"))
        headers (or (:headers body) {})
        job-body (:body body)
        timeout (or (:timeout_ms body) 30000)
        attempts (or (:attempts body) 3)]
    (cond
      (not (string? cron-text)) {:error "cron is required, five fields like \"0 9 * * 1-5\"."}
      (not (cron/valid-zone? tz)) {:error (str "tz \"" tz "\" is not a known time zone; use an IANA name like Europe/Berlin.")}
      (not (and (string? url) (re-find #"^https?://" url))) {:error "url must start with http:// or https://."}
      (not (contains? methods method)) {:error "method must be GET, POST, PUT, PATCH or DELETE."}
      (not (and (object? headers) (every? string? (vals headers)))) {:error "headers must be an object of strings."}
      (not (or (nil? job-body) (string? job-body) (object? job-body))) {:error "body must be a string, an object or absent."}
      (not (and (number? timeout) (>= timeout 1000) (<= timeout 300000))) {:error "timeout_ms must be between 1000 and 300000."}
      (not (and (number? attempts) (>= attempts 1) (<= attempts 10))) {:error "attempts must be between 1 and 10."}
      :else
      (let [parsed (try (cron/parse cron-text) (catch :default e {:error (.-message e)}))]
        (if (:error parsed)
          parsed
          (let [next-run (cron/next-run parsed tz now)]
            (if (nil? next-run)
              {:error "cron never matches within a year."}
              {:job {:id id :cron (str/trim cron-text) :tz tz :url url :method method
                     :headers_json (js/JSON.stringify headers)
                     :body (cond (nil? job-body) nil (string? job-body) job-body :else (js/JSON.stringify job-body))
                     :timeout_ms timeout :attempts attempts :next_run next-run :now now}})))))))

(defn- job-id [segment]
  (let [id (try (js/decodeURIComponent segment) (catch :default _ nil))]
    (when (and (string? id) (seq id) (not (str/includes? id "/")) (<= (count id) 512))
      id)))

(defn ^:async handle
  "The fetch handler of the Worker."
  [req env]
  (let [db (.-DB env)
        url (js/URL. (.-url req))
        method (.-method req)
        path (.-pathname url)
        parts (remove str/blank? (str/split path "/"))
        now (js/Date.now)]
    (cond
      (and (= method "GET") (= path "/health"))
      (json 200 {:ok true :name "cronus" :version version})

      (not (authorized? req env))
      (error 401 "A bearer token equal to CRONUS_SECRET is required.")

      (not= (first parts) "jobs")
      (error 404 "Unknown route; the API lives under /jobs.")

      :else
      (do
        (js-await (store/ensure! db))
        (let [[_ segment action] parts
              id (when segment (job-id segment))]
          (cond
            ;; GET /jobs?prefix=
            (and (nil? segment) (= method "GET"))
            (let [prefix (or (.get (.-searchParams url) "prefix") "")
                  limit (min 1000 (max 1 (or (js/parseInt (or (.get (.-searchParams url) "limit") "100")) 100)))
                  after (.get (.-searchParams url) "after")
                  rows (js-await (store/list-jobs db prefix limit after))]
              (json 200 {:jobs (mapv #(present % nil) rows)
                         :next_after (when (= (count rows) limit) (:id (last rows)))}))

            (nil? id)
            (error 400 "The job id is missing or not usable in a path.")

            (and (nil? action) (= method "PUT"))
            (let [body (try (js-await (.json req)) (catch :default _ nil))]
              (if (not (object? body))
                (error 400 "The body must be a JSON object.")
                (let [{:keys [error job]} (validate id body now)]
                  (if error
                    (json 400 {:error error})
                    (do (js-await (store/put-job! db job))
                        (json 200 (present (js-await (store/get-job db id)) nil)))))))

            (and (nil? action) (= method "GET"))
            (let [row (js-await (store/get-job db id))]
              (if (nil? row)
                (error 404 "No such job.")
                (json 200 (present row (js-await (store/attempts-of db id))))))

            (and (nil? action) (= method "DELETE"))
            (if (js-await (store/delete-job! db id))
              (json 200 {:deleted id})
              (error 404 "No such job."))

            (and (= method "POST") (contains? #{"run" "pause" "resume"} action))
            (let [row (js-await (store/get-job db id))]
              (cond
                (nil? row) (error 404 "No such job.")

                (= action "run")
                (let [result (js-await (deliver/fire! db row now "manual"))]
                  (json 200 {:job id :attempt (dissoc result :verdict) :ok (boolean (:ok? result))}))

                (= action "pause")
                (do (js-await (store/update-job! db id {"state" "paused" "pending_scheduled" nil "pending_attempt" 0} now))
                    (json 200 (present (js-await (store/get-job db id)) nil)))

                :else
                (do (js-await (store/update-job! db id {"state" "active" "next_run" (cron/next-run-of (:cron row) (:tz row) now)} now))
                    (json 200 (present (js-await (store/get-job db id)) nil)))))

            :else
            (error 405 "Method not allowed for this route.")))))))
