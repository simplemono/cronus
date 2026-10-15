(ns cronus.api
  "The management API, behind one bearer secret (`CRONUS_SECRET`):

      PUT    /jobs/<id>          {cron tz url timeout_ms attempts}
      GET    /jobs/<id>          the job, its next run and its last attempt
      GET    /jobs?prefix=&limit=&after=
      DELETE /jobs/<id>
      GET    /health             no secret needed

  Ids are opaque strings, URL-encoded in the path. JSON in, JSON out.
  Every firing is a POST to the job's URL with a JSON body; the URL is
  the credential. A paused job is a deleted one; a manual run is a call
  to the URL by whoever knows it."
  (:require [clojure.string :as str]
            [cronus.cron :as cron]
            [cronus.store :as store]))

(def version "0.2.0")

(defn- json [status data]
  (js/Response. (js/JSON.stringify (clj->js data))
                {:status status :headers {"content-type" "application/json"}}))

(defn- error [status message]
  (json status {:error message}))

(defn ^:async same-secret?
  "Whether two strings are equal, in constant time: both are hashed and
  the digests compared byte by byte, so neither the length nor the
  position of a difference shows in the timing."
  [a b]
  (let [enc (js/TextEncoder.)
        x (js/Uint8Array. (js-await (js/crypto.subtle.digest "SHA-256" (.encode enc (str a)))))
        y (js/Uint8Array. (js-await (js/crypto.subtle.digest "SHA-256" (.encode enc (str b)))))]
    (loop [i 0 diff 0]
      (if (< i 32)
        (recur (inc i) (bit-or diff (bit-xor (aget x i) (aget y i))))
        (= diff 0)))))

(defn ^:async authorized?
  "`Authorization: Bearer <CRONUS_SECRET>`; nothing passes while the
  secret is unset."
  [req env]
  (let [secret (.-CRONUS_SECRET env)
        header (or (.get (.-headers req) "authorization") "")]
    (if (and (string? secret) (pos? (.-length secret)) (.startsWith header "Bearer "))
      (js-await (same-secret? (.slice header 7) secret))
      false)))

(def url-limit 2048)

(defn- or-null
  "The value, or null (not undefined, which JSON would drop)."
  [v]
  (if (some? v) v nil))

(defn- iso
  "An ISO string, or null."
  [millis]
  (if (some? millis) (.toISOString (js/Date. millis)) nil))

(defn- present
  "A job row as the API shows it: times as ISO strings or null, the last
  attempt as `last_*`."
  [row]
  {:id (:id row)
   :cron (:cron row)
   :tz (:tz row)
   :url (:url row)
   :timeout_ms (:timeout_ms row)
   :attempts (:attempts row)
   :next_run (iso (:next_run row))
   :pending (if (some? (:pending_scheduled row))
              {:scheduled (iso (:pending_scheduled row)) :attempt (:pending_attempt row)}
              nil)
   :last_run (iso (:last_run row))
   :last_status (or-null (:last_status row))
   :last_http_status (or-null (:last_http_status row))
   :last_duration_ms (or-null (:last_duration_ms row))
   :last_answer (or-null (:last_answer row))
   :failures (:failures row)
   :created (iso (:created row))
   :updated (iso (:updated row))})

(defn- usable-url?
  "A parseable http(s) URL."
  [url]
  (and (string? url)
       (<= (.-length url) url-limit)
       (try (contains? #{"http:" "https:"} (.-protocol (js/URL. url)))
            (catch :default _ false))))

(defn- integer-between? [n lo hi]
  (and (number? n) (js/Number.isInteger n) (>= n lo) (<= n hi)))

(defn validate
  "The job definition from the request body as `{:job ...}`, or
  `{:error message}`."
  [id body now]
  (let [cron-text (:cron body)
        tz (or (:tz body) "UTC")
        url (:url body)
        timeout (or (:timeout_ms body) 30000)
        attempts (or (:attempts body) 3)]
    (cond
      (not (string? cron-text)) {:error "cron is required, five fields like \"0 9 * * 1-5\"."}
      (not (cron/five-fields? cron-text)) {:error "cron has five fields, minute hour day month weekday, or a shortcut like @daily."}
      (not (string? tz)) {:error "tz must be an IANA time zone name like Europe/Berlin."}
      (not (cron/valid-zone? tz)) {:error (str "tz \"" tz "\" is not a known time zone; use an IANA name like Europe/Berlin.")}
      (not (usable-url? url)) {:error "url must be a valid http:// or https:// URL."}
      (not (integer-between? timeout 1000 300000)) {:error "timeout_ms must be a whole number between 1000 and 300000."}
      (not (integer-between? attempts 1 10)) {:error "attempts must be a whole number between 1 and 10."}
      :else
      (let [parsed (try {:pattern (cron/parse cron-text tz)} (catch :default e {:error (.-message e)}))]
        (if (:error parsed)
          parsed
          (let [next-run (cron/next-run (:pattern parsed) now)]
            (if (nil? next-run)
              {:error "cron never matches."}
              {:job {:id id :cron (str/trim cron-text) :tz tz :url url
                     :timeout_ms timeout :attempts attempts :next_run next-run :now now}})))))))

(def ^:private id-pattern
  "1 to 200 characters, none of them a control character or a slash."
  (js/RegExp. "^[^\\x00-\\x1f\\x7f/]{1,200}$" "u"))

(defn- job-id [segment]
  (let [id (try (js/decodeURIComponent segment) (catch :default _ nil))]
    (when (and (string? id) (.test id-pattern id))
      id)))

(defn- limit-of [params]
  (let [n (js/parseInt (or (.get params "limit") "") 10)]
    (if (js/Number.isFinite n) (min 1000 (max 1 n)) 100)))

(defn ^:async handle
  "The fetch handler of the Worker."
  [req env]
  (let [db (.-DB env)
        url (js/URL. (.-url req))
        method (.-method req)
        path (.-pathname url)
        parts (remove str/blank? (str/split path "/"))
        now (js/Date.now)
        authorized (js-await (authorized? req env))]
    (cond
      (and (= method "GET") (= path "/health"))
      (json 200 {:ok true :name "cronus" :version version})

      (not authorized)
      (error 401 "A bearer token equal to CRONUS_SECRET is required.")

      (not= (first parts) "jobs")
      (error 404 "Unknown route; the API lives under /jobs.")

      :else
      (let [[_ segment action] parts
            id (when segment (job-id segment))]
        (cond
          (and (nil? segment) (not= method "GET"))
          (error 405 "The collection answers GET only; a job is created with PUT /jobs/<id>.")

          ;; GET /jobs?prefix=
          (nil? segment)
          (let [params (.-searchParams url)
                prefix (or (.get params "prefix") "")
                limit (limit-of params)
                after (.get params "after")
                rows (js-await (store/list-jobs db prefix limit after))]
            (json 200 {:jobs (mapv present rows)
                       :next_after (when (= (count rows) limit) (:id (last rows)))}))

          (nil? id)
          (error 400 "The job id is missing, longer than 200 characters, or contains a control character.")

          (some? action)
          (error 404 "Unknown route; a job has PUT, GET and DELETE only.")

          (= method "PUT")
          (let [body (try (js-await (.json req)) (catch :default _ nil))]
            (if (not (object? body))
              (error 400 "The body must be a JSON object.")
              (let [{:keys [error job]} (validate id body now)]
                (if error
                  (json 400 {:error error})
                  (do (js-await (store/put-job! db job))
                      (json 200 (present (js-await (store/get-job db id)))))))))

          (= method "GET")
          (let [row (js-await (store/get-job db id))]
            (if (nil? row)
              (error 404 "No such job.")
              (json 200 (present row))))

          (= method "DELETE")
          (if (js-await (store/delete-job! db id))
            (json 200 {:deleted id})
            (error 404 "No such job."))

          :else
          (error 405 "Method not allowed for this route."))))))
