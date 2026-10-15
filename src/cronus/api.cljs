(ns cronus.api
  "The management API, behind one bearer secret (`CRONUS_SECRET`):

      PUT    /jobs/<id>          {cron tz url method headers body timeout_ms attempts}
      GET    /jobs/<id>          the job, its next run and its last attempts
      GET    /jobs?prefix=&limit=&after=
      DELETE /jobs/<id>
      GET    /health             no secret needed

  Ids are opaque strings, URL-encoded in the path. JSON in, JSON out.
  A paused job is a deleted one; a manual run is a call to the job's
  URL by whoever holds its headers."
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

(def ^:private methods #{"GET" "POST" "PUT" "PATCH" "DELETE"})

(def body-limit "Bytes of a job's body." 65536)
(def headers-limit "Bytes of a job's headers as JSON." 4096)
(def url-limit 2048)

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

(defn- byte-length [text]
  (.-byteLength (.encode (js/TextEncoder.) (str text))))

(defn- usable-url?
  "A parseable http(s) URL."
  [url]
  (and (string? url)
       (<= (.-length url) url-limit)
       (try (contains? #{"http:" "https:"} (.-protocol (js/URL. url)))
            (catch :default _ false))))

(defn- usable-headers?
  "An object of strings the runtime accepts as header names and values."
  [headers]
  (and (object? headers)
       (every? string? (vals headers))
       (try (js/Headers. headers) true
            (catch :default _ false))))

(defn- integer-between? [n lo hi]
  (and (number? n) (js/Number.isInteger n) (>= n lo) (<= n hi)))

(defn validate
  "The job definition from the request body as `{:job ...}`, or
  `{:error message}` with an optional `:status` (413 for a size)."
  [id body now]
  (let [cron-text (:cron body)
        tz (or (:tz body) "UTC")
        url (:url body)
        method (str/upper-case (or (:method body) "POST"))
        headers (or (:headers body) {})
        job-body (:body body)
        timeout (or (:timeout_ms body) 30000)
        attempts (or (:attempts body) 3)
        body-text (cond (nil? job-body) nil (string? job-body) job-body :else (js/JSON.stringify job-body))]
    (cond
      (not (string? cron-text)) {:error "cron is required, five fields like \"0 9 * * 1-5\"."}
      (not (cron/five-fields? cron-text)) {:error "cron has five fields, minute hour day month weekday, or a shortcut like @daily."}
      (not (string? tz)) {:error "tz must be an IANA time zone name like Europe/Berlin."}
      (not (cron/valid-zone? tz)) {:error (str "tz \"" tz "\" is not a known time zone; use an IANA name like Europe/Berlin.")}
      (not (usable-url? url)) {:error "url must be a valid http:// or https:// URL."}
      (not (contains? methods method)) {:error "method must be GET, POST, PUT, PATCH or DELETE."}
      (not (usable-headers? headers)) {:error "headers must be an object of strings with usable header names and values."}
      (> (byte-length (js/JSON.stringify headers)) headers-limit) {:error "headers are limited to 4 KB." :status 413}
      (not (or (nil? job-body) (string? job-body) (object? job-body))) {:error "body must be a string, an object or absent."}
      (and (some? body-text) (> (byte-length body-text) body-limit)) {:error "body is limited to 64 KB." :status 413}
      (not (integer-between? timeout 1000 300000)) {:error "timeout_ms must be a whole number between 1000 and 300000."}
      (not (integer-between? attempts 1 10)) {:error "attempts must be a whole number between 1 and 10."}
      :else
      (let [parsed (try {:pattern (cron/parse cron-text tz)} (catch :default e {:error (.-message e)}))]
        (if (:error parsed)
          parsed
          (let [next-run (cron/next-run (:pattern parsed) now)]
            (if (nil? next-run)
              {:error "cron never matches."}
              {:job {:id id :cron (str/trim cron-text) :tz tz :url url :method method
                     :headers_json (js/JSON.stringify headers)
                     :body body-text
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
          ;; GET /jobs?prefix=
          (and (nil? segment) (= method "GET"))
          (let [params (.-searchParams url)
                prefix (or (.get params "prefix") "")
                limit (limit-of params)
                after (.get params "after")
                rows (js-await (store/list-jobs db prefix limit after))]
            (json 200 {:jobs (mapv #(present % nil) rows)
                       :next_after (when (= (count rows) limit) (:id (last rows)))}))

          (nil? id)
          (error 400 "The job id is missing, longer than 200 characters, or contains a control character.")

          (some? action)
          (error 404 "Unknown route; a job has PUT, GET and DELETE only.")

          (= method "PUT")
          (let [body (try (js-await (.json req)) (catch :default _ nil))]
            (if (not (object? body))
              (error 400 "The body must be a JSON object.")
              (let [{:keys [error status job]} (validate id body now)]
                (if error
                  (json (or status 400) {:error error})
                  (do (js-await (store/put-job! db job))
                      (json 200 (present (js-await (store/get-job db id)) nil)))))))

          (= method "GET")
          (let [row (js-await (store/get-job db id))]
            (if (nil? row)
              (error 404 "No such job.")
              (json 200 (present row (js-await (store/attempts-of db id))))))

          (= method "DELETE")
          (if (js-await (store/delete-job! db id))
            (json 200 {:deleted id})
            (error 404 "No such job."))

          :else
          (error 405 "Method not allowed for this route."))))))
