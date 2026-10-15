(ns cronus.cron
  "Cron expressions and the next run in a time zone.

  Five fields, `minute hour day-of-month month day-of-week`, with `*`,
  lists, ranges, steps, month and weekday names, 0 and 7 as Sunday, and
  the shortcuts @hourly @daily @weekly @monthly. Day-of-month and
  day-of-week combine as in Vixie cron: when both are restricted, a
  time matches when either does.

  The next run is found by walking forward through the minutes of the
  job's IANA zone, so \"9:00 Europe/Berlin\" stays 9:00 across a clock
  change. The walk skips whole hours and most of a day when they cannot
  match, and never goes further than 366 days.")

(def month-names
  {"jan" 1 "feb" 2 "mar" 3 "apr" 4 "may" 5 "jun" 6
   "jul" 7 "aug" 8 "sep" 9 "oct" 10 "nov" 11 "dec" 12})

(def weekday-names
  {"sun" 0 "mon" 1 "tue" 2 "wed" 3 "thu" 4 "fri" 5 "sat" 6})

(def shortcuts
  {"@hourly" "0 * * * *"
   "@daily" "0 0 * * *"
   "@midnight" "0 0 * * *"
   "@weekly" "0 0 * * 0"
   "@monthly" "0 0 1 * *"
   "@yearly" "0 0 1 1 *"
   "@annually" "0 0 1 1 *"})

(defn- fail [message]
  (throw (js/Error. message)))

(defn- parse-number [token field names lo hi]
  (let [lower (.toLowerCase (str token))
        named (get names lower)
        n (if (some? named) named (js/Number token))]
    (when (or (js/Number.isNaN n) (not (js/Number.isInteger n)))
      (fail (str "The " field " field has no number in \"" token "\".")))
    (when (or (< n lo) (> n hi))
      (fail (str "The " field " field allows " lo " to " hi ", not " token ".")))
    n))

(defn- expand-part
  "One comma-separated part of a field into a set of numbers."
  [part field names lo hi]
  (let [[range-part step-part] (.split part "/")
        step (if (some? step-part)
               (let [s (js/Number step-part)]
                 (when (or (js/Number.isNaN s) (<= s 0) (not (js/Number.isInteger s)))
                   (fail (str "The step in \"" part "\" of the " field " field must be a positive number.")))
                 s)
               1)
        [start end] (cond
                      (= range-part "*") [lo hi]
                      (.includes range-part "-")
                      (let [[a b] (.split range-part "-")]
                        [(parse-number a field names lo hi) (parse-number b field names lo hi)])
                      :else
                      (let [n (parse-number range-part field names lo hi)]
                        [n (if (some? step-part) hi n)]))]
    (when (> start end)
      (fail (str "The range \"" range-part "\" of the " field " field runs backwards.")))
    (let [out #js []]
      (loop [n start]
        (when (<= n end)
          (.push out n)
          (recur (+ n step))))
      out)))

(defn- expand-field [text field names lo hi]
  (let [values (js/Set.)]
    (doseq [part (.split text ",")]
      (when (= part "")
        (fail (str "The " field " field has an empty entry.")))
      (doseq [n (expand-part part field names lo hi)]
        (.add values n)))
    values))

(defn parse
  "The expression as `{:minutes :hours :doms :months :dows :dom-star? :dow-star?}`
  (JS Sets), or throws an Error whose message says what is wrong."
  [expression]
  (let [text (.trim (str expression))
        text (or (get shortcuts (.toLowerCase text)) text)
        fields (.split text #"\s+")]
    (when (not= (count fields) 5)
      (fail (str "A cron expression has five fields, minute hour day month weekday; \"" expression "\" has " (count fields) ".")))
    (let [[minute hour dom month dow] fields
          dows (expand-field dow "weekday" weekday-names 0 7)]
      ;; 7 is Sunday as well
      (when (.has dows 7)
        (.delete dows 7)
        (.add dows 0))
      {:minutes (expand-field minute "minute" {} 0 59)
       :hours (expand-field hour "hour" {} 0 23)
       :doms (expand-field dom "day" {} 1 31)
       :months (expand-field month "month" month-names 1 12)
       :dows dows
       :dom-star? (= dom "*")
       :dow-star? (= dow "*")})))

(def ^:private formatters (js/Map.))

(defn- formatter [tz]
  (or (.get formatters tz)
      (let [f (js/Intl.DateTimeFormat. "en-US"
                                       {:timeZone tz :hourCycle "h23"
                                        :year "numeric" :month "numeric" :day "numeric"
                                        :hour "numeric" :minute "numeric" :weekday "short"})]
        (.set formatters tz f)
        f)))

(defn valid-zone?
  "Whether the IANA name is known to this runtime."
  [tz]
  (try (formatter tz) true
       (catch :default _ false)))

(defn local-parts
  "The wall clock of `millis` in `tz`: `{:year :month :day :hour :minute :weekday}`,
  weekday 0 = Sunday."
  [millis tz]
  (let [parts (.formatToParts (formatter tz) (js/Date. millis))
        out {}]
    (doseq [p parts]
      (let [t (.-type p) v (.-value p)]
        (case t
          "year" (assoc! out :year (js/Number v))
          "month" (assoc! out :month (js/Number v))
          "day" (assoc! out :day (js/Number v))
          "hour" (assoc! out :hour (mod (js/Number v) 24))
          "minute" (assoc! out :minute (js/Number v))
          "weekday" (assoc! out :weekday (get weekday-names (.toLowerCase v)))
          nil)))
    out))

(def ^:private minute-ms 60000)
(def ^:private max-days 366)

(defn- day-matches? [{:keys [doms dows dom-star? dow-star?]} {:keys [day weekday]}]
  (cond
    (and dom-star? dow-star?) true
    dom-star? (.has dows weekday)
    dow-star? (.has doms day)
    :else (or (.has doms day) (.has dows weekday))))

(defn- next-midnight
  "The next local midnight after `t` (at least one minute later). The
  jump of a whole day lands an hour early or late around a clock
  change; one more look corrects it."
  [t parts tz]
  (let [t2 (+ t (* minute-ms (- (* 24 60) (+ (* (:hour parts) 60) (:minute parts)))))
        p2 (local-parts t2 tz)
        hour (:hour p2)
        minute (:minute p2)
        t3 (cond
             (and (= hour 0) (= minute 0)) t2
             (= hour 23) (+ t2 (* minute-ms (- 60 minute)))
             :else (- t2 (* minute-ms (+ (* hour 60) minute))))]
    (if (> t3 t) t3 (+ t minute-ms))))

(defn next-run
  "The first time after `after-millis` (exclusive) that matches, as epoch
  millis, or nil when nothing matches within 366 days."
  [parsed tz after-millis]
  (let [start (* minute-ms (inc (js/Math.floor (/ after-millis minute-ms))))
        limit (+ start (* max-days 24 60 minute-ms))
        {:keys [minutes hours months]} parsed]
    (loop [t start]
      (if (< t limit)
        (let [{:keys [month hour minute] :as parts} (local-parts t tz)]
          (cond
            (not (.has months month)) (recur (next-midnight t parts tz))
            (not (day-matches? parsed parts)) (recur (next-midnight t parts tz))
            (not (.has hours hour)) (recur (+ t (* minute-ms (- 60 minute))))
            (not (.has minutes minute)) (recur (+ t minute-ms))
            :else t))
        nil))))

(defn next-run-of
  "`next-run` from the expression text; throws on a bad expression."
  [expression tz after-millis]
  (next-run (parse expression) (or tz "UTC") after-millis))
