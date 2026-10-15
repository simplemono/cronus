(ns cronus.cron
  "Cron expressions and the next run in a time zone, computed by
  croner (https://github.com/Hexagon/croner).

  Five fields, `minute hour day-of-month month day-of-week`, with `*`,
  lists, ranges, steps, month and weekday names, 0 and 7 as Sunday, `L`
  and `W`, and the shortcuts @hourly @daily @weekly @monthly @yearly.
  Day-of-month and day-of-week combine as in Vixie cron: when both are
  restricted, a time matches when either does.

  Clock changes: \"9:00 Europe/Berlin\" stays 9:00 across them; a time
  inside the hour the spring change skips runs an hour later that day,
  a time inside the hour the autumn change repeats runs once."
  (:require ["croner" :refer [Cron]]))

(def ^:private known-zones (js/Set.))

(defn valid-zone?
  "Whether the IANA name is known to this runtime."
  [tz]
  (or (.has known-zones tz)
      (try (js/Intl.DateTimeFormat. "en-US" {:timeZone tz})
           (.add known-zones tz)
           true
           (catch :default _ false))))

(defn five-fields?
  "Whether the text is a five-field expression or a shortcut; croner
  would also take six (seconds) and seven (year), which cronus does not
  offer."
  [expression]
  (let [text (.trim (str expression))]
    (or (.startsWith text "@")
        (= 5 (.-length (.split text (js/RegExp. "\\s+")))))))

(defn parse
  "The expression as a croner pattern in `tz`; throws an Error whose
  message says what is wrong. Nothing is scheduled by this: the object
  only computes times."
  [expression tz]
  (Cron. (.trim (str expression)) {:timezone (or tz "UTC")}))

(defn next-run
  "The first time after `after-millis` (exclusive) that matches, as epoch
  millis, or nil when nothing matches; croner looks years ahead, so a
  29 February is found."
  [pattern after-millis]
  (let [d (.nextRun pattern (js/Date. after-millis))]
    (when (some? d) (.getTime d))))

(defn next-run-of
  "`next-run` from the expression text; throws on a bad expression."
  [expression tz after-millis]
  (next-run (parse expression tz) after-millis))
