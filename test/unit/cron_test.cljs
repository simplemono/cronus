(ns unit.cron-test
  (:require ["node:test" :as t]
            ["node:assert/strict" :as assert]
            [cronus.cron :as cron]))

(defn at [iso] (.getTime (js/Date. iso)))
(defn iso [millis] (.toISOString (js/Date. millis)))

(defn next-iso [expr tz after]
  (iso (cron/next-run-of expr tz (at after))))

(t/test "every minute, hourly, daily in UTC; the start is exclusive"
  (fn []
    (assert/equal (next-iso "* * * * *" "UTC" "2026-10-15T08:00:00Z") "2026-10-15T08:01:00.000Z")
    (assert/equal (next-iso "* * * * *" "UTC" "2026-10-15T08:00:30Z") "2026-10-15T08:01:00.000Z")
    (assert/equal (next-iso "@hourly" "UTC" "2026-10-15T08:00:00Z") "2026-10-15T09:00:00.000Z")
    (assert/equal (next-iso "0 9 * * *" "UTC" "2026-10-15T09:00:00Z") "2026-10-16T09:00:00.000Z")
    (assert/equal (next-iso "30 9 * * 1-5" "UTC" "2026-10-16T10:00:00Z") "2026-10-19T09:30:00.000Z")))

(t/test "Berlin keeps 9:00 across the clock change"
  (fn []
    ;; summer time until 2026-10-25 03:00 CEST
    (assert/equal (next-iso "0 9 * * *" "Europe/Berlin" "2026-10-24T08:00:00Z") "2026-10-25T08:00:00.000Z")
    ;; winter time: 9:00 Berlin is 08:00Z
    (assert/equal (next-iso "0 9 * * *" "Europe/Berlin" "2026-10-25T08:00:00Z") "2026-10-26T08:00:00.000Z")
    (assert/equal (next-iso "0 9 * * *" "Europe/Berlin" "2026-03-28T08:00:00Z") "2026-03-29T07:00:00.000Z")
    ;; a job in the hour after midnight on the day the clocks move
    (assert/equal (next-iso "30 0 * * *" "Europe/Berlin" "2026-03-28T23:40:00Z") "2026-03-29T22:30:00.000Z")
    (assert/equal (next-iso "30 0 * * *" "Europe/Berlin" "2026-03-27T23:40:00Z") "2026-03-28T23:30:00.000Z")
    ;; half-hour zone
    (assert/equal (next-iso "0 9 * * *" "Asia/Kolkata" "2026-10-15T00:00:00Z") "2026-10-15T03:30:00.000Z")))

(t/test "the hour a clock change skips runs an hour later; the hour it repeats runs once"
  (fn []
    ;; Berlin, 2026-03-29: 02:00 CET becomes 03:00 CEST; 02:30 runs at 03:30 CEST
    (assert/equal (next-iso "30 2 * * *" "Europe/Berlin" "2026-03-28T12:00:00Z") "2026-03-29T01:30:00.000Z")
    (assert/equal (next-iso "30 2 * * *" "Europe/Berlin" "2026-03-29T01:30:00Z") "2026-03-30T00:30:00.000Z")
    ;; Berlin, 2026-10-25: 02:30 CEST, then not 02:30 CET an hour later
    (assert/equal (next-iso "30 2 * * *" "Europe/Berlin" "2026-10-24T12:00:00Z") "2026-10-25T00:30:00.000Z")
    (assert/equal (next-iso "30 2 * * *" "Europe/Berlin" "2026-10-25T00:30:00Z") "2026-10-26T01:30:00.000Z")
    ;; New York, 2026-03-08 and 2026-11-01
    (assert/equal (next-iso "30 2 * * *" "America/New_York" "2026-03-07T12:00:00Z") "2026-03-08T07:30:00.000Z")
    (assert/equal (next-iso "30 1 * * *" "America/New_York" "2026-10-31T12:00:00Z") "2026-11-01T05:30:00.000Z")
    (assert/equal (next-iso "30 1 * * *" "America/New_York" "2026-11-01T05:30:00Z") "2026-11-02T06:30:00.000Z")
    ;; zones that change at midnight keep one @daily per day
    (assert/equal (next-iso "@daily" "America/Santiago" "2026-09-05T12:00:00Z") "2026-09-06T04:00:00.000Z")
    (assert/equal (next-iso "@daily" "America/Santiago" "2026-09-06T04:00:00Z") "2026-09-07T03:00:00.000Z")
    (assert/equal (next-iso "@daily" "America/Havana" "2026-10-31T12:00:00Z") "2026-11-01T04:00:00.000Z")
    (assert/equal (next-iso "@daily" "America/Havana" "2026-11-01T04:00:00Z") "2026-11-02T05:00:00.000Z")))

(t/test "Sunday as 0 and 7, names, steps, lists, ranges, day OR weekday"
  (fn []
    (assert/equal (next-iso "0 7 * * 7" "UTC" "2026-10-15T00:00:00Z") "2026-10-18T07:00:00.000Z")
    (assert/equal (next-iso "0 7 * * 0" "UTC" "2026-10-15T00:00:00Z") "2026-10-18T07:00:00.000Z")
    (assert/equal (next-iso "0 7 * * sun" "UTC" "2026-10-15T00:00:00Z") "2026-10-18T07:00:00.000Z")
    (assert/equal (next-iso "*/15 * * * *" "UTC" "2026-10-15T08:01:00Z") "2026-10-15T08:15:00.000Z")
    (assert/equal (next-iso "0 0 1 jan,jul *" "UTC" "2026-10-15T08:00:00Z") "2027-01-01T00:00:00.000Z")
    (assert/equal (next-iso "0 12 15 * mon" "UTC" "2026-10-15T13:00:00Z") "2026-10-19T12:00:00.000Z")
    (assert/equal (next-iso "0 12 15 * mon" "UTC" "2026-10-13T13:00:00Z") "2026-10-15T12:00:00.000Z")
    (assert/equal (next-iso "@weekly" "UTC" "2026-10-15T00:00:00Z") "2026-10-18T00:00:00.000Z")))

(t/test "errors say what is wrong"
  (fn []
    (.throws assert (fn [] (cron/parse "0 9 * *" "UTC")) #"five")
    (.throws assert (fn [] (cron/parse "61 9 * * *" "UTC")) #"minute")
    (.throws assert (fn [] (cron/parse "0 9 * * xyz" "UTC")) #"illegal characters")
    (.throws assert (fn [] (cron/parse "0 9 * * */0" "UTC")) #"stepping")
    (.throws assert (fn [] (cron/parse "0 9 5-2 * *" "UTC")) #"larger")
    (.throws assert (fn [] (cron/parse "- 9 * * *" "UTC")) #"Syntax error")
    (.throws assert (fn [] (cron/parse "0x1f 9 * * *" "UTC")) #"illegal characters")
    (assert/equal (cron/five-fields? "0 9 * * *") true)
    (assert/equal (cron/five-fields? "@daily") true)
    (assert/equal (cron/five-fields? "0 0 9 * * *") false)
    (assert/equal (cron/valid-zone? "Europe/Berlin") true)
    (assert/equal (cron/valid-zone? "Mars/Olympus") false)))

(t/test "Feb 29 is found four years ahead; 30 Feb never matches"
  (fn []
    (assert/equal (next-iso "0 0 29 2 *" "UTC" "2027-03-01T00:00:00Z") "2028-02-29T00:00:00.000Z")
    (assert/equal (next-iso "0 0 29 2 *" "UTC" "2028-03-01T00:00:00Z") "2032-02-29T00:00:00.000Z")
    (assert/equal (nil? (cron/next-run-of "0 0 30 2 *" "UTC" (at "2026-10-15T00:00:00Z"))) true)))
