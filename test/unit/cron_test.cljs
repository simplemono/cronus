(ns unit.cron-test
  (:require ["node:test" :as t]
            ["node:assert/strict" :as assert]
            [cronus.cron :as cron]))

(defn at [iso] (.getTime (js/Date. iso)))
(defn iso [millis] (.toISOString (js/Date. millis)))

(defn next-iso [expr tz after]
  (iso (cron/next-run-of expr tz (at after))))

(t/test "every minute, hourly, daily in UTC"
  (fn []
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

(t/test "Sunday as 0 and 7, names, steps, lists, ranges"
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
    (.throws assert (fn [] (cron/parse "0 9 * *")) #"five fields")
    (.throws assert (fn [] (cron/parse "61 9 * * *")) #"minute field allows 0 to 59")
    (.throws assert (fn [] (cron/parse "0 9 * * xyz")) #"weekday field has no number")
    (.throws assert (fn [] (cron/parse "0 9 * * */0")) #"positive number")
    (.throws assert (fn [] (cron/parse "0 9 5-2 * *")) #"runs backwards")
    (assert/equal (cron/valid-zone? "Europe/Berlin") true)
    (assert/equal (cron/valid-zone? "Mars/Olympus") false)))

(t/test "Feb 29 is found within 366 days or answers nil"
  (fn []
    (assert/equal (next-iso "0 0 29 2 *" "UTC" "2027-03-01T00:00:00Z") "2028-02-29T00:00:00.000Z")
    (assert/equal (nil? (cron/next-run-of "0 0 30 2 *" "UTC" (at "2026-10-15T00:00:00Z"))) true)))
