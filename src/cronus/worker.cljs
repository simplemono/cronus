(ns cronus.worker
  "The Worker: `fetch` is the management API, `scheduled` the minute that
  claims what is due and queues it, `queue` the consumer that makes the
  calls. Deploy with wrangler; see wrangler.toml."
  (:require [cronus.api :as api]
            [cronus.deliver :as deliver]))

(defn ^:async handler [req env _ctx]
  (try
    (js-await (api/handle req env))
    (catch :default e
      (js/console.error "cronus: request failed" (or (.-stack e) e))
      (js/Response. (js/JSON.stringify {:error "cronus failed; see the Worker's logs."})
                    {:status 500 :headers {"content-type" "application/json"}}))))

(defn ^:async scheduled-handler [event env _ctx]
  (let [now (or (.-scheduledTime event) (js/Date.now))
        queued (js-await (deliver/tick! (.-DB env) (.-QUEUE env) now))]
    (when (pos? queued)
      (js/console.log (str "cronus: " queued " job(s) queued at " (.toISOString (js/Date. now)))))))

(defn ^:async queue-handler [batch env _ctx]
  (doseq [message (.-messages batch)]
    (let [body (.-body message)]
      (try
        (let [outcome (js-await (deliver/deliver! (.-DB env) body))]
          (when (contains? #{:stale :gone} outcome)
            (js/console.log (str "cronus: message for " (:id body) " dropped, " (name outcome))))
          (.ack message))
        (catch :default e
          (js/console.error "cronus: delivery of" (:id body) "failed" (or (.-stack e) e))
          (.retry message))))))

(def default
  {:fetch handler
   :scheduled scheduled-handler
   :queue queue-handler})
