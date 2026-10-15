(ns cronus.worker
  "The Worker: `fetch` is the management API, `scheduled` the minute that
  fires what is due. Deploy with wrangler; see wrangler.toml."
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
        fired (js-await (deliver/tick! (.-DB env) now))]
    (when (pos? fired)
      (js/console.log (str "cronus: " fired " job(s) due at " (.toISOString (js/Date. now)))))))

(def default
  {:fetch handler
   :scheduled scheduled-handler})
