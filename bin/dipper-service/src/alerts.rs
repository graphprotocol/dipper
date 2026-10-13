//! Sends the log lines an operator has to act on to Slack. A line is picked out by its `event`
//! tag, so the code that logs it needs no change. Posting happens in the background, at most once
//! per event in each throttle window, and never holds up or fails the code that logged it.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use tokio::{sync::mpsc, time::Instant};
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context};
use url::Url;

use crate::config::AlertsConfig;

/// Alerts that can wait to be posted. More are dropped, as a burst that size is mostly held back
/// by the throttle anyway, and counted into the held-back figure for their event.
const QUEUE: usize = 64;

/// How long a single post to Slack may take.
const POST_TIMEOUT: Duration = Duration::from_secs(10);

/// How often to check for events whose window has ended with alerts held back, to post a count.
const HELD_BACK_CHECK: Duration = Duration::from_secs(60);

/// A tagged log line to alert on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Alert {
    event: String,
    level: Level,
    message: String,
    fields: String,
}

/// Picks the tagged log lines out of dipper's logging and queues them for posting.
pub struct AlertLayer {
    events: HashSet<String>,
    queue: mpsc::Sender<Alert>,
    dropped: Dropped,
}

/// Alerts dropped because the queue was full, counted by event.
type Dropped = Arc<Mutex<HashMap<String, u64>>>;

/// The alert layer for dipper's logging, with its poster running in the background, or `None`
/// when no Slack webhook is configured.
pub fn layer(config: &AlertsConfig) -> Option<AlertLayer> {
    let url = config.slack_webhook_url.as_ref()?.as_ref().clone();
    let (queue, alerts) = mpsc::channel(QUEUE);
    let dropped = Dropped::default();
    tokio::spawn(post_alerts(
        alerts,
        url,
        config.throttle,
        Arc::clone(&dropped),
    ));
    Some(AlertLayer {
        events: config.events.iter().cloned().collect(),
        queue,
        dropped,
    })
}

impl<S: Subscriber> Layer<S> for AlertLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut tag = EventTag::default();
        event.record(&mut tag);
        let Some(tag) = tag.0.filter(|tag| self.events.contains(tag)) else {
            return;
        };
        let mut line = LogLine::default();
        event.record(&mut line);
        let alert = Alert {
            event: tag,
            level: *event.metadata().level(),
            message: line.message,
            fields: line.fields,
        };
        // Waiting here would hold up the code that logged it, so a full queue drops the alert;
        // the poster counts it into the next message for its event.
        if let Err(full) = self.queue.try_send(alert) {
            *self
                .dropped
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(full.into_inner().event)
                .or_default() += 1;
        }
    }
}

/// Only a log line's `event` tag, read first so a line no alert is set up for costs no more.
#[derive(Default)]
struct EventTag(Option<String>);

impl Visit for EventTag {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" {
            self.0 = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "event" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

/// A log line's message and its fields other than the `event` tag.
#[derive(Default)]
struct LogLine {
    message: String,
    fields: String,
}

impl LogLine {
    fn record(&mut self, field: &Field, value: String) {
        match field.name() {
            "event" => {}
            "message" => self.message = value,
            name => {
                if !self.fields.is_empty() {
                    self.fields.push(' ');
                }
                self.fields.push_str(&format!("{name}={value}"));
            }
        }
    }
}

impl Visit for LogLine {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record(field, format!("{value:?}"));
    }
}

/// Post each queued alert to Slack, subject to the throttle, until the queue closes.
async fn post_alerts(
    mut alerts: mpsc::Receiver<Alert>,
    url: Url,
    window: Duration,
    dropped: Dropped,
) {
    let http = match reqwest::Client::builder().timeout(POST_TIMEOUT).build() {
        Ok(http) => http,
        Err(err) => {
            tracing::error!(error = %err, "Failed to build the HTTP client for Slack alerts; none will be sent");
            return;
        }
    };
    let mut throttle = Throttle::new(window);
    // A window shorter than the usual check is checked as often as it ends.
    let mut check = tokio::time::interval(HELD_BACK_CHECK.min(window).max(Duration::from_secs(1)));
    loop {
        let mut texts = Vec::new();
        tokio::select! {
            alert = alerts.recv() => {
                let Some(alert) = alert else {
                    return;
                };
                if let Some(held_back) = throttle.admit(&alert.event, Instant::now()) {
                    texts.push(alert_text(&alert, held_back));
                }
            }
            _ = check.tick() => {
                for (event, held_back) in throttle.due(Instant::now()) {
                    texts.push(held_back_text(&event, held_back, window));
                }
            }
        }
        let lost = std::mem::take(&mut *dropped.lock().unwrap_or_else(PoisonError::into_inner));
        for (event, count) in lost {
            throttle.hold_back(&event, count, Instant::now());
        }
        for text in texts {
            post(&http, &url, &text).await;
        }
    }
}

/// At most 1 message per event in each window; alerts in between are counted and reported in
/// the next message for that event.
struct Throttle {
    window: Duration,
    events: HashMap<String, Window>,
}

struct Window {
    started: Instant,
    held_back: u64,
}

impl Throttle {
    fn new(window: Duration) -> Self {
        Self {
            window,
            events: HashMap::new(),
        }
    }

    /// Whether to post this alert now and, if so, how many were held back before it.
    fn admit(&mut self, event: &str, now: Instant) -> Option<u64> {
        if let Some(window) = self.events.get_mut(event)
            && now.duration_since(window.started) < self.window
        {
            window.held_back += 1;
            return None;
        }
        let held_back = self
            .events
            .insert(
                event.to_owned(),
                Window {
                    started: now,
                    held_back: 0,
                },
            )
            .map_or(0, |window| window.held_back);
        Some(held_back)
    }

    /// Count alerts that never reached the throttle into its held-back figure for their event;
    /// one with no window yet is reported at the next check.
    fn hold_back(&mut self, event: &str, count: u64, now: Instant) {
        let window = self.window;
        self.events
            .entry(event.to_owned())
            .or_insert_with(|| Window {
                started: now.checked_sub(window).unwrap_or(now),
                held_back: 0,
            })
            .held_back += count;
    }

    /// Events whose window has ended with alerts held back, each with how many; the message
    /// reporting them starts that event's next window.
    fn due(&mut self, now: Instant) -> Vec<(String, u64)> {
        let mut due = Vec::new();
        for (event, window) in &mut self.events {
            if window.held_back > 0 && now.duration_since(window.started) >= self.window {
                due.push((event.clone(), window.held_back));
                *window = Window {
                    started: now,
                    held_back: 0,
                };
            }
        }
        due
    }
}

fn alert_text(alert: &Alert, held_back: u64) -> String {
    let mut text = format!(
        "dipper {} `{}`: {}",
        alert.level,
        slack_escape(&alert.event),
        slack_escape(&alert.message)
    );
    if !alert.fields.is_empty() {
        text.push_str(&format!("\n{}", slack_escape(&alert.fields)));
    }
    if held_back > 0 {
        text.push_str(&format!("\n{held_back} more since the last alert"));
    }
    text
}

/// Escape the 3 characters Slack reads as markup, so text like an HTML error page shows as written
/// instead of turning into links or mentions.
fn slack_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn held_back_text(event: &str, held_back: u64, window: Duration) -> String {
    format!(
        "dipper `{event}`: {held_back} more in the last {}",
        describe(window)
    )
}

/// A throttle window in words: whole minutes where it divides into them, seconds otherwise.
fn describe(window: Duration) -> String {
    let seconds = window.as_secs();
    match (seconds / 60, seconds % 60) {
        (1, 0) => "minute".to_owned(),
        (minutes, 0) if minutes > 0 => format!("{minutes} minutes"),
        _ if seconds == 1 => "second".to_owned(),
        _ => format!("{seconds} seconds"),
    }
}

/// Post a message to the Slack webhook, logging a failure without the webhook's URL, which is a
/// secret.
async fn post(http: &reqwest::Client, url: &Url, text: &str) {
    let sent = http
        .post(url.clone())
        .json(&serde_json::json!({ "text": text }))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status);
    if let Err(err) = sent {
        tracing::warn!(error = %err.without_url(), "Failed to post an alert to Slack");
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::layer::SubscriberExt;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method},
    };

    use super::*;

    fn test_layer(events: &[&str]) -> (AlertLayer, mpsc::Receiver<Alert>) {
        let (queue, alerts) = mpsc::channel(QUEUE);
        let layer = AlertLayer {
            events: events.iter().map(|event| (*event).to_owned()).collect(),
            queue,
            dropped: Dropped::default(),
        };
        (layer, alerts)
    }

    #[test]
    fn picks_out_only_the_listed_events_whatever_their_level() {
        let (layer, mut alerts) = test_layer(&["agreement_cancel_stuck", "nonce_gap_fill_failed"]);
        let logging = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));

        tracing::dispatcher::with_default(&logging, || {
            tracing::error!(
                event = "agreement_cancel_stuck",
                agreement_id = %"0xab",
                attempts = 10u32,
                "Cancelling an agreement keeps failing"
            );
            tracing::warn!(
                event = "nonce_gap_fill_failed",
                nonce = 7u64,
                "Gap fill failed"
            );
        });
        tracing::dispatcher::with_default(&logging, || {
            tracing::error!(event = "something_else", "Not listed");
            tracing::error!("Not tagged");
        });

        assert_eq!(
            alerts.try_recv().expect("first alert"),
            Alert {
                event: "agreement_cancel_stuck".to_owned(),
                level: Level::ERROR,
                message: "Cancelling an agreement keeps failing".to_owned(),
                fields: "agreement_id=0xab attempts=10".to_owned(),
            }
        );
        assert_eq!(
            alerts.try_recv().expect("second alert").event,
            "nonce_gap_fill_failed"
        );
        assert!(alerts.try_recv().is_err(), "nothing else");
    }

    /// Every warning in dipper passes through this layer, so 1 it won't post shouldn't cost
    /// formatting its fields.
    #[test]
    fn leaves_the_fields_of_lines_it_wont_post_unformatted() {
        struct Counted<'a>(&'a std::sync::atomic::AtomicUsize);
        impl std::fmt::Debug for Counted<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                f.write_str("counted")
            }
        }
        let formatted = std::sync::atomic::AtomicUsize::new(0);
        let (layer, _alerts) = test_layer(&["agreement_cancel_stuck"]);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(event = "something_else", value = ?Counted(&formatted), "Not listed");
            tracing::warn!(value = ?Counted(&formatted), "Not tagged");
        });

        assert_eq!(formatted.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn counts_alerts_dropped_when_the_queue_is_full() {
        let (queue, _alerts) = mpsc::channel(1);
        let layer = AlertLayer {
            events: HashSet::from(["rpc_blocks_refused".to_owned()]),
            queue,
            dropped: Dropped::default(),
        };
        let dropped = Arc::clone(&layer.dropped);
        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            for _ in 0..3 {
                tracing::error!(event = "rpc_blocks_refused", "Refused");
            }
        });

        assert_eq!(
            *dropped.lock().unwrap(),
            HashMap::from([("rpc_blocks_refused".to_owned(), 2)])
        );
    }

    #[test]
    fn sends_at_most_1_message_per_event_in_each_window() {
        let window = Duration::from_secs(900);
        let mut throttle = Throttle::new(window);
        let start = Instant::now();

        assert_eq!(throttle.admit("a", start), Some(0));
        assert_eq!(throttle.admit("a", start + Duration::from_secs(60)), None);
        assert_eq!(throttle.admit("a", start + Duration::from_secs(120)), None);
        assert_eq!(throttle.admit("b", start), Some(0), "each event on its own");

        assert!(throttle.due(start + Duration::from_secs(600)).is_empty());
        let after = start + window;
        assert_eq!(throttle.due(after), vec![("a".to_owned(), 2)]);
        assert_eq!(
            throttle.admit("a", after + Duration::from_secs(60)),
            None,
            "the count starts the next window"
        );
        assert_eq!(throttle.admit("a", after + window), Some(1));
    }

    #[test]
    fn counts_dropped_alerts_into_the_held_back_figure() {
        let window = Duration::from_secs(900);
        let mut throttle = Throttle::new(window);
        let start = Instant::now();
        assert_eq!(throttle.admit("a", start), Some(0));

        throttle.hold_back("a", 5, start + Duration::from_secs(60));
        throttle.hold_back("b", 2, start + Duration::from_secs(60));

        let mut due = throttle.due(start + window);
        due.sort();
        assert_eq!(due, vec![("a".to_owned(), 5), ("b".to_owned(), 2)]);
    }

    #[test]
    fn escapes_what_slack_reads_as_markup() {
        let alert = Alert {
            event: "nonce_gap_fill_failed".to_owned(),
            level: Level::WARN,
            message: "Gap fill failed".to_owned(),
            fields: "error=<html>502 & more</html>".to_owned(),
        };

        assert!(alert_text(&alert, 0).ends_with("error=&lt;html&gt;502 &amp; more&lt;/html&gt;"));
    }

    #[test]
    fn says_what_happened_and_how_many_more() {
        let alert = Alert {
            event: "agreement_cancel_stuck".to_owned(),
            level: Level::ERROR,
            message: "Cancelling an agreement keeps failing".to_owned(),
            fields: "agreement_id=0xab".to_owned(),
        };

        assert_eq!(
            alert_text(&alert, 3),
            "dipper ERROR `agreement_cancel_stuck`: Cancelling an agreement keeps failing\n\
             agreement_id=0xab\n3 more since the last alert"
        );
        assert_eq!(
            held_back_text("rpc_blocks_refused", 5, Duration::from_secs(900)),
            "dipper `rpc_blocks_refused`: 5 more in the last 15 minutes"
        );
        assert_eq!(describe(Duration::from_secs(60)), "minute");
        assert_eq!(describe(Duration::from_secs(90)), "90 seconds");
        assert_eq!(describe(Duration::from_secs(30)), "30 seconds");
    }

    #[tokio::test]
    async fn posts_the_message_as_slack_text() {
        let slack = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_json(serde_json::json!({ "text": "hello" })))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&slack)
            .await;
        let url: Url = slack.uri().parse().expect("URL");

        post(&reqwest::Client::new(), &url, "hello").await;
    }
}
