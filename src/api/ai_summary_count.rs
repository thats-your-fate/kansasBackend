// One bounded, read-only preview scan per API process. Nothing is persisted and
// no inference token or endpoint is needed. Results describe the scan time;
// generation always compares current facts again.
#[derive(serde::Serialize)]
struct SummaryCount {
    scan_id: String,
    activity_since: String,
    model: String,
    prompt_version: &'static str,
    status: &'static str,
    total_candidates: Option<u64>,
    scanned: u64,
    new_wells: u64,
    changed_wells: u64,
    unchanged_wells: u64,
    excluded_wells: u64,
    checked_at: Option<u64>,
    error: Option<String>,
    #[serde(skip)]
    started: std::time::Instant,
}
impl SummaryCount {
    fn new(since: &str, model: &str) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            scan_id: format!(
                "{}-{}",
                count_now_ms(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ),
            activity_since: since.to_owned(),
            model: model.to_owned(),
            prompt_version: PROMPT_VERSION,
            status: "scanning",
            total_candidates: None,
            scanned: 0,
            new_wells: 0,
            changed_wells: 0,
            unchanged_wells: 0,
            excluded_wells: 0,
            checked_at: None,
            error: None,
            started: std::time::Instant::now(),
        }
    }
    fn record(&mut self, saved_matches: Option<Option<bool>>) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.started.elapsed() < std::time::Duration::from_secs(1800),
            "Count exceeded 30 minutes; try a narrower activity window"
        );
        self.scanned += 1;
        match saved_matches {
            None => self.excluded_wells += 1,
            Some(None) => self.new_wells += 1,
            Some(Some(false)) => self.changed_wells += 1,
            Some(Some(true)) => self.unchanged_wells += 1,
        }
        Ok(())
    }
    fn json(&self) -> Value {
        let mut value = serde_json::to_value(self).expect("serializable count");
        value["new_or_changed_wells"] = json!(self.new_wells + self.changed_wells);
        value["eligible_wells"] = json!(self.new_wells + self.changed_wells + self.unchanged_wells);
        value["exact"] = json!(self.status == "completed");
        value
    }
}
fn count_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
struct CountRegistry {
    current: Option<Arc<Mutex<SummaryCount>>>,
}
static SUMMARY_COUNT: Mutex<CountRegistry> = Mutex::new(CountRegistry { current: None });
impl CountRegistry {
    fn reserve(
        &mut self,
        since: &str,
        model: &str,
        scan_id: Option<&str>,
    ) -> Result<(Arc<Mutex<SummaryCount>>, bool), String> {
        if let Some(current) = self.current.as_ref() {
            let count = current.lock().map_err(|_| "Count registry unavailable")?;
            if let Some(id) = scan_id {
                if id != count.scan_id || since != count.activity_since || model != count.model {
                    return Err("Count expired or filters changed; count wells again".to_owned());
                }
                return Ok((current.clone(), false));
            }
            if count.status == "scanning" {
                if since != count.activity_since || model != count.model {
                    return Err("Another count is in progress; wait for it to finish before changing the count filters".to_owned());
                }
                return Ok((current.clone(), false));
            }
        } else if scan_id.is_some() {
            return Err("Count expired after an API restart; count wells again".to_owned());
        }
        let count = Arc::new(Mutex::new(SummaryCount::new(since, model)));
        self.current = Some(count.clone());
        Ok((count, true))
    }
}
fn reserve_count(
    since: &str,
    model: &str,
    scan_id: Option<&str>,
) -> Result<(Arc<Mutex<SummaryCount>>, bool), String> {
    SUMMARY_COUNT
        .lock()
        .map_err(|_| "Count registry unavailable")?
        .reserve(since, model, scan_id)
}
fn count_snapshot(count: &Mutex<SummaryCount>) -> Result<Value, String> {
    Ok(count
        .lock()
        .map_err(|_| "Count registry unavailable")?
        .json())
}
fn count_total(count: &Mutex<SummaryCount>, total: usize) -> anyhow::Result<()> {
    count
        .lock()
        .map_err(|_| anyhow::anyhow!("Count registry unavailable"))?
        .total_candidates = Some(total as u64);
    Ok(())
}
fn count_record(count: &Mutex<SummaryCount>, matches: Option<Option<bool>>) -> anyhow::Result<()> {
    count
        .lock()
        .map_err(|_| anyhow::anyhow!("Count registry unavailable"))?
        .record(matches)
}
fn finish_count(count: &Mutex<SummaryCount>, result: Result<(), String>) {
    if let Ok(mut c) = count.lock() {
        let result = result.and_then(|_| {
            if c.total_candidates == Some(c.scanned) {
                Ok(())
            } else {
                Err("Count did not finish; count wells again".to_owned())
            }
        });
        match result {
            Ok(()) => {
                c.status = "completed";
                c.checked_at = Some(count_now_ms());
            }
            Err(error) => {
                c.status = "failed";
                c.error = Some(error);
            }
        }
    }
}
async fn count_no_store(mut response: axum::response::Response) -> axum::response::Response {
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("private, no-store"),
    );
    response
}

struct CountTaskGuard(Arc<Mutex<SummaryCount>>);
impl Drop for CountTaskGuard {
    fn drop(&mut self) {
        if let Ok(mut c) = self.0.lock() {
            if c.status == "scanning" {
                c.status = "failed";
                c.error = Some("Count interrupted; count wells again".to_owned());
            }
        }
    }
}

#[cfg(test)]
mod exact_count_tests {
    use super::*;
    #[test]
    fn new_changed_unchanged_and_excluded_are_distinct_and_complete() {
        let c = Mutex::new(SummaryCount::new("2025-01-01", "model"));
        count_total(&c, 4).unwrap();
        for saved in [Some(None), Some(Some(false)), Some(Some(true)), None] {
            count_record(&c, saved).unwrap();
        }
        assert_eq!(count_snapshot(&c).unwrap()["exact"], false);
        finish_count(&c, Ok(()));
        let v = count_snapshot(&c).unwrap();
        assert_eq!(v["exact"], true);
        assert_eq!(v["new_or_changed_wells"], 2);
        assert_eq!(v["new_wells"], 1);
        assert_eq!(v["changed_wells"], 1);
        assert_eq!(v["unchanged_wells"], 1);
        assert_eq!(v["excluded_wells"], 1);
        assert_eq!(v["eligible_wells"], 3);
    }
    #[test]
    fn incomplete_or_failed_scan_never_claims_an_exact_count() {
        let c = Mutex::new(SummaryCount::new("2025-01-01", "model"));
        count_total(&c, 2).unwrap();
        count_record(&c, Some(None)).unwrap();
        finish_count(&c, Ok(()));
        assert_eq!(count_snapshot(&c).unwrap()["status"], "failed");
        assert_eq!(count_snapshot(&c).unwrap()["exact"], false);
        finish_count(&c, Err("database unavailable".into()));
        assert_eq!(count_snapshot(&c).unwrap()["error"], "database unavailable");
    }
    #[test]
    fn one_scan_is_shared_only_for_identical_filters_and_refresh_is_fresh() {
        let mut registry = CountRegistry { current: None };
        let (a, start) = registry.reserve("2025-01-01", "model", None).unwrap();
        assert!(start);
        let (b, start) = registry.reserve("2025-01-01", "model", None).unwrap();
        assert!(!start);
        assert!(Arc::ptr_eq(&a, &b));
        assert!(registry.reserve("2025-01-02", "model", None).is_err());
        assert!(registry
            .reserve("2025-01-01", "different-model", None)
            .is_err());
        assert!(registry
            .reserve("2025-01-01", "model", Some("stale"))
            .is_err());
        count_total(&a, 0).unwrap();
        finish_count(&a, Ok(()));
        let old_id = a.lock().unwrap().scan_id.clone();
        assert!(
            !registry
                .reserve("2025-01-01", "model", Some(&old_id))
                .unwrap()
                .1
        );
        let (fresh, start) = registry.reserve("2025-01-01", "model", None).unwrap();
        assert!(start);
        assert_ne!(old_id, fresh.lock().unwrap().scan_id);
    }
    #[test]
    fn interrupted_or_overlong_scan_is_retryable_and_never_exact() {
        let c = Arc::new(Mutex::new(SummaryCount::new("2025-01-01", "model")));
        {
            let _guard = CountTaskGuard(c.clone());
        }
        assert_eq!(count_snapshot(&c).unwrap()["status"], "failed");
        assert_eq!(count_snapshot(&c).unwrap()["exact"], false);
        let mut old = SummaryCount::new("2025-01-01", "model");
        old.started -= std::time::Duration::from_secs(1801);
        assert!(old.record(Some(None)).is_err());
        assert_eq!(old.scanned, 0);
    }
}
