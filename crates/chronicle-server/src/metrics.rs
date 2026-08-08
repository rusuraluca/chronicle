use std::sync::OnceLock;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

pub fn init() -> anyhow::Result<()> {
    let handle = PrometheusBuilder::new().install_recorder()?;
    let _ = HANDLE.set(handle);
    Ok(())
}

pub fn render() -> String {
    HANDLE
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}
