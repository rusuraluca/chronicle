use chronicle_core::{pacing_delay, ReplaySpeed};
use chrono::{Duration, TimeZone, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_pacing(c: &mut Criterion) {
    let a = Utc.timestamp_opt(0, 0).unwrap();
    let b = a + Duration::milliseconds(250);

    c.bench_function("pacing_delay_10x", |bencher| {
        bencher.iter(|| {
            black_box(pacing_delay(
                black_box(a),
                black_box(b),
                black_box(ReplaySpeed::TenX),
            ))
        });
    });
}

criterion_group!(benches, bench_pacing);
criterion_main!(benches);
