export default function App() {
  return (
    <div className="shell">
      <header className="brand-bar">
        <div>
          <p className="brand">Chronicle</p>
          <p className="tagline">Ordered event log · correctness · variable-speed replay</p>
        </div>
      </header>
      <main className="panel">
        <h1>Scaffold</h1>
        <p>
          Ops dashboard and benchmark views land in a follow-up PR. API health lives at{" "}
          <code>/healthz</code>.
        </p>
      </main>
    </div>
  );
}
