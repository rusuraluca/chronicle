export async function fetchHealth(): Promise<{ status: string }> {
  const base = import.meta.env.VITE_API_BASE ?? "";
  const res = await fetch(`${base}/healthz`);
  if (!res.ok) throw new Error(`healthz failed: ${res.status}`);
  return res.json();
}
