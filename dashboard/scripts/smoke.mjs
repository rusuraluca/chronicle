import { readFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const distIndex = resolve(root, "dist/index.html");

if (!existsSync(distIndex)) {
  console.error("smoke: dist/index.html missing — run npm run build first");
  process.exit(1);
}

const html = readFileSync(distIndex, "utf8");
if (!html.includes("Chronicle") && !html.includes("root")) {
  console.error("smoke: unexpected dist/index.html contents");
  process.exit(1);
}

console.log("dashboard smoke ok");
