// Writes dist/cronus/schema.mjs from schema.sql, so the Worker can apply the
// schema itself and the SQL has one source.
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
const sql = readFileSync(new URL("../schema.sql", import.meta.url), "utf8");
mkdirSync(new URL("../dist/cronus/", import.meta.url), { recursive: true });
writeFileSync(new URL("../dist/cronus/schema.mjs", import.meta.url),
  "export const schema = " + JSON.stringify(sql) + ";\n");
console.log("dist/cronus/schema.mjs written");
