/*
 * Softr "Import classes from MTEK" vibe-coding block (Subs).
 *
 * Replaces the Airtable view "1. IMPORT HERE MTEK" (download MTEK report + paste).
 * Flow: this page creates a row in Airtable "Import Runs" (Instructors - Subs base)
 *   -> calls the Make webhook "Subs - Import MTEK classes -> GitHub"
 *   -> GitHub workflow "Subs - Import classes from MTEK" (scripts/subs-import-mtek-classes.mjs)
 *   -> the script validates, (optionally) creates the classes in All Classes and writes
 *      status / counters / notes back to the same Import Runs row, which this page polls.
 * "Import" is only enabled after a successful dry run ("Validate") with the same studios + dates.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { datasource, q, useRecordCreate, useRecords } from "@/lib/datasource";
import { useCurrentUser } from "@/lib/user";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { AlertTriangle, CheckCircle2, ExternalLink, Loader2, XCircle } from "lucide-react";
import { toast } from "sonner";

const ds = datasource.define({
  importRuns: "importRuns", // Import Runs tblGRf0nyYyCo1bXT (Instructors - Subs)
});

// Make scenario "Subs - Import MTEK classes -> GitHub" (custom webhook, GET ?recordId=)
const MAKE_HOOK = "https://hook.us2.make.com/fqx3widwtfujxl2qm49wwjj7qm34nkjn";
const STUDIOS = ["Centre-Ville", "Vieux-Port", "Westmount", "Rockland"];
const MAX_DAYS = 93;

const select = q.select({
  name: "Name",
  mode: "Mode",
  status: "Status",
  studios: "Studios",
  minDate: "Min date",
  maxDate: "Max date",
  step: "Current step",
  found: "Classes found",
  exists: "Already in Airtable",
  toCreate: "To create",
  created: "Created",
  failed: "Failed",
  fix: "Needs fixing",
  noInstr: "No instructor match",
  first: "First class",
  last: "Last class",
  notes: "Notes",
  preview: "Preview JSON",
  by: "Requested by",
  started: "Started at",
  finished: "Finished at",
  run: "GitHub run",
});
const createSel = q.select({
  name: "Name",
  mode: "Mode",
  status: "Status",
  studios: "Studios",
  minDate: "Min date",
  maxDate: "Max date",
  step: "Current step",
  by: "Requested by",
  started: "Started at",
});

type Rec = { id: string; fields: Record<string, any> };
const asText = (v: any): string => (v == null ? "" : typeof v === "object" ? String(v.label ?? v.name ?? v.title ?? "") : String(v));
const day = (v: any) => asText(v).slice(0, 10);
const num = (v: any) => (v == null || v === "" ? null : Number(v));

function todayYmd(offset = 0) {
  const d = new Date();
  d.setDate(d.getDate() + offset);
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto" }).format(d);
}
function daysBetween(a: string, b: string) {
  return Math.round((new Date(b).getTime() - new Date(a).getTime()) / 86400000) + 1;
}
function fmtWhen(iso: string) {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "";
  return d.toLocaleString("en-CA", { timeZone: "America/Toronto", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

const STATUS_STYLE: Record<string, string> = {
  Queued: "bg-slate-100 text-slate-800 border-slate-300",
  Running: "bg-sky-100 text-sky-900 border-sky-300",
  "Ready (dry run done)": "bg-violet-100 text-violet-900 border-violet-300",
  Completed: "bg-green-100 text-green-900 border-green-300",
  "Completed with warnings": "bg-amber-100 text-amber-900 border-amber-300",
  Failed: "bg-red-100 text-red-900 border-red-300",
};
const isActive = (s: string) => s === "Queued" || s === "Running";

export default function SubsImportMtek() {
  const user = useCurrentUser();
  const [picked, setPicked] = useState<string[]>([]);
  const [minDate, setMinDate] = useState(todayYmd(0));
  const [maxDate, setMaxDate] = useState(todayYmd(30));
  const [currentId, setCurrentId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [filter, setFilter] = useState<"all" | "new" | "exists" | "fix">("all");
  const notesRef = useRef<HTMLPreElement | null>(null);

  const allStudios = picked.length === STUDIOS.length;
  const studiosValue = allStudios ? "All" : STUDIOS.filter((s) => picked.includes(s)).join(", ");

  const runsQ = useRecords({
    from: ds.importRuns,
    select,
    count: 25,
    orderBy: q.desc("started"),
  });
  const runs: Rec[] = useMemo(() => (runsQ.data?.pages?.[0]?.items ?? []) as Rec[], [runsQ.data]);

  const create = useRecordCreate({
    from: ds.importRuns,
    fields: createSel,
    onError: (e: any) => toast.error(e?.message ?? "Could not create the run"),
  });

  // poll while something is running
  const current: Rec | null = useMemo(() => {
    if (currentId) return runs.find((r) => r.id === currentId) ?? null;
    return runs[0] ?? null;
  }, [runs, currentId]);
  const curStatus = asText(current?.fields.status);
  useEffect(() => {
    if (!busy && !runs.some((r) => isActive(asText(r.fields.status)))) return;
    const t = setInterval(() => runsQ.refetch(), 3000);
    return () => clearInterval(t);
  }, [busy, runs.map((r) => r.id + asText(r.fields.status)).join("|")]);
  useEffect(() => {
    if (notesRef.current) notesRef.current.scrollTop = notesRef.current.scrollHeight;
  }, [current?.fields.notes]);

  const rangeDays = /^\d{4}-\d{2}-\d{2}$/.test(minDate) && /^\d{4}-\d{2}-\d{2}$/.test(maxDate) ? daysBetween(minDate, maxDate) : 0;
  const inputsError =
    picked.length === 0
      ? "Pick at least one studio."
      : rangeDays <= 0
        ? "Pick a valid date range (max date on or after min date)."
        : rangeDays > MAX_DAYS
          ? `Maximum ${MAX_DAYS} days per import (you picked ${rangeDays}).`
          : "";

  const anyActive = runs.some((r) => isActive(asText(r.fields.status)));

  // Latest finished dry run for exactly these inputs
  const matchingDry = useMemo(
    () =>
      runs.find(
        (r) =>
          asText(r.fields.mode) === "Dry run" &&
          asText(r.fields.status) === "Ready (dry run done)" &&
          asText(r.fields.studios) === studiosValue &&
          day(r.fields.minDate) === minDate &&
          day(r.fields.maxDate) === maxDate,
      ) ?? null,
    [runs, studiosValue, minDate, maxDate],
  );
  const matchingImportDone = useMemo(
    () =>
      matchingDry
        ? runs.some(
            (r) =>
              asText(r.fields.mode) === "Import" &&
              asText(r.fields.studios) === studiosValue &&
              day(r.fields.minDate) === minDate &&
              day(r.fields.maxDate) === maxDate &&
              !!r.fields.started &&
              new Date(asText(r.fields.started)) > new Date(asText(matchingDry.fields.started)),
          )
        : false,
    [runs, matchingDry, studiosValue, minDate, maxDate],
  );
  const dryToCreate = num(matchingDry?.fields.toCreate) ?? 0;

  async function start(mode: "Dry run" | "Import") {
    if (inputsError || busy) return;
    if (!create.enabled) {
      toast.error("You don't have permission to start imports.");
      return;
    }
    setBusy(true);
    try {
      const now = new Date().toISOString();
      const rec: any = await create.mutateAsync({
        name: `${mode} - ${studiosValue} - ${minDate} to ${maxDate}`,
        mode,
        status: "Queued",
        studios: studiosValue,
        minDate,
        maxDate,
        step: "Waiting for the import to start…",
        by: user?.email ?? "",
        started: now,
      });
      const id = String(rec?.id ?? "");
      if (!id) throw new Error("The run was created but no record id came back.");
      setCurrentId(id);
      // Fire the Make webhook (GET; no-cors because Make doesn't send CORS headers)
      await fetch(`${MAKE_HOOK}?recordId=${encodeURIComponent(id)}`, { method: "GET", mode: "no-cors" });
      toast.success(mode === "Dry run" ? "Validation started" : "Import started");
      setFilter("all");
      await runsQ.refetch();
    } catch (e: any) {
      toast.error(e?.message ?? "Could not start the run");
    } finally {
      setBusy(false);
    }
  }

  const f = current?.fields ?? {};
  const preview: any[] = useMemo(() => {
    try {
      const p = JSON.parse(asText(f.preview) || "[]");
      return Array.isArray(p) ? p : [];
    } catch {
      return [];
    }
  }, [f.preview]);
  const shownPreview = preview.filter((p) => filter === "all" || p.s === filter);
  const waitingLong = curStatus === "Queued" && f.started && Date.now() - new Date(asText(f.started)).getTime() > 3 * 60000;

  const stat = (label: string, value: any, tone = "") => (
    <div className={`rounded-md border p-3 ${tone}`}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-xl font-semibold">{value ?? "—"}</div>
    </div>
  );

  return (
    <div className="space-y-6 p-4">
      <div>
        <h1 className="text-2xl font-semibold">Import classes from MTEK</h1>
        <p className="text-sm text-muted-foreground">
          Pulls the MTEK "Class Session Utilization Details" report straight into Airtable. Existing classes are never changed — a class that is already in
          Airtable (same MTEK Class ID) is skipped. Always run <b>Validate</b> first: nothing is written until you click <b>Import</b>.
        </p>
      </div>

      {/* Inputs */}
      <section className="space-y-4 rounded-md border p-4">
        <div className="space-y-2">
          <div className="text-sm font-medium">Studios</div>
          <div className="flex flex-wrap gap-3">
            <label className="flex items-center gap-2 text-sm font-medium">
              <input type="checkbox" checked={allStudios} onChange={(e) => setPicked(e.target.checked ? [...STUDIOS] : [])} />
              All studios
            </label>
            {STUDIOS.map((s) => (
              <label key={s} className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={picked.includes(s)}
                  onChange={(e) => setPicked(e.target.checked ? [...picked, s] : picked.filter((x) => x !== s))}
                />
                {s}
              </label>
            ))}
          </div>
        </div>
        <div className="flex flex-wrap items-end gap-4">
          <div className="space-y-1">
            <label className="text-sm font-medium">From (any date)</label>
            <Input type="date" value={minDate} onChange={(e) => setMinDate(e.target.value)} className="w-44" />
          </div>
          <div className="space-y-1">
            <label className="text-sm font-medium">To (any date)</label>
            <Input type="date" value={maxDate} onChange={(e) => setMaxDate(e.target.value)} className="w-44" />
          </div>
          <div className="pb-2 text-sm text-muted-foreground">{rangeDays > 0 ? `${rangeDays} day${rangeDays === 1 ? "" : "s"}` : ""}</div>
        </div>
        {inputsError && <div className="text-sm text-red-700">{inputsError}</div>}
        <div className="flex flex-wrap items-center gap-3">
          <Button onClick={() => start("Dry run")} disabled={!!inputsError || busy || anyActive}>
            {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}1. Validate (dry run — writes nothing)
          </Button>
          <Button
            variant="default"
            className="bg-green-700 hover:bg-green-800"
            onClick={() => start("Import")}
            disabled={!!inputsError || busy || anyActive || !matchingDry || dryToCreate === 0 || matchingImportDone}
          >
            2. Import {matchingDry && dryToCreate > 0 && !matchingImportDone ? `${dryToCreate} class${dryToCreate === 1 ? "" : "es"}` : ""}
          </Button>
          {anyActive && <span className="text-sm text-muted-foreground">A run is in progress — wait for it to finish.</span>}
          {!anyActive && !matchingDry && !inputsError && (
            <span className="text-sm text-muted-foreground">Run a validation for these exact studios and dates to unlock the import.</span>
          )}
          {matchingDry && dryToCreate === 0 && <span className="text-sm text-muted-foreground">Nothing new to import for this selection.</span>}
          {matchingImportDone && <span className="text-sm text-muted-foreground">This selection was already imported after the last validation.</span>}
        </div>
      </section>

      {/* Current run */}
      {current && (
        <section className="space-y-4 rounded-md border p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="space-y-1">
              <div className="text-lg font-semibold">{asText(f.name) || "Run"}</div>
              <div className="text-xs text-muted-foreground">
                {asText(f.mode)} · {asText(f.studios)} · {day(f.minDate)} → {day(f.maxDate)}
                {f.by ? ` · by ${asText(f.by)}` : ""}
                {f.started ? ` · ${fmtWhen(asText(f.started))}` : ""}
              </div>
            </div>
            <div className="flex items-center gap-2">
              {isActive(curStatus) && <Loader2 className="h-4 w-4 animate-spin" />}
              <Badge variant="outline" className={STATUS_STYLE[curStatus] ?? ""}>
                {curStatus || "—"}
              </Badge>
              {f.run && (
                <a href={asText(f.run)} target="_blank" rel="noreferrer" className="flex items-center gap-1 text-xs text-primary underline">
                  GitHub log <ExternalLink className="h-3 w-3" />
                </a>
              )}
            </div>
          </div>

          <div className="flex items-center gap-2 text-sm">
            {curStatus === "Failed" ? (
              <XCircle className="h-4 w-4 text-red-600" />
            ) : curStatus === "Completed" ? (
              <CheckCircle2 className="h-4 w-4 text-green-600" />
            ) : curStatus === "Completed with warnings" ? (
              <AlertTriangle className="h-4 w-4 text-amber-600" />
            ) : null}
            <span className="font-medium">{asText(f.step) || "—"}</span>
          </div>
          {waitingLong && (
            <div className="rounded-md border border-amber-300 bg-amber-50 p-2 text-sm text-amber-900">
              Still waiting to start after 3 minutes. The GitHub workflow may be queued behind another run or the Make scenario may be off.
            </div>
          )}

          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            {stat("Classes found in MTEK", f.found)}
            {stat("Already in Airtable (skipped)", f.exists)}
            {stat(asText(f.mode) === "Import" ? "New classes" : "Would be created", f.toCreate)}
            {stat("Created", f.created, asText(f.mode) === "Import" ? "" : "opacity-50")}
            {stat("Needs fixing", f.fix, num(f.fix) ? "border-red-300 bg-red-50" : "")}
            {stat("No instructor match", f.noInstr, num(f.noInstr) ? "border-amber-300 bg-amber-50" : "")}
            {stat("Failed", f.failed, num(f.failed) ? "border-red-300 bg-red-50" : "")}
            {stat("First → last new class", f.first ? `${asText(f.first)} → ${asText(f.last)}` : "—")}
          </div>

          <div>
            <div className="mb-1 text-sm font-medium">Notes</div>
            <pre
              ref={notesRef}
              className="max-h-72 overflow-auto whitespace-pre-wrap rounded-md border bg-muted p-3 text-xs leading-relaxed"
            >
              {asText(f.notes) || "Waiting for the first update…"}
            </pre>
          </div>

          {preview.length > 0 && (
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-sm font-medium">Classes in this run</div>
                {(
                  [
                    ["all", `All (${preview.length})`],
                    ["new", `New (${preview.filter((p) => p.s === "new").length})`],
                    ["exists", `Already there (${preview.filter((p) => p.s === "exists").length})`],
                    ["fix", `Needs fixing (${preview.filter((p) => p.s === "fix").length})`],
                  ] as const
                ).map(([k, label]) => (
                  <Button key={k} size="sm" variant={filter === k ? "default" : "outline"} onClick={() => setFilter(k)}>
                    {label}
                  </Button>
                ))}
              </div>
              <div className="max-h-96 overflow-auto rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Class</TableHead>
                      <TableHead>Instructor (MTEK)</TableHead>
                      <TableHead>Class ID</TableHead>
                      <TableHead>Result</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {shownPreview.slice(0, 400).map((p, i) => (
                      <TableRow key={i}>
                        <TableCell className="whitespace-nowrap font-medium">{p.t}</TableCell>
                        <TableCell>
                          {p.i || "—"}
                          {p.s === "new" && p.m === 0 && <span className="ml-2 text-xs text-amber-700">no match</span>}
                        </TableCell>
                        <TableCell className="text-muted-foreground">{p.id || ""}</TableCell>
                        <TableCell>
                          {p.s === "new" && <Badge variant="outline" className="border-green-300 bg-green-100 text-green-900">new</Badge>}
                          {p.s === "exists" && <Badge variant="outline">already in Airtable</Badge>}
                          {p.s === "fix" && <Badge variant="outline" className="border-red-300 bg-red-100 text-red-900">needs fixing</Badge>}
                          {p.n ? <span className="ml-2 text-xs text-muted-foreground">{p.n}</span> : null}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              {preview.length >= 700 && <p className="text-xs text-muted-foreground">Showing the first 700 classes; the counters above cover everything.</p>}
            </div>
          )}
        </section>
      )}

      {/* History */}
      <section className="space-y-2">
        <h2 className="text-lg font-semibold">Recent runs</h2>
        <div className="overflow-x-auto rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>When</TableHead>
                <TableHead>Mode</TableHead>
                <TableHead>Studios</TableHead>
                <TableHead>Dates</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Result</TableHead>
                <TableHead>By</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7} className="text-muted-foreground">
                    No runs yet.
                  </TableCell>
                </TableRow>
              )}
              {runs.map((r) => (
                <TableRow key={r.id} className={`cursor-pointer ${current?.id === r.id ? "bg-muted" : ""}`} onClick={() => setCurrentId(r.id)}>
                  <TableCell className="whitespace-nowrap">{r.fields.started ? fmtWhen(asText(r.fields.started)) : ""}</TableCell>
                  <TableCell>{asText(r.fields.mode)}</TableCell>
                  <TableCell>{asText(r.fields.studios)}</TableCell>
                  <TableCell className="whitespace-nowrap">
                    {day(r.fields.minDate)} → {day(r.fields.maxDate)}
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline" className={STATUS_STYLE[asText(r.fields.status)] ?? ""}>
                      {asText(r.fields.status)}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-sm">
                    {asText(r.fields.mode) === "Import"
                      ? `${r.fields.created ?? 0} created / ${r.fields.found ?? 0} found`
                      : `${r.fields.toCreate ?? 0} new / ${r.fields.found ?? 0} found`}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">{asText(r.fields.by)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </section>
    </div>
  );
}
