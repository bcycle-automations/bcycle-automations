/*
 * Softr "Review Check (Budget)" vibe-coding block.
 *
 * Coaching 1-on-1 (HQ / SPINCO HQ), Class Review and Instructor Eval hours,
 * checked per Budget week. Uses the SAME Budget week records as the studio
 * staff-hours flow. Read-only for Budget week / Budget week - Studio.
 *
 *   List view ... Budget weeks (newest first), one chip per studio showing whether it has
 *                 finished its staff-hours fetch ("Completed studio"). b.home is ignored.
 *                 No "Add record".
 *   Week view ... Step 1 studios fetched, Step 2 run the check (matches punch hours to the
 *                 reviews pushed to MTEK), Step 3 one tab per issue type with instructions
 *                 and in-place fixes. A tab empties out when its count is 0.
 *
 * Payroll issue tabs are LIVE (read from Time Punches' `Payroll issues` formula, same as the
 * studio flow). "Hours differ" / "Punch without review" come from the last Run check.
 * A review that was never pushed to MTEK, or has no punch, is NOT an issue.
 *
 * Data sources (HR base appiwfeujJzUZPPBx): Budget week, Budget week - Studio,
 * Review Check - Week, Review Check - Item, Time Punches, Employees.
 */

import { useEffect, useMemo, useState } from "react";
import { datasource, q, useLinkedRecords, useRecordCreate, useRecordUpdate, useRecords } from "@/lib/datasource";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import { ArrowLeft, CheckCircle2, Clock, Info, Loader2, Play, RefreshCw } from "lucide-react";
import { toast } from "sonner";

// Make webhook -> GitHub repository_dispatch "airtable-hr-review-check".
const RUN_CHECK_WEBHOOK = "https://hook.us2.make.com/7o12uchu91c7sehjy0qhy9n5eq3c15w9";

const ds = datasource.define({
  budgetWeek: "ac6cd17d-4051-4139-b97e-1318975c30e4",
  studioWeek: "7596a7af-3f34-4fdc-93cf-ab74fe1f9f00",
  runWeek: "5cb24d23-a1be-4400-95ec-d6db1fd9c74f",
  runItem: "0446af12-2cd9-4088-9d17-0be1b85ac4d9",
  punch: "3d1b6a52-8f0e-4c7d-9a41-2b7e5c6d8f10", // Time Punches
  employee: "9e4c7a13-5b2d-4f86-8a3c-1d0e6b7f2a59", // Employees
});

const TYPES = ["Coaching 1-on-1 (HQ)", "Coaching 1-on-1 (SPINCO HQ)", "Class Review", "Instructor Eval"];

const weekSel = q.select({ start: "Start Date", end: "End Date" });
const studioSel = q.select({ studio: "Studio", week: "Budget week", completed: "Completed studio", overall: "Overall Status" });
const runSel = q.select({
  name: "Name",
  week: "Budget week",
  status: "Overall Status",
  notes: "Notes",
  punchNoReview: "Punch without review",
  hoursDiffer: "Hours differ",
  matched: "Matched",
  punchNoReviewCheck: "Punch without review Check",
  hoursDifferCheck: "Hours differ Check",
});
const runCreate = q.select({ name: "Name", week: "Budget week", status: "Overall Status" });
const itemSel = q.select({
  name: "Name",
  run: "Run",
  type: "Type",
  result: "Result",
  date: "Date",
  employee: "Employee Name",
  punchHours: "Punch hours",
  reviewHours: "Review hours",
  punch: "Time Punch",
  detail: "Detail",
});
const punchSel = q.select({
  date: "Date",
  timeIn: "Time In",
  timeOut: "Time Out",
  name: "Employee Name",
  rateType: "Rate Type",
  employee: "Employee",
  instructor: "Instructor",
  rate: "Rate",
  issues: "Payroll issues",
  hourly: "Hourly rate",
  hours: "Total Hours",
  remove: "REMOVE/DO NOT PAY?",
});
const punchUpd = q.select({ employee: "Employee", rate: "Rate", timeOut: "Time Out", remove: "REMOVE/DO NOT PAY?" });
const employeeUpd = q.select({ desjardins: "DESJARDINS ID" });

type Rec = { id: string; fields: Record<string, any> };
type Opt = { id: string; title: string };
const asArray = (v: unknown): any[] => (v == null ? [] : Array.isArray(v) ? v : [v]);
const labelOf = (v: any): string =>
  v == null ? "" : Array.isArray(v) ? v.map(labelOf).filter(Boolean).join(", ") : typeof v === "object" ? String(v.label ?? v.title ?? v.name ?? "") : String(v);
const idsOf = (v: any): string[] => asArray(v).map((x) => String(x?.id ?? x)).filter(Boolean);
const day = (v: any) => String(v ?? "").slice(0, 10);
const num = (v: any) => (typeof v === "number" ? v : Number(v) || 0);
const shortStudio = (s: string) => s.replace(/^b\.cycle - /i, "");

function useLoadAll(r: { hasNextPage?: boolean; isFetchingNextPage?: boolean; fetchNextPage: () => unknown }) {
  useEffect(() => {
    if (r.hasNextPage && !r.isFetchingNextPage) r.fetchNextPage();
  }, [r.hasNextPage, r.isFetchingNextPage]);
}
const rowsOf = (data: any): Rec[] => (data?.pages ?? []).flatMap((p: any) => p.items) as Rec[];
const toOpts = (d: any): Opt[] => (d?.pages ?? []).flatMap((p: any) => p.items).map((o: any) => ({ id: String(o.id), title: String(o.title ?? "") }));

const RESULT_STYLE: Record<string, string> = {
  Matched: "bg-green-100 text-green-800",
  "Hours differ": "bg-yellow-100 text-yellow-800",
  "Punch without review": "bg-red-100 text-red-800",
  "Review without punch": "bg-orange-100 text-orange-800",
  Ignored: "bg-gray-100 text-gray-700",
};

function Chip({ ok, children }: { ok: boolean; children: React.ReactNode }) {
  return (
    <span className={cn("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium", ok ? "bg-green-100 text-green-800" : "bg-amber-100 text-amber-800")}>
      {ok ? <CheckCircle2 className="h-3 w-3" /> : <Clock className="h-3 w-3" />}
      {children}
    </span>
  );
}

function Picker({ options, placeholder, onPick }: { options: Opt[]; placeholder: string; onPick: (id: string) => void }) {
  const [text, setText] = useState("");
  const [open, setOpen] = useState(false);
  const shown = options.filter((o) => o.title.toLowerCase().includes(text.toLowerCase())).slice(0, 8);
  return (
    <div className="relative w-72">
      <Input value={text} placeholder={placeholder} onFocus={() => setOpen(true)} onChange={(e) => { setText(e.target.value); setOpen(true); }} />
      {open && shown.length > 0 && (
        <div className="absolute z-20 mt-1 w-full rounded-md border bg-background shadow">
          {shown.map((o) => (
            <button key={o.id} className="block w-full px-3 py-1.5 text-left text-sm hover:bg-muted" onClick={() => { setOpen(false); setText(""); onPick(o.id); }}>
              {o.title}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function TextFix({ placeholder, label, onSave }: { placeholder: string; label: string; onSave: (v: string) => void }) {
  const [v, setV] = useState("");
  return (
    <div className="flex items-center gap-2">
      <Input className="w-44" value={v} placeholder={placeholder} onChange={(e) => setV(e.target.value)} />
      <Button size="sm" disabled={!v.trim()} onClick={() => onSave(v.trim())}>{label}</Button>
    </div>
  );
}

/* One tab per issue type. Instructions are shown on every tab. */
const TABS: { key: string; label: string; kind: "punch" | "item"; how: string }[] = [
  {
    key: "No employee",
    label: "No employee/instructor",
    kind: "punch",
    how: "The punch isn't linked to an employee or instructor, so it can't be paid. Search for the person (the name MTEK shows is on the card) and pick them. Nobody found? Add them to Employees first.",
  },
  {
    key: "No rate",
    label: "No rate",
    kind: "punch",
    how: "No rate record is attached to the punch. Pick the person's rate for this shift type (matching names are listed first). If it doesn't exist, add it in Rates (HR) first, then come back and pick it.",
  },
  {
    key: "$0 wages",
    label: "$0 wages",
    kind: "punch",
    how: "The punch pays $0: either the attached rate is $0 or the hours are 0. Pick the correct rate below, or fix the clock out time if the hours are wrong. If the rate record itself is $0, correct it in Rates (HR).",
  },
  {
    key: "No Desjardins ID",
    label: "No Desjardins ID",
    kind: "punch",
    how: "The employee has no Desjardins ID. Type it below: it is saved on the employee record, so it fixes all of their punches. (A punch linked to an instructor needs the ID on the instructor record.)",
  },
  {
    key: "No clock out",
    label: "No clock out",
    kind: "punch",
    how: "The person never clocked out. Enter the clock-out time (HH:mm) below AND clock them out in MTEK so both agree. Wrong shift or a mistake? Set the punch to REMOVE / DO NOT PAY instead (staff will not be paid).",
  },
  {
    key: "Hours differ",
    label: "Hours differ",
    kind: "item",
    how: "The punch hours in MTEK don't match the review/coaching record in HR - Instructors. Decide which is right: if the review hours are wrong, fix the Mentor Review in HR - Instructors; if the punch is wrong, correct the shift in MTEK, re-run that studio's fetch, then Run check again.",
  },
  {
    key: "Punch without review",
    label: "Punch without review",
    kind: "item",
    how: "A punch exists but there is no review, coaching session or evaluation for that person on that date. Check name and date: add the missing review/coaching/eval in HR - Instructors, or if the punch is a mistake use the button to set it to REMOVE / DO NOT PAY (staff will not be paid). Then Run check again.",
  },
];

function IssueTabs({ start, end, items, hasRun }: { start: string; end: string; items: Rec[]; hasRun: boolean }) {
  const from = useMemo(() => {
    const d = new Date(`${start}T12:00:00Z`);
    d.setUTCDate(d.getUTCDate() - 1);
    return d.toISOString();
  }, [start]);
  const punchesQ = useRecords({ from: ds.punch, select: punchSel, count: 200, where: q.and(q.date("date").gte(from), q.text("rateType").isOneOf(TYPES)), orderBy: q.asc("date") });
  const empOptsQ = useLinkedRecords({ from: ds.punch, select: punchSel, field: "employee", sortOrder: "ASC", count: 1000 });
  const rateOptsQ = useLinkedRecords({ from: ds.punch, select: punchSel, field: "rate", sortOrder: "ASC", count: 1000 });
  useLoadAll(punchesQ as any);
  useLoadAll(empOptsQ as any);
  useLoadAll(rateOptsQ as any);
  const empOpts = useMemo(() => toOpts(empOptsQ.data), [empOptsQ.data]);
  const rateOpts = useMemo(() => toOpts(rateOptsQ.data), [rateOptsQ.data]);

  const punches = useMemo(
    () => rowsOf(punchesQ.data).filter((r) => day(r.fields.date) >= start && day(r.fields.date) <= end && !labelOf(r.fields.remove)),
    [punchesQ.data, start, end],
  );
  const issueRows = useMemo(
    () => punches.map((r) => ({ r, issues: labelOf(r.fields.issues).split(",").map((s) => s.trim()).filter(Boolean) })).filter((x) => x.issues.length),
    [punches],
  );

  const countFor = (key: string, kind: string) =>
    kind === "punch" ? issueRows.filter((x) => x.issues.includes(key)).length : items.filter((i) => labelOf(i.fields.result) === key).length;

  const [tab, setTab] = useState<string>("");
  const firstWithIssues = TABS.find((t) => countFor(t.key, t.kind) > 0)?.key;
  const active = tab || firstWithIssues || TABS[0].key;
  const current = TABS.find((t) => t.key === active);

  const upd = useRecordUpdate({
    from: ds.punch,
    fields: punchUpd,
    onSuccess: async () => { await punchesQ.refetch(); toast.success("Saved"); },
    onError: (e: any) => toast.error(e?.message ?? "Could not save"),
  });
  const empUpd = useRecordUpdate({
    from: ds.employee,
    fields: employeeUpd,
    onSuccess: async () => { await punchesQ.refetch(); toast.success("Desjardins ID saved on the employee"); },
    onError: (e: any) => toast.error(e?.message ?? "Could not save"),
  });
  const removePunch = (id: string, name: string) => {
    if (window.confirm(`Set ${name || "this punch"} to REMOVE / DO NOT PAY? Staff will NOT be paid for it.`)) {
      upd.mutate({ recordId: id, fields: { remove: "REMOVE/DO NOT PAY" } });
    }
  };

  const ratesFor = (name: string, type: string) => {
    const n = name.toLowerCase();
    const own = rateOpts.filter((o) => n && o.title.toLowerCase().includes(n));
    const pool = own.length ? own : rateOpts;
    return [...pool].sort((a, b) => Number(b.title.includes(type)) - Number(a.title.includes(type)));
  };

  const list = current?.kind === "punch" ? issueRows.filter((x) => x.issues.includes(current.key)) : items.filter((i) => labelOf(i.fields.result) === current?.key);

  return (
    <div className="space-y-4">
      <div className="text-sm font-medium">Step 3: fix the issues ({punches.length} punches this week)</div>
      <Tabs value={active} onValueChange={setTab}>
        <TabsList className="h-auto flex-wrap justify-start">
          {TABS.map((t) => {
            const c = countFor(t.key, t.kind);
            return (
              <TabsTrigger key={t.key} value={t.key} className="gap-2">
                {t.label}
                <span className={cn("rounded-full px-1.5 text-xs font-semibold", c ? "bg-amber-100 text-amber-800" : "bg-green-100 text-green-800")}>{c}</span>
              </TabsTrigger>
            );
          })}
          <TabsTrigger value="all">All results</TabsTrigger>
        </TabsList>
      </Tabs>

      {punchesQ.isLoading && <Loader2 className="h-5 w-5 animate-spin" />}

      {active === "all" ? (
        <div className="rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Employee</TableHead>
                <TableHead>Type</TableHead>
                <TableHead className="text-right">Punch h</TableHead>
                <TableHead className="text-right">Review h</TableHead>
                <TableHead>Result</TableHead>
                <TableHead>Detail</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7} className="py-8 text-center text-muted-foreground">No results yet - run the check.</TableCell>
                </TableRow>
              )}
              {items.map((i) => (
                <TableRow key={i.id}>
                  <TableCell>{day(i.fields.date)}</TableCell>
                  <TableCell>{labelOf(i.fields.employee)}</TableCell>
                  <TableCell>{labelOf(i.fields.type)}</TableCell>
                  <TableCell className="text-right">{i.fields.punchHours ?? ""}</TableCell>
                  <TableCell className="text-right">{i.fields.reviewHours ?? ""}</TableCell>
                  <TableCell>
                    <span className={cn("rounded-full px-2 py-0.5 text-xs font-medium", RESULT_STYLE[labelOf(i.fields.result)])}>{labelOf(i.fields.result)}</span>
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">{labelOf(i.fields.detail)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      ) : (
        current && (
          <div className="space-y-3">
            <div className="flex gap-2 rounded-lg border border-blue-200 bg-blue-50 p-3 text-sm text-blue-900">
              <Info className="mt-0.5 h-4 w-4 shrink-0" />
              <div>
                <div className="font-medium">How to fix: {current.label}</div>
                <div>{current.how}</div>
              </div>
            </div>
            {current.kind === "item" && !hasRun ? (
              <p className="text-sm text-muted-foreground">Run the check (Step 2) to see these.</p>
            ) : list.length === 0 ? (
              <p className="flex items-center gap-1 text-sm font-medium text-green-700"><CheckCircle2 className="h-4 w-4" /> Nothing to fix here.</p>
            ) : current.kind === "item" ? (
              (list as Rec[]).map((i) => {
                const punchId = idsOf(i.fields.punch)[0];
                return (
                  <Card key={i.id}>
                    <CardContent className="flex flex-wrap items-center justify-between gap-3 p-4">
                      <div>
                        <div className="font-medium">{labelOf(i.fields.employee)} - {labelOf(i.fields.type)}</div>
                        <div className="text-sm text-muted-foreground">
                          {day(i.fields.date)} · punch {i.fields.punchHours ?? "-"} h · review {i.fields.reviewHours ?? "-"} h
                        </div>
                        {labelOf(i.fields.detail) && <div className="text-sm text-amber-700">{labelOf(i.fields.detail)}</div>}
                      </div>
                      {current.key === "Punch without review" && punchId && (
                        <Button size="sm" variant="outline" onClick={() => removePunch(punchId, labelOf(i.fields.employee))}>
                          Set to REMOVE / DO NOT PAY
                        </Button>
                      )}
                    </CardContent>
                  </Card>
                );
              })
            ) : (
              (list as { r: Rec; issues: string[] }[]).map(({ r, issues }) => {
                const f = r.fields;
                const name = labelOf(f.name);
                const type = labelOf(f.rateType);
                const employeeId = idsOf(f.employee)[0];
                return (
                  <Card key={r.id}>
                    <CardContent className="space-y-3 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div>
                          <div className="font-medium">{name || "(no name)"} - {type}</div>
                          <div className="text-sm text-muted-foreground">
                            {day(f.date)} · {labelOf(f.timeIn) || "?"} to {labelOf(f.timeOut) || "?"} · {f.hours ?? 0} h · rate {labelOf(f.hourly) || "none"}
                          </div>
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {issues.map((i) => (
                            <span key={i} className={cn("rounded-full px-2 py-0.5 text-xs font-medium", i === current.key ? "bg-amber-100 text-amber-800" : "bg-gray-100 text-gray-600")}>{i}</span>
                          ))}
                        </div>
                      </div>
                      {current.key === "No employee" && (
                        <Picker options={empOpts} placeholder={`Search employee (MTEK: ${name || "?"})`} onPick={(id) => upd.mutate({ recordId: r.id, fields: { employee: [id] } })} />
                      )}
                      {(current.key === "No rate" || current.key === "$0 wages") && (
                        <Picker options={ratesFor(name, type)} placeholder="Search rate" onPick={(id) => upd.mutate({ recordId: r.id, fields: { rate: [id] } })} />
                      )}
                      {current.key === "No Desjardins ID" &&
                        (employeeId ? (
                          <TextFix placeholder="Desjardins ID" label="Save ID" onSave={(v) => empUpd.mutate({ recordId: employeeId, fields: { desjardins: v } })} />
                        ) : (
                          <span className="text-sm text-amber-700">
                            {idsOf(f.instructor)[0] ? "Linked to an instructor, not an employee - add the ID on the instructor record." : "Select the employee first (see the No employee tab)."}
                          </span>
                        ))}
                      {(current.key === "No clock out" || current.key === "$0 wages") && (
                        <div className="flex flex-wrap items-center gap-3">
                          <TextFix placeholder="Clock out e.g. 17:30" label="Save clock out" onSave={(v) => upd.mutate({ recordId: r.id, fields: { timeOut: v } })} />
                          {current.key === "No clock out" && (
                            <Button size="sm" variant="outline" onClick={() => removePunch(r.id, name)}>Set to REMOVE / DO NOT PAY</Button>
                          )}
                        </div>
                      )}
                    </CardContent>
                  </Card>
                );
              })
            )}
          </div>
        )
      )}
    </div>
  );
}

export default function Block() {
  const [weekId, setWeekId] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  const weeksQ = useRecords({ from: ds.budgetWeek, select: weekSel, count: 12, orderBy: q.desc("start") });
  const studiosQ = useRecords({ from: ds.studioWeek, select: studioSel, count: 200 });
  const runsQ = useRecords({ from: ds.runWeek, select: runSel, count: 100 });
  const itemsQ = useRecords({ from: ds.runItem, select: itemSel, count: 200, orderBy: q.desc("date") });
  useLoadAll(studiosQ as any);
  useLoadAll(itemsQ as any);

  const weeks = useMemo(() => rowsOf(weeksQ.data), [weeksQ.data]);
  const studioRows = useMemo(() => rowsOf(studiosQ.data), [studiosQ.data]);
  const runs = useMemo(() => rowsOf(runsQ.data), [runsQ.data]);
  const items = useMemo(() => rowsOf(itemsQ.data), [itemsQ.data]);

  // Every studio that has ever had a Budget week - Studio row is expected in every week (b.home is not required).
  const studioNames = useMemo(() => {
    const names = new Set<string>();
    studioRows.forEach((r) => {
      const n = labelOf(r.fields.studio);
      if (n && !/b\.home/i.test(n)) names.add(n);
    });
    return [...names].sort();
  }, [studioRows]);

  const studioStatus = (wid: string) => {
    const rows = studioRows.filter((r) => idsOf(r.fields.week).includes(wid));
    return studioNames.map((name) => {
      const rec = rows.find((r) => labelOf(r.fields.studio) === name);
      const done = Boolean(rec && labelOf(rec.fields.completed));
      return { name, done, exists: Boolean(rec), overall: rec ? labelOf(rec.fields.overall) : "" };
    });
  };

  const runFor = (wid: string) => runs.find((r) => idsOf(r.fields.week).includes(wid));

  const createRun = useRecordCreate({
    from: ds.runWeek,
    fields: runCreate,
    onSuccess: async () => {
      await runsQ.refetch();
      toast.success("Run created");
    },
    onError: (e: Error) => toast.error(e.message),
  });

  const trigger = async (runId: string) => {
    setRunning(true);
    try {
      await fetch(`${RUN_CHECK_WEBHOOK}?recordId=${encodeURIComponent(runId)}`, { method: "POST", mode: "no-cors" });
      toast.success("Check started - refresh in about a minute.");
      setTimeout(() => {
        runsQ.refetch();
        itemsQ.refetch();
      }, 60000);
    } catch (e: any) {
      toast.error(e?.message ?? "Could not start the check");
    } finally {
      setRunning(false);
    }
  };

  const loading = weeksQ.isLoading || studiosQ.isLoading || runsQ.isLoading;

  /* ---------------- week view ---------------- */
  if (weekId) {
    const week = weeks.find((w) => w.id === weekId);
    const status = studioStatus(weekId);
    const allDone = status.length > 0 && status.every((s) => s.done);
    const run = runFor(weekId);
    const weekItems = run ? items.filter((i) => idsOf(i.fields.run).includes(run.id)) : [];
    const f = run?.fields ?? {};

    return (
      <div className="mx-auto max-w-6xl space-y-6 p-4">
        <button className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground" onClick={() => setWeekId(null)}>
          <ArrowLeft className="h-4 w-4" /> All weeks
        </button>
        <div>
          <h1 className="text-2xl font-semibold">
            {day(week?.fields.start)} → {day(week?.fields.end)}
          </h1>
          <p className="text-sm text-muted-foreground">Coaching 1-on-1, Class Review and Instructor Eval hours vs. the reviews pushed to MTEK.</p>
        </div>

        <Card>
          <CardContent className="space-y-3 p-4">
            <div className="text-sm font-medium">Step 1: every studio has fetched its staff hours</div>
            <div className="flex flex-wrap gap-2">
              {status.map((s) => (
                <Chip key={s.name} ok={s.done}>
                  {shortStudio(s.name)}: {s.done ? "fetched" : s.exists ? s.overall || "in progress" : "not started"}
                </Chip>
              ))}
            </div>
            {!allDone && <p className="text-sm text-amber-700">The check unlocks once all studios are done.</p>}
          </CardContent>
        </Card>

        <Card>
          <CardContent className="space-y-4 p-4">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium">Step 2: run the check (matches the punch hours to the reviews pushed to MTEK)</div>
              <div className="flex items-center gap-2">
                {run && <Badge variant="outline">{labelOf(f.status) || "Not started"}</Badge>}
                {run ? (
                  <Button size="sm" disabled={!allDone || running} onClick={() => trigger(run.id)}>
                    {running ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : <Play className="mr-1 h-4 w-4" />}
                    Run check
                  </Button>
                ) : (
                  <Button
                    size="sm"
                    disabled={!allDone || createRun.isPending}
                    onClick={() => createRun.mutate({ name: `${day(week?.fields.start)} - ${day(week?.fields.end)}`, week: [weekId], status: "Not started" })}
                  >
                    Create run
                  </Button>
                )}
                <Button size="sm" variant="ghost" onClick={() => { runsQ.refetch(); itemsQ.refetch(); }}>
                  <RefreshCw className="h-4 w-4" />
                </Button>
              </div>
            </div>
            {run && (
              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Matched</div>
                  <div className="text-2xl font-semibold">{num(f.matched)}</div>
                </div>
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Hours differ</div>
                  <div className="text-2xl font-semibold">{num(f.hoursDiffer)}</div>
                  <span className={cn("text-xs font-medium", labelOf(f.hoursDifferCheck) === "ALL GOOD" ? "text-green-700" : "text-amber-700")}>{labelOf(f.hoursDifferCheck)}</span>
                </div>
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Punch without review</div>
                  <div className="text-2xl font-semibold">{num(f.punchNoReview)}</div>
                  <span className={cn("text-xs font-medium", labelOf(f.punchNoReviewCheck) === "ALL GOOD" ? "text-green-700" : "text-amber-700")}>{labelOf(f.punchNoReviewCheck)}</span>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <IssueTabs start={day(week?.fields.start)} end={day(week?.fields.end)} items={weekItems} hasRun={Boolean(run)} />
      </div>
    );
  }

  /* ---------------- list view ---------------- */
  return (
    <div className="mx-auto max-w-6xl space-y-4 p-4">
      <div>
        <h1 className="text-2xl font-semibold">Coaching & Review Hours</h1>
        <p className="text-sm text-muted-foreground">Open a week once every studio has fetched its staff hours.</p>
      </div>
      {loading && <Loader2 className="h-5 w-5 animate-spin" />}
      {weeks.map((w) => {
        const status = studioStatus(w.id);
        const done = status.filter((s) => s.done).length;
        const allDone = status.length > 0 && done === status.length;
        const run = runFor(w.id);
        return (
          <Card key={w.id}>
            <CardContent className="flex flex-wrap items-center justify-between gap-4 p-4">
              <div className="min-w-[200px]">
                <div className="text-lg font-semibold">{day(w.fields.start)}</div>
                <div className="text-sm text-muted-foreground">{day(w.fields.end)}</div>
              </div>
              <div className="flex flex-1 flex-wrap gap-2">
                {status.map((s) => (
                  <Chip key={s.name} ok={s.done}>
                    {shortStudio(s.name)}
                  </Chip>
                ))}
              </div>
              <div className="flex items-center gap-3">
                <span className="text-sm text-muted-foreground">{allDone ? "All studios fetched" : `${done}/${status.length} studios`}</span>
                {run && <Badge variant="outline">{labelOf(run.fields.status)}</Badge>}
                <Button onClick={() => setWeekId(w.id)}>Open</Button>
              </div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
