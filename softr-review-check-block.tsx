/*
 * Softr "Review Check (Budget)" vibe-coding block.
 *
 * Coaching 1-on-1 (HQ / SPINCO HQ), Class Review and Instructor Eval hours,
 * checked per Budget week. Uses the SAME Budget week records as the studio
 * staff-hours flow. Read-only for Budget week / Budget week - Studio.
 *
 *   List view ... Budget weeks (newest first) with one chip per studio showing
 *                 whether that studio has finished its staff-hours fetch
 *                 ("Completed studio"). The check can only run once EVERY studio
 *                 is done. No "Add record".
 *   Week view ... run status, the three issue checks, matched count, the items
 *                 table (Issues / All) and the Run check button.
 *
 * The check itself runs in GitHub Actions (scripts/hr-review-check.mjs), started
 * through a Make webhook. Items are rebuilt on every run.
 *
 * Data sources (HR base appiwfeujJzUZPPBx):
 *   budgetWeek  = Budget week
 *   studioWeek  = Budget week - Studio
 *   runWeek     = Review Check - Week
 *   runItem     = Review Check - Item
 */

import { useEffect, useMemo, useState } from "react";
import { datasource, q, useRecordCreate, useRecords } from "@/lib/datasource";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import { ArrowLeft, CheckCircle2, Clock, Loader2, Play, RefreshCw } from "lucide-react";
import { toast } from "sonner";

// Make webhook -> GitHub repository_dispatch "airtable-hr-review-check". Set after the scenario exists.
const RUN_CHECK_WEBHOOK = "https://hook.us2.make.com/7o12uchu91c7sehjy0qhy9n5eq3c15w9";

const ds = datasource.define({
  budgetWeek: "ac6cd17d-4051-4139-b97e-1318975c30e4",
  studioWeek: "7596a7af-3f34-4fdc-93cf-ab74fe1f9f00",
  runWeek: "5cb24d23-a1be-4400-95ec-d6db1fd9c74f",
  runItem: "0446af12-2cd9-4088-9d17-0be1b85ac4d9",
});

const weekSel = q.select({ start: "Start Date", end: "End Date" });
const studioSel = q.select({
  studio: "Studio",
  week: "Budget week",
  completed: "Completed studio",
  overall: "Overall Status",
  punches: "Time punches count",
});
const runSel = q.select({
  name: "Name",
  week: "Budget week",
  status: "Overall Status",
  notes: "Notes",
  punchNoReview: "Punch without review",
  reviewNoPunch: "Review without punch",
  hoursDiffer: "Hours differ",
  matched: "Matched",
  punchNoReviewCheck: "Punch without review Check",
  reviewNoPunchCheck: "Review without punch Check",
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
  detail: "Detail",
});

type Rec = { id: string; fields: Record<string, any> };
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

export default function Block() {
  const [weekId, setWeekId] = useState<string | null>(null);
  const [tab, setTab] = useState<"issues" | "all">("issues");
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

  // Every studio that has ever had a Budget week - Studio row is expected in every week.
  const studioNames = useMemo(() => {
    const names = new Set<string>();
    studioRows.forEach((r) => labelOf(r.fields.studio) && names.add(labelOf(r.fields.studio)));
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
    if (!RUN_CHECK_WEBHOOK) {
      toast.error("The Run check webhook isn't connected yet.");
      return;
    }
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
    const shown = tab === "issues" ? weekItems.filter((i) => !["Matched", "Ignored"].includes(labelOf(i.fields.result))) : weekItems;
    const f = run?.fields ?? {};
    const checks = [
      ["Punch without review", f.punchNoReview, f.punchNoReviewCheck],
      ["Review without punch", f.reviewNoPunch, f.reviewNoPunchCheck],
      ["Hours differ", f.hoursDiffer, f.hoursDifferCheck],
    ] as const;

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
              <div className="text-sm font-medium">Step 2: run the check</div>
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
              <div className="grid gap-3 sm:grid-cols-4">
                {checks.map(([label, count, check]) => (
                  <div key={label} className="rounded-lg border p-3">
                    <div className="text-xs text-muted-foreground">{label}</div>
                    <div className="text-2xl font-semibold">{num(count)}</div>
                    <span className={cn("text-xs font-medium", labelOf(check) === "ALL GOOD" ? "text-green-700" : "text-amber-700")}>{labelOf(check)}</span>
                  </div>
                ))}
                <div className="rounded-lg border p-3">
                  <div className="text-xs text-muted-foreground">Matched</div>
                  <div className="text-2xl font-semibold">{num(f.matched)}</div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {run && (
          <div className="space-y-3">
            <Tabs value={tab} onValueChange={(v) => setTab(v as any)}>
              <TabsList>
                <TabsTrigger value="issues">Issues</TabsTrigger>
                <TabsTrigger value="all">All ({weekItems.length})</TabsTrigger>
              </TabsList>
            </Tabs>
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
                  {shown.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={7} className="py-8 text-center text-muted-foreground">
                        {weekItems.length === 0 ? "No results yet - run the check." : "No issues."}
                      </TableCell>
                    </TableRow>
                  )}
                  {shown.map((i) => (
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
          </div>
        )}
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
