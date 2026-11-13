/*
 * Softr "Review Check (Budget)" vibe-coding block.
 *
 * Coaching 1-on-1 (HQ / SPINCO HQ), Class Review and Instructor Eval hours, checked per
 * Budget week. Uses the SAME Budget week records as the studio staff-hours flow.
 * The per-studio fetch ignores these four shift types; this page fetches them itself.
 *
 *   List view ... Budget weeks (newest first) with fetch / check / Program clock in status.
 *   Week view ... Step 1 Fetch hours (all studios, these four shift types only),
 *                 Step 2 Run check (punch hours vs. the reviews pushed to MTEK),
 *                 Step 3 one tab per issue type with instructions and in-place fixes,
 *                 Step 4 Confirm to payroll (sets Budget week "Program clock in status").
 *
 * Payroll issue tabs are LIVE (Time Punches `Payroll issues` formula, same as the studio flow);
 * "Hours differ" / "Punch without review" come from the last Run check. A review that was never
 * pushed to MTEK, or has no punch, is NOT an issue.
 *
 * Data sources (HR base appiwfeujJzUZPPBx): Budget week, Review Check - Week,
 * Review Check - Item, Time Punches, Employees.
 */

import { useEffect, useMemo, useState } from "react";
import { datasource, q, useLinkedRecords, useRecordCreate, useRecordUpdate, useRecords } from "@/lib/datasource";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import { ArrowLeft, CheckCircle2, Download, Info, Loader2, Play, RefreshCw } from "lucide-react";
import { toast } from "sonner";

// Make webhook -> GitHub repository_dispatch "airtable-hr-review-check".
const RUN_CHECK_WEBHOOK = "https://hook.us2.make.com/7o12uchu91c7sehjy0qhy9n5eq3c15w9";
// Make webhook -> GitHub repository_dispatch "airtable-hr-review-fetch" (Step 1).
const FETCH_WEBHOOK = "https://hook.us2.make.com/s9cn8uxzj5x3tq0s5kgrif49r2ug7vlt";

const ds = datasource.define({
  budgetWeek: "ac6cd17d-4051-4139-b97e-1318975c30e4",
  runWeek: "5cb24d23-a1be-4400-95ec-d6db1fd9c74f",
  runItem: "0446af12-2cd9-4088-9d17-0be1b85ac4d9",
  punch: "3d1b6a52-8f0e-4c7d-9a41-2b7e5c6d8f10", // Time Punches
  employee: "9e4c7a13-5b2d-4f86-8a3c-1d0e6b7f2a59", // Employees
  rate: "fd37266e-971a-4b88-822e-ea715c405c59", // Rates
});

const TYPES = ["Coaching 1-on-1 (HQ)", "Coaching 1-on-1 (SPINCO HQ)", "Class Review", "Instructor Eval"];

const weekSel = q.select({ start: "Start Date", end: "End Date", progress: "Program clock in status" });
const weekUpd = q.select({ progress: "Program clock in status" });
const runSel = q.select({
  name: "Name",
  week: "Budget week",
  status: "Overall Status",
  fetchStatus: "Fetch Status",
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
  ok: "Review not required (OK)",
});
const punchUpd = q.select({ employee: "Employee", rate: "Rate", timeOut: "Time Out", remove: "REMOVE/DO NOT PAY?", ok: "Review not required (OK)" });
const itemUpd = q.select({ result: "Result", detail: "Detail" });
const employeeUpd = q.select({ desjardins: "DESJARDINS ID" });
const rateSel = q.select({
  kind: "Employee or Instructor?",
  employee: "Employee",
  instructor: "Instructor",
  shiftType: "Shift Type",
  rate: "Rate",
  ftPay: "Include in FT Pay",
  minHours: "Min hours",
  ftOrAt: "FT Rate or AT Rate",
});

type Rec = { id: string; fields: Record<string, any> };
type Opt = { id: string; title: string };
const asArray = (v: unknown): any[] => (v == null ? [] : Array.isArray(v) ? v : [v]);
const labelOf = (v: any): string =>
  v == null ? "" : Array.isArray(v) ? v.map(labelOf).filter(Boolean).join(", ") : typeof v === "object" ? String(v.label ?? v.title ?? v.name ?? "") : String(v);
const idsOf = (v: any): string[] => asArray(v).map((x) => String(x?.id ?? x)).filter(Boolean);
const day = (v: any) => String(v ?? "").slice(0, 10);
const num = (v: any) => (typeof v === "number" ? v : Number(v) || 0);

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

function Picker({ options, placeholder, onPick }: { options: Opt[]; placeholder: string; onPick: (id: string) => void }) {
  const [text, setText] = useState("");
  const [open, setOpen] = useState(false);
  const shown = options.filter((o) => o.title.toLowerCase().includes(text.toLowerCase())).slice(0, 8);
  return (
    <div className="relative w-56">
      <Input className="h-8" value={text} placeholder={placeholder} onFocus={() => setOpen(true)} onChange={(e) => { setText(e.target.value); setOpen(true); }} />
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
      <Input className="h-8 w-36" value={v} placeholder={placeholder} onChange={(e) => setV(e.target.value)} />
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
    how: "No rate record is attached to the punch. Pick the person's rate for this shift type (matching names are listed first). If it doesn't exist yet, press Create rate to add it; it is attached to the punch for you (if it doesn't attach, pick it from the list).",
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
    how: "The punch hours in MTEK don't match the review/coaching record in HR - Instructors. Decide which is right: if the review hours are wrong, fix the Mentor Review in HR - Instructors; if the punch is wrong, correct the shift in MTEK, fetch again (Step 1), then Run check again.",
  },
  {
    key: "Punch without review",
    label: "Punch without review",
    kind: "item",
    how: "A punch exists but there is no review, coaching session or evaluation for that person on that date. Check name and date: add the missing review/coaching/eval in HR - Instructors, or if the punch is a mistake press OK if the punch is fine without a review (it is then accepted and no longer flagged), or set it to REMOVE / DO NOT PAY if it is a mistake (staff will not be paid).",
  },
];

const TH = "border-b px-2 py-1.5 text-left text-xs font-medium text-muted-foreground";
const TD = "border-b px-2 py-1.5 align-middle";

const SHIFT_TYPES = ["MC/SC", "Bike Rental", "Bike Rental TD", "EE", "b.home", "Filming b.home", "Team Lead", "Master Trainer (per location)", "Coaching 1-on-1 (HQ)", "Coaching 1-on-1 (SPINCO HQ)", "Evaluation", "Reception & Clean", "Admin Hours", "Studio Maintenance", "MC/SC Training", "EE Training", "Marketing Event - HQ", "Marketing Event - Studio", "Class Review", "Instructor Eval"];
const selectCls = "h-8 w-full rounded-md border bg-background px-2 text-sm";

/* "Add a rate" - same fields and wording as the Airtable form (the Airtable page itself can't be embedded: it refuses to load inside another site). */
function RateModal({ punch, onClose, onCreated }: { punch: Rec; onClose: () => void; onCreated: (rateId: string | null) => void }) {
  const f = punch.fields;
  const punchEmployee = idsOf(f.employee)[0] ?? "";
  const punchInstructor = idsOf(f.instructor)[0] ?? "";
  const [kind, setKind] = useState(punchInstructor && !punchEmployee ? "Instructor" : "Employee");
  const [employeeId, setEmployeeId] = useState(punchEmployee);
  const [instructorId, setInstructorId] = useState(punchInstructor);
  const [shiftType, setShiftType] = useState(SHIFT_TYPES.includes(labelOf(f.rateType)) ? labelOf(f.rateType) : "");
  const [rate, setRate] = useState("");
  const [ftPay, setFtPay] = useState("");
  const [minHours, setMinHours] = useState("");
  const [ftOrAt, setFtOrAt] = useState("");

  const empQ = useLinkedRecords({ from: ds.rate, select: rateSel, field: "employee", sortOrder: "ASC", count: 1000 });
  const insQ = useLinkedRecords({ from: ds.rate, select: rateSel, field: "instructor", sortOrder: "ASC", count: 1000 });
  useLoadAll(empQ as any);
  useLoadAll(insQ as any);
  const empOpts = useMemo(() => toOpts(empQ.data), [empQ.data]);
  const insOpts = useMemo(() => toOpts(insQ.data), [insQ.data]);
  const chosenId = kind === "Employee" ? employeeId : instructorId;
  const chosen = (kind === "Employee" ? empOpts : insOpts).find((o) => o.id === chosenId)?.title;

  const create = useRecordCreate({
    from: ds.rate,
    fields: rateSel,
    onSuccess: (rec: any) => {
      toast.success("Rate created");
      onCreated(String(rec?.id ?? rec?.record?.id ?? rec?.recordId ?? "") || null);
    },
    onError: (e: any) => toast.error(e?.message ?? "Could not create the rate"),
  });
  const valid = Boolean(chosenId && shiftType);
  const submit = () => {
    const fields: Record<string, any> = { kind, shiftType };
    if (kind === "Employee") fields.employee = [employeeId];
    else fields.instructor = [instructorId];
    if (rate) fields.rate = Number(rate);
    if (ftPay) fields.ftPay = ftPay;
    if (minHours) fields.minHours = Number(minHours);
    if (ftOrAt) fields.ftOrAt = ftOrAt;
    create.mutate(fields);
  };
  const Label = ({ children, hint }: { children: React.ReactNode; hint?: string }) => (
    <div className="mb-1">
      <div className="text-sm font-medium">{children}</div>
      {hint && <div className="whitespace-pre-line text-xs text-muted-foreground">{hint}</div>}
    </div>
  );

  return (
    <Dialog open onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-h-[90vh] max-w-lg overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Add a rate</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <div>
            <Label>Employee or Instructor? *</Label>
            <select className={selectCls} value={kind} onChange={(e) => setKind(e.target.value)}>
              <option value="Employee">Employee</option>
              <option value="Instructor">Instructor</option>
            </select>
          </div>
          <div>
            <Label>{kind} *</Label>
            {chosen && <div className="mb-1 text-sm text-green-700">Selected: {chosen}</div>}
            <Picker
              options={kind === "Employee" ? empOpts : insOpts}
              placeholder={`Search ${kind.toLowerCase()} (MTEK: ${labelOf(f.name) || "?"})`}
              onPick={(id) => (kind === "Employee" ? setEmployeeId(id) : setInstructorId(id))}
            />
          </div>
          <div>
            <Label hint="The same as what is on MTEK">Shift Type *</Label>
            <select className={selectCls} value={shiftType} onChange={(e) => setShiftType(e.target.value)}>
              <option value="">Select...</option>
              {SHIFT_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <Label hint="$ amount per hour">Rate</Label>
            <Input className="h-8" type="number" step="0.01" value={rate} onChange={(e) => setRate(e.target.value)} />
          </div>
          <div>
            <Label hint={"Only change this if the employee is Full Time.\nNo: All of their hours will be paid regardless.\nYes - with minimum: If they are Full Time and have a minimum hours to do per week to be paid.\nYes - ALL: If they are Full Time and all of their hours should NOT be paid for this shift type."}>Include in FT Pay</Label>
            <select className={selectCls} value={ftPay} onChange={(e) => setFtPay(e.target.value)}>
              <option value="">Select...</option>
              <option value="No">No</option>
              <option value="Yes - with minimum">Yes - with minimum</option>
              <option value="Yes - ALL">Yes - ALL</option>
            </select>
          </div>
          <div>
            <Label hint="Minimum hours an employee has to work to get paid extra above this.">Min hours</Label>
            <Input className="h-8" type="number" step="0.25" value={minHours} onChange={(e) => setMinHours(e.target.value)} />
          </div>
          <div>
            <Label hint={'This is only changed from "Full Time" to "Airtable", only if the Full Time employee should be paid the hours above their minimum at the rate in Airtable rather than their Full Time rate'}>FT Rate or AT Rate</Label>
            <select className={selectCls} value={ftOrAt} onChange={(e) => setFtOrAt(e.target.value)}>
              <option value="">Select...</option>
              <option value="Full Time">Full Time</option>
              <option value="Airtable">Airtable</option>
            </select>
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={onClose}>Cancel</Button>
            <Button disabled={!valid || create.isPending} onClick={submit}>
              {create.isPending ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : null}
              Create rate
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

function IssueTabs({
  start,
  end,
  items,
  hasRun,
  fetched,
  progress,
  onProgress,
  onItemsChanged,
}: {
  start: string;
  end: string;
  items: Rec[];
  hasRun: boolean;
  fetched: boolean;
  progress: string;
  onProgress: (v: "Started" | "COMPLETE") => void;
  onItemsChanged: () => void;
}) {
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

  const allPunches = useMemo(() => rowsOf(punchesQ.data).filter((r) => day(r.fields.date) >= start && day(r.fields.date) <= end), [punchesQ.data, start, end]);
  const punches = useMemo(() => allPunches.filter((r) => !labelOf(r.fields.remove)), [allPunches]);
  const issueRows = useMemo(
    () => punches.map((r) => ({ r, issues: labelOf(r.fields.issues).split(",").map((s) => s.trim()).filter(Boolean) })).filter((x) => x.issues.length),
    [punches],
  );
  const itemByPunch = useMemo(() => {
    const m = new Map<string, Rec>();
    items.forEach((i) => idsOf(i.fields.punch).forEach((id) => m.set(id, i)));
    return m;
  }, [items]);

  const countFor = (key: string, kind: string) =>
    kind === "punch" ? issueRows.filter((x) => x.issues.includes(key)).length : items.filter((i) => labelOf(i.fields.result) === key).length;
  const totalIssues = TABS.reduce((n, t) => n + countFor(t.key, t.kind), 0);

  const [tab, setTab] = useState<string>("");
  const [rateFor, setRateFor] = useState<Rec | null>(null);
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
  const itemUpdate = useRecordUpdate({
    from: ds.runItem,
    fields: itemUpd,
    onSuccess: async () => { onItemsChanged(); },
    onError: (e: any) => toast.error(e?.message ?? "Could not save"),
  });
  // OK: accept a punch that has no review. Remembered on the punch (so a re-run keeps it) and on the item (so it clears now).
  const acceptPunch = (punchId: string, itemId: string) => {
    upd.mutate({ recordId: punchId, fields: { ok: true } });
    itemUpdate.mutate({ recordId: itemId, fields: { result: "Ignored", detail: "Accepted without review (OK)" } });
  };
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

  const punchRows = current?.kind === "punch" ? issueRows.filter((x) => x.issues.includes(current.key)) : [];
  const itemRows = current?.kind === "item" ? items.filter((i) => labelOf(i.fields.result) === current.key) : [];

  // Status of every punch for the All results tab: payroll issues first, else the match result.
  const statusOf = (r: Rec): { text: string; cls: string } => {
    if (labelOf(r.fields.remove)) return { text: "Removed / do not pay", cls: "bg-gray-100 text-gray-700" };
    const issues = labelOf(r.fields.issues);
    if (issues) return { text: issues, cls: "bg-amber-100 text-amber-800" };
    const it = itemByPunch.get(r.id);
    if (it) return { text: labelOf(it.fields.result), cls: RESULT_STYLE[labelOf(it.fields.result)] ?? "bg-gray-100 text-gray-700" };
    return { text: hasRun ? "Good" : "No payroll issues", cls: "bg-green-100 text-green-800" };
  };

  const confirmBlockers = [
    !fetched && "Fetch the hours (Step 1).",
    !hasRun && "Run the check (Step 2).",
    totalIssues > 0 && `${totalIssues} issue${totalIssues === 1 ? "" : "s"} left in Step 3.`,
  ].filter(Boolean) as string[];

  return (
    <div className="space-y-6">
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
            <TabsTrigger value="all" className="gap-2">
              All time punches
              <span className="rounded-full bg-gray-100 px-1.5 text-xs font-semibold text-gray-700">{allPunches.length}</span>
            </TabsTrigger>
          </TabsList>
        </Tabs>

        {punchesQ.isLoading && <Loader2 className="h-5 w-5 animate-spin" />}

        {active === "all" ? (
          <div className="rounded-lg border">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th className={TH}>Date</th>
                  <th className={TH}>Employee</th>
                  <th className={TH}>Type</th>
                  <th className={TH}>In - Out</th>
                  <th className={cn(TH, "text-right")}>Hours</th>
                  <th className={TH}>Rate</th>
                  <th className={TH}>Status</th>
                </tr>
              </thead>
              <tbody>
                {allPunches.length === 0 && (
                  <tr><td colSpan={7} className="px-2 py-6 text-center text-muted-foreground">No time punches of these types this week.</td></tr>
                )}
                {allPunches.map((r) => {
                  const st = statusOf(r);
                  return (
                    <tr key={r.id}>
                      <td className={TD}>{day(r.fields.date)}</td>
                      <td className={TD}>{labelOf(r.fields.name)}</td>
                      <td className={TD}>{labelOf(r.fields.rateType)}</td>
                      <td className={TD}>{labelOf(r.fields.timeIn) || "?"} - {labelOf(r.fields.timeOut) || "?"}</td>
                      <td className={cn(TD, "text-right")}>{r.fields.hours ?? ""}</td>
                      <td className={TD}>{labelOf(r.fields.hourly)}</td>
                      <td className={TD}><span className={cn("rounded-full px-2 py-0.5 text-xs font-medium", st.cls)}>{st.text}</span></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          current && (
            <div className="space-y-3">
              <div className="flex gap-2 rounded-lg border border-blue-200 bg-blue-50 p-3 text-sm text-blue-900">
                <Info className="mt-0.5 h-4 w-4 shrink-0" />
                <div>
                  <span className="font-medium">How to fix {current.label}: </span>
                  {current.how}
                </div>
              </div>
              {current.kind === "item" && !hasRun ? (
                <p className="text-sm text-muted-foreground">Run the check (Step 2) to see these.</p>
              ) : (current.kind === "punch" ? punchRows.length : itemRows.length) === 0 ? (
                <p className="flex items-center gap-1 text-sm font-medium text-green-700"><CheckCircle2 className="h-4 w-4" /> Nothing to fix here.</p>
              ) : current.kind === "item" ? (
                <div className="rounded-lg border">
                  <table className="w-full text-sm">
                    <thead>
                      <tr>
                        <th className={TH}>Date</th>
                        <th className={TH}>Employee</th>
                        <th className={TH}>Type</th>
                        <th className={cn(TH, "text-right")}>Punch h</th>
                        <th className={cn(TH, "text-right")}>Review h</th>
                        <th className={TH}>Detail</th>
                        <th className={TH}>Fix</th>
                      </tr>
                    </thead>
                    <tbody>
                      {itemRows.map((i) => {
                        const punchId = idsOf(i.fields.punch)[0];
                        return (
                          <tr key={i.id}>
                            <td className={TD}>{day(i.fields.date)}</td>
                            <td className={TD}>{labelOf(i.fields.employee)}</td>
                            <td className={TD}>{labelOf(i.fields.type)}</td>
                            <td className={cn(TD, "text-right")}>{i.fields.punchHours ?? "-"}</td>
                            <td className={cn(TD, "text-right")}>{i.fields.reviewHours ?? "-"}</td>
                            <td className={cn(TD, "text-muted-foreground")}>{labelOf(i.fields.detail)}</td>
                            <td className={TD}>
                              {current.key === "Punch without review" && punchId ? (
                                <div className="flex items-center gap-2">
                                  <Button size="sm" className="h-7" onClick={() => acceptPunch(punchId, i.id)}>
                                    OK
                                  </Button>
                                  <Button size="sm" variant="outline" className="h-7" onClick={() => removePunch(punchId, labelOf(i.fields.employee))}>
                                    REMOVE / DO NOT PAY
                                  </Button>
                                </div>
                              ) : (
                                <span className="text-xs text-muted-foreground">See instructions</span>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="rounded-lg border">
                  <table className="w-full text-sm">
                    <thead>
                      <tr>
                        <th className={TH}>Date</th>
                        <th className={TH}>Employee</th>
                        <th className={TH}>Type</th>
                        <th className={TH}>In - Out</th>
                        <th className={cn(TH, "text-right")}>Hours</th>
                        <th className={TH}>Rate</th>
                        <th className={TH}>Other issues</th>
                        <th className={TH}>Fix</th>
                      </tr>
                    </thead>
                    <tbody>
                      {punchRows.map(({ r, issues }) => {
                        const f = r.fields;
                        const name = labelOf(f.name);
                        const type = labelOf(f.rateType);
                        const employeeId = idsOf(f.employee)[0];
                        return (
                          <tr key={r.id}>
                            <td className={TD}>{day(f.date)}</td>
                            <td className={TD}>{name || "(no name)"}</td>
                            <td className={TD}>{type}</td>
                            <td className={TD}>{labelOf(f.timeIn) || "?"} - {labelOf(f.timeOut) || "?"}</td>
                            <td className={cn(TD, "text-right")}>{f.hours ?? 0}</td>
                            <td className={TD}>{labelOf(f.hourly) || "none"}</td>
                            <td className={cn(TD, "text-xs text-muted-foreground")}>{issues.filter((i) => i !== current.key).join(", ")}</td>
                            <td className={TD}>
                              {current.key === "No employee" && (
                                <Picker options={empOpts} placeholder={`Employee (MTEK: ${name || "?"})`} onPick={(id) => upd.mutate({ recordId: r.id, fields: { employee: [id] } })} />
                              )}
                              {(current.key === "No rate" || current.key === "$0 wages") && (
                                <div className="flex items-center gap-2">
                                  <Picker options={ratesFor(name, type)} placeholder="Search rate" onPick={(id) => upd.mutate({ recordId: r.id, fields: { rate: [id] } })} />
                                  {current.key === "No rate" && (
                                    <Button size="sm" variant="outline" className="h-7" onClick={() => setRateFor(r)}>Create rate</Button>
                                  )}
                                  {current.key === "$0 wages" && (
                                    <TextFix placeholder="Clock out 17:30" label="Save" onSave={(v) => upd.mutate({ recordId: r.id, fields: { timeOut: v } })} />
                                  )}
                                </div>
                              )}
                              {current.key === "No Desjardins ID" &&
                                (employeeId ? (
                                  <TextFix placeholder="Desjardins ID" label="Save ID" onSave={(v) => empUpd.mutate({ recordId: employeeId, fields: { desjardins: v } })} />
                                ) : (
                                  <span className="text-xs text-amber-700">
                                    {idsOf(f.instructor)[0] ? "Instructor punch: add the ID on the instructor record" : "Select the employee first"}
                                  </span>
                                ))}
                              {current.key === "No clock out" && (
                                <div className="flex items-center gap-2">
                                  <TextFix placeholder="Clock out 17:30" label="Save" onSave={(v) => upd.mutate({ recordId: r.id, fields: { timeOut: v } })} />
                                  <Button size="sm" variant="outline" className="h-7" onClick={() => removePunch(r.id, name)}>REMOVE / DO NOT PAY</Button>
                                </div>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )
        )}
      </div>

      <Card>
        <CardContent className="space-y-2 p-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-sm font-medium">Step 4: confirm to payroll</div>
            <div className="flex items-center gap-3">
              <Badge variant="outline">Program clock in status: {progress || "Not started"}</Badge>
              {progress === "COMPLETE" ? (
                <Button size="sm" variant="outline" onClick={() => onProgress("Started")}>Reopen</Button>
              ) : (
                <Button size="sm" disabled={confirmBlockers.length > 0} onClick={() => window.confirm("Confirm these Coaching / Class Review / Instructor Eval hours to payroll?") && onProgress("COMPLETE")}>
                  <CheckCircle2 className="mr-1 h-4 w-4" /> Confirm to payroll
                </Button>
              )}
            </div>
          </div>
          {progress === "COMPLETE" ? (
            <p className="text-sm text-green-700">Confirmed. This week's coaching, class review and instructor eval hours are marked COMPLETE on the Budget week.</p>
          ) : confirmBlockers.length > 0 ? (
            <ul className="list-disc pl-5 text-sm text-amber-700">
              {confirmBlockers.map((b) => <li key={b}>{b}</li>)}
            </ul>
          ) : (
            <p className="text-sm text-green-700">All checks pass. Confirm to mark this week COMPLETE.</p>
          )}
        </CardContent>
      </Card>

      {rateFor && (
        <RateModal
          punch={rateFor}
          onClose={() => setRateFor(null)}
          onCreated={(id) => {
            if (id) upd.mutate({ recordId: rateFor.id, fields: { rate: [id] } });
            else toast.success("Rate created - pick it from the list");
            rateOptsQ.refetch();
            setRateFor(null);
          }}
        />
      )}
    </div>
  );
}

export default function Block() {
  const [weekId, setWeekId] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [fetching, setFetching] = useState(false);

  const weeksQ = useRecords({ from: ds.budgetWeek, select: weekSel, count: 12, orderBy: q.desc("start") });
  const runsQ = useRecords({ from: ds.runWeek, select: runSel, count: 100 });
  const itemsQ = useRecords({ from: ds.runItem, select: itemSel, count: 200, orderBy: q.desc("date") });
  useLoadAll(itemsQ as any);

  const weeks = useMemo(() => rowsOf(weeksQ.data), [weeksQ.data]);
  const runs = useMemo(() => rowsOf(runsQ.data), [runsQ.data]);
  const items = useMemo(() => rowsOf(itemsQ.data), [itemsQ.data]);

  const runFor = (wid: string) => runs.find((r) => idsOf(r.fields.week).includes(wid));

  const weekUpdate = useRecordUpdate({
    from: ds.budgetWeek,
    fields: weekUpd,
    onSuccess: async () => {
      await weeksQ.refetch();
      toast.success("Program clock in status updated");
    },
    onError: (e: any) => toast.error(e?.message ?? "Could not update the status"),
  });
  const setProgress = (wid: string, v: "Started" | "COMPLETE") => weekUpdate.mutate({ recordId: wid, fields: { progress: v } });

  const createRun = useRecordCreate({
    from: ds.runWeek,
    fields: runCreate,
    onSuccess: async () => {
      await runsQ.refetch();
      toast.success("Run created");
    },
    onError: (e: Error) => toast.error(e.message),
  });

  const triggerFetch = async (runId: string, wid: string, progress: string) => {
    setFetching(true);
    // Fetching again reopens the week: Started until it is confirmed to payroll again.
    if (progress !== "Started") setProgress(wid, "Started");
    try {
      await fetch(`${FETCH_WEBHOOK}?recordId=${encodeURIComponent(runId)}`, { method: "POST", mode: "no-cors" });
      toast.success("Fetching the hours - refresh in about a minute.");
      [45000, 90000].forEach((ms) => setTimeout(() => { runsQ.refetch(); }, ms));
      setTimeout(() => setFetching(false), 60000); // stay locked until the run shows Started
    } catch (e: any) {
      toast.error(e?.message ?? "Could not start the fetch");
      setFetching(false);
    }
  };

  const trigger = async (runId: string, wid: string, progress: string) => {
    setRunning(true);
    // Re-checking reopens the week: Started until it is confirmed to payroll again.
    if (progress !== "Started") setProgress(wid, "Started");
    try {
      await fetch(`${RUN_CHECK_WEBHOOK}?recordId=${encodeURIComponent(runId)}`, { method: "POST", mode: "no-cors" });
      toast.success("Check started - refresh in about a minute.");
      setTimeout(() => {
        runsQ.refetch();
        itemsQ.refetch();
      }, 60000);
      setTimeout(() => setRunning(false), 60000); // stay locked until the run shows Started
    } catch (e: any) {
      toast.error(e?.message ?? "Could not start the check");
      setRunning(false);
    }
  };

  const loading = weeksQ.isLoading || runsQ.isLoading;

  /* ---------------- week view ---------------- */
  if (weekId) {
    const week = weeks.find((w) => w.id === weekId);
    const run = runFor(weekId);
    const weekItems = run ? items.filter((i) => idsOf(i.fields.run).includes(run.id)) : [];
    const f = run?.fields ?? {};
    const progress = labelOf(week?.fields.progress);
    const fetchStatus = labelOf(f.fetchStatus);
    const fetched = fetchStatus === "COMPLETE";
    const lastNote = labelOf(f.notes).split("\n")[0];

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
          <CardContent className="space-y-2 p-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-sm font-medium">Step 1: fetch the hours (every studio; Coaching 1-on-1, Class Review and Instructor Eval only)</div>
              <div className="flex items-center gap-2">
                {run && <Badge variant="outline">Fetch: {fetchStatus || "Not started"}</Badge>}
                {run ? (
                  <Button size="sm" disabled={fetching || fetchStatus === "Started"} onClick={() => triggerFetch(run.id, weekId, progress)}>
                    {fetching || fetchStatus === "Started" ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : <Download className="mr-1 h-4 w-4" />}
                    Fetch hours
                  </Button>
                ) : (
                  <Button
                    size="sm"
                    disabled={createRun.isPending}
                    onClick={() => {
                      createRun.mutate({ name: `${day(week?.fields.start)} - ${day(week?.fields.end)}`, week: [weekId], status: "Not started" });
                      if (!progress) setProgress(weekId, "Started");
                    }}
                  >
                    Create run
                  </Button>
                )}
                <Button size="sm" variant="ghost" onClick={() => { runsQ.refetch(); itemsQ.refetch(); }}>
                  <RefreshCw className="h-4 w-4" />
                </Button>
              </div>
            </div>
            {!run && <p className="text-sm text-muted-foreground">Create the run for this week first, then fetch.</p>}
            {run && lastNote && <p className="text-xs text-muted-foreground">{lastNote}</p>}
          </CardContent>
        </Card>

        <Card>
          <CardContent className="space-y-3 p-3">
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium">Step 2: run the check (matches the punch hours to the reviews pushed to MTEK)</div>
              <div className="flex items-center gap-2">
                {run && <Badge variant="outline">{labelOf(f.status) || "Not started"}</Badge>}
                <Button size="sm" disabled={!run || !fetched || running || labelOf(f.status) === "Started"} onClick={() => run && trigger(run.id, weekId, progress)}>
                  {running ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : <Play className="mr-1 h-4 w-4" />}
                  Run check
                </Button>
              </div>
            </div>
            {!fetched && <p className="text-sm text-amber-700">Fetch the hours first (Step 1).</p>}
            {run && (
              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-lg border p-2">
                  <div className="text-xs text-muted-foreground">Matched</div>
                  <div className="text-xl font-semibold">{num(f.matched)}</div>
                </div>
                <div className="rounded-lg border p-2">
                  <div className="text-xs text-muted-foreground">Hours differ</div>
                  <div className="text-xl font-semibold">{num(f.hoursDiffer)}</div>
                  <span className={cn("text-xs font-medium", labelOf(f.hoursDifferCheck) === "ALL GOOD" ? "text-green-700" : "text-amber-700")}>{labelOf(f.hoursDifferCheck)}</span>
                </div>
                <div className="rounded-lg border p-2">
                  <div className="text-xs text-muted-foreground">Punch without review</div>
                  <div className="text-xl font-semibold">{num(f.punchNoReview)}</div>
                  <span className={cn("text-xs font-medium", labelOf(f.punchNoReviewCheck) === "ALL GOOD" ? "text-green-700" : "text-amber-700")}>{labelOf(f.punchNoReviewCheck)}</span>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <IssueTabs
          start={day(week?.fields.start)}
          end={day(week?.fields.end)}
          items={weekItems}
          hasRun={Boolean(run)}
          fetched={fetched}
          progress={progress}
          onProgress={(v) => setProgress(weekId, v)}
          onItemsChanged={() => itemsQ.refetch()}
        />
      </div>
    );
  }

  /* ---------------- list view ---------------- */
  return (
    <div className="mx-auto max-w-6xl space-y-3 p-4">
      <div>
        <h1 className="text-2xl font-semibold">Coaching & Review Hours</h1>
        <p className="text-sm text-muted-foreground">Open a week, fetch the hours, run the check, fix the issues, then confirm to payroll.</p>
      </div>
      {loading && <Loader2 className="h-5 w-5 animate-spin" />}
      <div className="rounded-lg border">
        <table className="w-full text-sm">
          <thead>
            <tr>
              <th className={TH}>Week</th>
              <th className={TH}>Fetch</th>
              <th className={TH}>Check</th>
              <th className={TH}>Program clock in status</th>
              <th className={TH}></th>
            </tr>
          </thead>
          <tbody>
            {weeks.map((w) => {
              const run = runFor(w.id);
              const progress = labelOf(w.fields.progress);
              return (
                <tr key={w.id}>
                  <td className={cn(TD, "whitespace-nowrap font-medium")}>{day(w.fields.start)} → {day(w.fields.end)}</td>
                  <td className={TD}>{labelOf(run?.fields.fetchStatus) || "Not started"}</td>
                  <td className={TD}>{labelOf(run?.fields.status) || "Not started"}</td>
                  <td className={TD}>
                    <span className={cn("rounded-full px-2 py-0.5 text-xs font-medium", progress === "COMPLETE" ? "bg-green-100 text-green-800" : progress ? "bg-yellow-100 text-yellow-800" : "bg-gray-100 text-gray-600")}>
                      {progress || "Not started"}
                    </span>
                  </td>
                  <td className={cn(TD, "text-right")}>
                    <Button size="sm" className="h-7" onClick={() => setWeekId(w.id)}>Open</Button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
