Parser. Transcribe the plan below into the machine-readable sub-task queue. No planning decisions of your own: every sub-task, its order, files, descriptions, and acceptance criteria come from the plan.

Plan:

{{.Plan}}

Output contract: your entire reply must be exactly one raw JSON array and nothing else. No prose, no markdown code fences, no commentary. Each element an object:

[{"id": 1, "type": "<short kind: implement | test | docs | fix>", "target_files": ["path/from/repo/root"], "description": "<what to do, precisely>", "acceptance_criteria": ["<concrete, terminal check>", "..."]}]

- "id": integer, 1-based, execution order.
- "type": the sub-task's kind.
- "target_files": the 1–2 files this sub-task creates or edits.
- "description": one paragraph — enough to implement without re-deriving the whole task.
- "acceptance_criteria": strings; at least one per sub-task.

Transcribe faithfully: do not add, drop, merge, split, or reorder sub-tasks; keep every description and acceptance criterion in the plan's own words.
