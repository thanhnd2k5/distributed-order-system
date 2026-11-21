# Document set of the distributed-order-system project

Updated 21 Nov 2026. The project is in the implementation phase: step 0 (preparation) of document 03 is done, and the business code is next, starting with step 1.

The repository is `https://github.com/congdtptit/distributed-order-system`, default branch `main`. It holds this document set, the code in `backend/` and `frontend/`, and the Claude Code configuration.

Project documents are written in English. They were written in Vietnamese until 21 Nov 2026; the Vietnamese originals of that date are kept, frozen, in `archive/vi-2026-11-21/`.

## Documents

Read them in number order. When two documents disagree, the lower-numbered one is right and the higher-numbered one must be fixed.

| # | Document | Answers the question | State |
|---|---|---|---|
| 01 | [Business specification v1.5](01-business-spec.md) | What the system must do: rules BR1–BR12, four state machines, four flows, 44 test scenarios, deployment assumptions | Master copy of the specification from v1.5, after three review rounds. 2 points still await team confirmation (section 11); both are configuration values |
| 02 | [Detailed design, revision 7](02-detailed-design.md) | What to code against: 13 Kafka event types, the three DB schemas, replication and reporting, the API, the implementation order | Follows specification v1.5. The SQL was trial-run on PostgreSQL (section 9). All 12 points in section 8 were confirmed on 21 Nov 2026 |
| 03 | [Implementation plan, revision 9](03-implementation-plan.md) | What to build with and how: Python and its libraries, the TypeScript frontend, modules, three test tiers, order of work, how to use Claude Code | The language is settled; the libraries are proposals to verify in step 0 |

## Pending work

- [x] Confirm the three points in section 11 of document 01 that change behaviour: BR12 with the Consumed message, local cancel under BR9, blocking flagged ingredients under BR6. Done on 21 Nov 2026: @Naul agreed to all three as proposed.
- [ ] Confirm the two remaining points in section 11 of document 01: the parameters of section 10 and the list of customer cancel reasons. They are configuration values and do not block coding.
- [x] Confirm points 1–11 in section 8 of document 02. Done on 21 Nov 2026: @Naul agreed to all eleven as proposed.
- [x] Confirm point 12 in section 8 of document 02 (replication and reporting). Done on 21 Nov 2026: @Naul agreed to the proposal, PostgreSQL logical replication.
- [x] Settle the language. Done on 21 Nov 2026: @Naul chose Python; the project is built from scratch, with no B5 baseline.
- [x] Move the project to the implementation phase and run step 0 of document 03. Done on 21 Nov 2026: @Naul changed `.claude/project-phase` to `code`.
- [ ] Decide where the order form gets its list of dishes in phase 1: `GET /menu` belongs to phase 2, and section 6 of document 02 has no other source. Needed before the branch screen in step 6b.

## Claude Code configuration

The configuration is in `CLAUDE.md` at the project root and in the `.claude/` directory. Section 5.7 of document 03 lists each part that exists and each part that is pending.

Two things to know when working on the documents:

- After editing a file in this directory, run `python3 .claude/skills/doc-check/scripts/check_docs.py` from the project root, or call `/doc-check` in Claude Code. It checks tables, links, the scenario list and the version numbers across the files.
- The file `.claude/project-phase` contains `code`. Only a person changes it; a hook stops Claude Code from editing it.
- On GitHub, the `docs` workflow runs the same check on every push to `main` and every pull request; the `ci` workflow checks the code.

## Documents outside this set

- **Specification v1.3, the full edition.** Section 15 of document 01 points to sections of v1.3 for the path to production. Not in the project yet.
- **Specification v1.4, PDF of 19 Nov 2026.** The original that document 01 was copied from; no longer the master copy since v1.5.
- **Advanced Claude Code: study notes, parts 03–06** (PDF, 21 Nov 2026). General study material about Claude Code, not specific to this project, so it is not copied here. Section 5 of document 03 applies it to the project.

## Conventions for editing the documents

- From v1.5, document 01 in this directory is the master copy of the specification. To change the business, edit document 01, raise its version and add a row to the change table at the top of the file, then bring documents 02 and 03 in line.
- When editing document 02 or 03, raise the revision number at the top of the file and add a "Changes in revision N" table giving the section, the change and the reason.
- When a point is confirmed, mark it in the section of the document that holds it, with the date and who confirmed it, then update "Pending work" above. Marking a point does not count as a revision.
- The files in `archive/` are frozen. Do not update them.
