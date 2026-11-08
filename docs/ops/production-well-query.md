# Well-profile lease production lookup

November 8, 2026: a real linked-well request returned 18 reports but spent approximately 18 seconds counting and seven seconds selecting rows. The correlated well/lease EXISTS ran against the statewide production table, causing the new well-profile chart's bounded fetch to time out.

The API now resolves distinct exact-confidence public well/lease/product relationships once in a materialized CTE, then joins through the existing lease index. Product matching, current source/parser gates, monthly sentinel exclusion, scope/date filters and pagination are preserved. Duplicate link methods cannot multiply source reports. Lease production is never allocated to individual wells.

Validation: 128 library tests pass; an explicit pg_temp-only SQL fixture verifies duplicate links, oil/gas product isolation, low-confidence and tombstoned relationships, API/KID lookup and unfiltered scope. No permanent production rows are changed by that fixture.

Deploy the pushed main revision with `bash scripts/deploy-production-api.sh` under a native user transient unit limited to one CPU and 1600MB memory. Build one Rust job; restart only the Kansas API. Do not overlap this build with another server build. Recheck the actual linked-well API, pagination/global filters and raw HTTPS well-profile chart after activation.
