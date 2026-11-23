# Demonstration runs (M3: concurrent execution)

Generated with `careeros demo --scenario <name>` on a fresh synthetic database each time,
with up to 3 concurrent workers (so event lines of parallel tasks interleave).
All model calls are simulated by offline mock adapters; all costs are simulated USD.

## Scenario: standard

```text
Scenario 'standard' (all model calls simulated)

Objective 1: 'Prepare application package for a synthetic posting' (exact playbook match for kind 'demo_application_package')
  plan v1 (playbook:demo_application_package): 7 steps in 5 waves; parallelizable groups: [['extract_keywords', 'research_company'], ['draft_cover_letter', 'tailor_resume']]
  [extract_keywords] ACCEPTED: deterministic extract_keywords: $0
  [research_company] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01186 -> ok $0.00919
  [research_company] ACCEPTED: mock-medium/low accepted
  [classify_role] attempt 1: mock-small (tier 3, effort none) p=0.97 est=$0.00021 -> ok $0.00021
  [classify_role] ACCEPTED: mock-small/none accepted
  [tailor_resume] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01668 -> ok $0.01356
  [tailor_resume] review by mock-large/low: accept score=0.91 ($0.02379)
  [tailor_resume] ACCEPTED: mock-medium/low accepted (review 0.91)
  [draft_cover_letter] attempt 1: mock-medium (tier 2, effort medium) p=0.97 est=$0.01891 -> ok $0.01693
  [draft_cover_letter] review by mock-large/low: accept score=0.83 ($0.02159)
  [draft_cover_letter] ACCEPTED: mock-medium/medium accepted (review 0.83)
  [assemble] ACCEPTED: deterministic assemble_package: $0
  [strategic_review] attempt 1: mock-large (tier 1, effort high) p=0.97 est=$0.09452 -> ok $0.08642
  [strategic_review] ACCEPTED: mock-large/high accepted
Objective 1 done: definition of done met

Objective 1: Prepare application package for a synthetic posting  [done]   (ALL COSTS SIMULATED)

Model calls:
    # step               purpose  att model        T effort status        in   out       est      cost     ms
    1 research_company   task       1 mock-medium  2 low    ok           202   572  $0.01186  $0.00919   3932
    2 classify_role      task       1 mock-small   3 none   ok           243   122  $0.00021  $0.00021    444
    3 tailor_resume      task       1 mock-medium  2 low    ok           310   842  $0.01668  $0.01356   5552
    4 tailor_resume      review     1 mock-large   1 low    ok            96   298  $0.02957  $0.02379   4476
    5 draft_cover_letter task       1 mock-medium  2 medium ok           302  1068  $0.01891  $0.01693   6908
    6 draft_cover_letter review     1 mock-large   1 low    ok            74   273  $0.02924  $0.02159   4176
    7 strategic_review   task       1 mock-large   1 high   ok           301  1092  $0.09452  $0.08642  14004

Tasks:
  step               mode          status         att esc      cost    wasted quality  final model
  extract_keywords   deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  research_company   model         succeeded        1   0  $0.00919  $0.00000    0.80  mock-medium
  classify_role      model         succeeded        1   0  $0.00021  $0.00000    0.77  mock-small
  tailor_resume      model         succeeded        1   0  $0.03735  $0.00000    0.82  mock-medium
  draft_cover_letter model         succeeded        1   0  $0.03851  $0.00000    0.78  mock-medium
  assemble           deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  strategic_review   model         succeeded        1   0  $0.08642  $0.00000    0.79  mock-large

Totals (simulated USD):
  total $0.17168 = tasks $0.12630 + reviews $0.04537 + planning $0.00000
  estimated before calls $0.20097; budget $1.00000
  spent on failed/rejected attempts $0.00000
  calls 7 (abandoned 0), cache hits 0, retries 0, escalations 0, deterministic steps 2
  downgrades 0, budget requeues 0, recovered tasks 0
  tasks succeeded 7; cost per success $0.02453; mean accepted quality 0.85
  quality-adjusted cost per success $0.02878
  by tier: tier 1 (mock-large) $0.13179, tier 2 (mock-medium) $0.03967, tier 3 (mock-small) $0.00021
```

## Scenario: escalation

```text
Scenario 'escalation' (all model calls simulated)

Objective 1: 'Prepare application package for a synthetic posting' (exact playbook match for kind 'demo_application_package')
  plan v1 (playbook:demo_application_package): 7 steps in 5 waves; parallelizable groups: [['extract_keywords', 'research_company'], ['draft_cover_letter', 'tailor_resume']]
  [extract_keywords] ACCEPTED: deterministic extract_keywords: $0
  [research_company] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01186 -> timeout $0.00061
  [research_company] timeout -> RETRY_SAME (transient timeout; retry with backoff)
  [research_company] attempt 2: mock-medium (tier 2, effort low) p=0.97 est=$0.01186 -> ok $0.01291
  [research_company] ACCEPTED: mock-medium/low accepted
  [classify_role] attempt 1: mock-small (tier 3, effort none) p=0.97 est=$0.00021 -> malformed $0.00013
  [classify_role] malformed -> RETRY_WITH_FEEDBACK (malformed output; one repair retry)
  [classify_role] attempt 2: mock-small (tier 3, effort none) p=0.78 est=$0.00022 -> ok $0.00023
  [classify_role] invalid -> ESCALATE_TIER (invalid output after a repair retry)
  [classify_role] attempt 3: mock-medium (tier 2, effort none) p=0.97 est=$0.00259 -> ok $0.00240
  [classify_role] ACCEPTED: mock-medium/none accepted
  [draft_cover_letter] attempt 1: mock-medium (tier 2, effort medium) p=0.97 est=$0.01891 -> ok $0.01987
  [tailor_resume] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01668 -> ok $0.01620
  [draft_cover_letter] review by mock-large/low: revise score=0.70 ($0.03006)
  [draft_cover_letter] review_revise -> ESCALATE_EFFORT (review_revise: more reasoning should help)
  [tailor_resume] low_confidence -> ESCALATE_EFFORT (low_confidence: more reasoning should help)
  [draft_cover_letter] attempt 2: mock-medium (tier 2, effort high) p=0.97 est=$0.02799 -> ok $0.02965
  [tailor_resume] attempt 2: mock-medium (tier 2, effort medium) p=0.97 est=$0.02199 -> ok $0.01614
  [tailor_resume] review by mock-large/low: accept score=0.96 ($0.02596)
  [tailor_resume] ACCEPTED: mock-medium/medium accepted (review 0.96)
  [draft_cover_letter] review by mock-large/low: accept score=0.81 ($0.02526)
  [draft_cover_letter] ACCEPTED: mock-medium/high accepted (review 0.81)
  [assemble] ACCEPTED: deterministic assemble_package: $0
  [strategic_review] attempt 1: mock-large (tier 1, effort high) p=0.97 est=$0.09452 -> ok $0.09519
  [strategic_review] ACCEPTED: mock-large/high accepted
Objective 1 done: definition of done met

Objective 1: Prepare application package for a synthetic posting  [done]   (ALL COSTS SIMULATED)

Model calls:
    # step               purpose  att model        T effort status        in   out       est      cost     ms
    1 research_company   task       1 mock-medium  2 low    timeout      202     0  $0.01186  $0.00061  60000
    2 research_company   task       2 mock-medium  2 low    ok           202   820  $0.01186  $0.01291   5420
    3 classify_role      task       1 mock-small   3 none   malformed    243    58  $0.00021  $0.00013    316
    4 classify_role      task       2 mock-small   3 none   ok           278   132  $0.00022  $0.00023    464
    5 classify_role      task       3 mock-medium  2 none   ok           264   107  $0.00259  $0.00240   1142
    6 draft_cover_letter task       1 mock-medium  2 medium ok           302  1264  $0.01891  $0.01987   8084
    7 tailor_resume      task       1 mock-medium  2 low    ok           310  1018  $0.01668  $0.01620   6608
    8 draft_cover_letter review     1 mock-large   1 low    ok            74   386  $0.02924  $0.03006   5532
    9 draft_cover_letter task       2 mock-medium  2 high   ok           329  1911  $0.02799  $0.02965  11966
   10 draft_cover_letter review     2 mock-large   1 low    ok            74   322  $0.02924  $0.02526   4764
   11 tailor_resume      task       2 mock-medium  2 medium ok           329  1010  $0.02199  $0.01614   6560
   12 tailor_resume      review     2 mock-large   1 low    ok            96   327  $0.02957  $0.02596   4824
   13 strategic_review   task       1 mock-large   1 high   ok           301  1209  $0.09452  $0.09519  15408

Tasks:
  step               mode          status         att esc      cost    wasted quality  final model
  extract_keywords   deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  research_company   model         succeeded        2   0  $0.01351  $0.00061    0.78  mock-medium
  classify_role      model         succeeded        3   1  $0.00276  $0.00037    0.90  mock-medium
  tailor_resume      model         succeeded        2   1  $0.05830  $0.01620    0.96  mock-medium
  draft_cover_letter model         succeeded        2   1  $0.10484  $0.04993    0.77  mock-medium
  assemble           deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  strategic_review   model         succeeded        1   0  $0.09519  $0.00000    0.83  mock-large

Totals (simulated USD):
  total $0.27461 = tasks $0.19332 + reviews $0.08128 + planning $0.00000
  estimated before calls $0.29484; budget $1.00000
  spent on failed/rejected attempts $0.06710
  calls 13 (abandoned 0), cache hits 0, retries 2, escalations 3, deterministic steps 2
  downgrades 0, budget requeues 0, recovered tasks 0
  tasks succeeded 7; cost per success $0.03923; mean accepted quality 0.89
  quality-adjusted cost per success $0.04409
  by tier: tier 1 (mock-large) $0.17647, tier 2 (mock-medium) $0.09776, tier 3 (mock-small) $0.00037
```

## Scenario: budget

```text
Scenario 'budget' (all model calls simulated)

Objective 1: 'Prepare application package for a synthetic posting' (exact playbook match for kind 'demo_application_package')
  plan v1 (playbook:demo_application_package): 7 steps in 5 waves; parallelizable groups: [['extract_keywords', 'research_company'], ['draft_cover_letter', 'tailor_resume']]
  [extract_keywords] ACCEPTED: deterministic extract_keywords: $0
  [research_company] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01186 -> ok $0.00919
  [research_company] ACCEPTED: mock-medium/low accepted
  [classify_role] attempt 1: mock-small (tier 3, effort none) p=0.97 est=$0.00021 -> ok $0.00021
  [classify_role] ACCEPTED: mock-small/none accepted
  [draft_cover_letter] attempt 1: mock-medium (tier 2, effort medium) p=0.97 est=$0.01891 -> ok $0.01693
  [draft_cover_letter] review by mock-large/low: accept score=0.83 ($0.02159)
  [draft_cover_letter] ACCEPTED: mock-medium/medium accepted (review 0.83)
  [tailor_resume] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01668 -> ok $0.01356
  [tailor_resume] review by mock-large/low: accept score=0.91 ($0.02379)
  [tailor_resume] ACCEPTED: mock-medium/low accepted (review 0.91)
  [assemble] ACCEPTED: deterministic assemble_package: $0
  [strategic_review] WAITING FOR YOU (budget_increase, request 1): Meeting the critical quality floor (0.92) needs ~$0.0945; only $0.0347 is available (critical priority: never downgraded automatically). Approve a budget increase.
Objective is waiting for your decision (see approvals).
*** USER APPROVES request 1 (budget_increase): +$0.1145 simulated budget ***
  [strategic_review] attempt 1: mock-large (tier 1, effort high) p=0.97 est=$0.09452 -> ok $0.08642
  [strategic_review] ACCEPTED: mock-large/high accepted
Objective 1 done: definition of done met

Objective 1: Prepare application package for a synthetic posting  [done]   (ALL COSTS SIMULATED)

Model calls:
    # step               purpose  att model        T effort status        in   out       est      cost     ms
    1 research_company   task       1 mock-medium  2 low    ok           202   572  $0.01186  $0.00919   3932
    2 classify_role      task       1 mock-small   3 none   ok           243   122  $0.00021  $0.00021    444
    3 draft_cover_letter task       1 mock-medium  2 medium ok           302  1068  $0.01891  $0.01693   6908
    4 draft_cover_letter review     1 mock-large   1 low    ok            74   273  $0.02924  $0.02159   4176
    5 tailor_resume      task       1 mock-medium  2 low    ok           310   842  $0.01668  $0.01356   5552
    6 tailor_resume      review     1 mock-large   1 low    ok            96   298  $0.02957  $0.02379   4476
    7 strategic_review   task       1 mock-large   1 high   ok           301  1092  $0.09452  $0.08642  14004

Tasks:
  step               mode          status         att esc      cost    wasted quality  final model
  extract_keywords   deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  research_company   model         succeeded        1   0  $0.00919  $0.00000    0.80  mock-medium
  classify_role      model         succeeded        1   0  $0.00021  $0.00000    0.77  mock-small
  tailor_resume      model         succeeded        1   0  $0.03735  $0.00000    0.82  mock-medium
  draft_cover_letter model         succeeded        1   0  $0.03851  $0.00000    0.78  mock-medium
  assemble           deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  strategic_review   model         succeeded        1   0  $0.08642  $0.00000    0.79  mock-large

Totals (simulated USD):
  total $0.17168 = tasks $0.12630 + reviews $0.04537 + planning $0.00000
  estimated before calls $0.20097; budget $0.23450
  spent on failed/rejected attempts $0.00000
  calls 7 (abandoned 0), cache hits 0, retries 0, escalations 0, deterministic steps 2
  downgrades 0, budget requeues 0, recovered tasks 0
  tasks succeeded 7; cost per success $0.02453; mean accepted quality 0.85
  quality-adjusted cost per success $0.02878
  by tier: tier 1 (mock-large) $0.13179, tier 2 (mock-medium) $0.03967, tier 3 (mock-small) $0.00021
```

## Scenario: interrupt

```text
Scenario 'interrupt' (all model calls simulated)

Objective 1: 'Prepare application package for a synthetic posting' (exact playbook match for kind 'demo_application_package')
  plan v1 (playbook:demo_application_package): 7 steps in 5 waves; parallelizable groups: [['extract_keywords', 'research_company'], ['draft_cover_letter', 'tailor_resume']]
  [extract_keywords] ACCEPTED: deterministic extract_keywords: $0
  [research_company] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01186 -> ok $0.00919
  [research_company] ACCEPTED: mock-medium/low accepted
  [classify_role] attempt 1: mock-small (tier 3, effort none) p=0.97 est=$0.00021 -> ok $0.00021
  [classify_role] ACCEPTED: mock-small/none accepted
  [tailor_resume] attempt 1: mock-medium (tier 2, effort low) p=0.97 est=$0.01668 -> ok $0.01356
*** SESSION TERMINATED: session terminated after the model replied ***
*** NEW SESSION (10 min later): resuming ***
  recovered task 4 (tailor_resume) interrupted at attempt 1
  recovered task 5 (draft_cover_letter) interrupted at attempt 1
  [tailor_resume] attempt 1: resuming from checkpoint (call 3, no new model call)
  [tailor_resume] review by mock-large/low: accept score=0.91 ($0.02379)
  [tailor_resume] ACCEPTED: mock-medium/low accepted (review 0.91)
  [draft_cover_letter] attempt 1: mock-medium (tier 2, effort medium) p=0.97 est=$0.01891 -> ok $0.01693
  [draft_cover_letter] review by mock-large/low: accept score=0.83 ($0.02159)
  [draft_cover_letter] ACCEPTED: mock-medium/medium accepted (review 0.83)
  [assemble] ACCEPTED: deterministic assemble_package: $0
  [strategic_review] attempt 1: mock-large (tier 1, effort high) p=0.97 est=$0.09452 -> ok $0.08642
  [strategic_review] ACCEPTED: mock-large/high accepted
Objective 1 done: definition of done met

Objective 1: Prepare application package for a synthetic posting  [done]   (ALL COSTS SIMULATED)

Model calls:
    # step               purpose  att model        T effort status        in   out       est      cost     ms
    1 research_company   task       1 mock-medium  2 low    ok           202   572  $0.01186  $0.00919   3932
    2 classify_role      task       1 mock-small   3 none   ok           243   122  $0.00021  $0.00021    444
    3 tailor_resume      task       1 mock-medium  2 low    ok           310   842  $0.01668  $0.01356   5552
    4 tailor_resume      review     1 mock-large   1 low    ok            96   298  $0.02957  $0.02379   4476
    5 draft_cover_letter task       1 mock-medium  2 medium ok           302  1068  $0.01891  $0.01693   6908
    6 draft_cover_letter review     1 mock-large   1 low    ok            74   273  $0.02924  $0.02159   4176
    7 strategic_review   task       1 mock-large   1 high   ok           301  1092  $0.09452  $0.08642  14004

Tasks:
  step               mode          status         att esc      cost    wasted quality  final model
  extract_keywords   deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  research_company   model         succeeded        1   0  $0.00919  $0.00000    0.80  mock-medium
  classify_role      model         succeeded        1   0  $0.00021  $0.00000    0.77  mock-small
  tailor_resume      model         succeeded        1   0  $0.03735  $0.00000    0.82  mock-medium
  draft_cover_letter model         succeeded        1   0  $0.03851  $0.00000    0.78  mock-medium
  assemble           deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  strategic_review   model         succeeded        1   0  $0.08642  $0.00000    0.79  mock-large

Totals (simulated USD):
  total $0.17168 = tasks $0.12630 + reviews $0.04537 + planning $0.00000
  estimated before calls $0.20097; budget $1.00000
  spent on failed/rejected attempts $0.00000
  calls 7 (abandoned 0), cache hits 0, retries 0, escalations 0, deterministic steps 2
  downgrades 0, budget requeues 0, recovered tasks 2
  tasks succeeded 7; cost per success $0.02453; mean accepted quality 0.85
  quality-adjusted cost per success $0.02878
  by tier: tier 1 (mock-large) $0.13179, tier 2 (mock-medium) $0.03967, tier 3 (mock-small) $0.00021
```

## Scenario: novel

```text
Scenario 'novel' (all model calls simulated)

Objective 1: 'Summarize what my robotics club notes say' (no familiar playbook matches)
  no familiar playbook matches: requesting model-assisted planning
  planning attempt 1: mock-large/medium -> ok ($0.07694)
  plan v1 (model): 3 steps in 3 waves; parallelizable groups: none
  [step1] ACCEPTED: deterministic extract_keywords: $0
  [step2] attempt 1: mock-small (tier 3, effort none) p=0.97 est=$0.00042 -> ok $0.00044
  [step2] ACCEPTED: mock-small/none accepted
  [step3] ACCEPTED: deterministic assemble_package: $0
Objective 1 done: definition of done met

Objective 1: Summarize what my robotics club notes say  [done]   (ALL COSTS SIMULATED)

Model calls:
    # step               purpose  att model        T effort status        in   out       est      cost     ms
    1 (planning)         planning   1 mock-large   1 medium ok           129  1000  $0.07694  $0.07694  12900
    2 step2              task       1 mock-small   3 none   ok           185   318  $0.00042  $0.00044    836

Tasks:
  step               mode          status         att esc      cost    wasted quality  final model
  step1              deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)
  step2              model         succeeded        1   0  $0.00044  $0.00000    0.91  mock-small
  step3              deterministic succeeded        1   0  $0.00000  $0.00000    1.00  (no model)

Totals (simulated USD):
  total $0.07738 = tasks $0.00044 + reviews $0.00000 + planning $0.07694
  estimated before calls $0.07736; budget $1.00000
  spent on failed/rejected attempts $0.00000
  calls 2 (abandoned 0), cache hits 0, retries 0, escalations 0, deterministic steps 2
  downgrades 0, budget requeues 0, recovered tasks 0
  tasks succeeded 3; cost per success $0.02579; mean accepted quality 0.97
  quality-adjusted cost per success $0.02663
  by tier: tier 1 (mock-large) $0.07694, tier 3 (mock-small) $0.00044
```
