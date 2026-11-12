# Fork

The canonical branch of this fork is `dev`. Deploy from `dev`, or from the working branch that will become `dev`. When that branch is finished and its tests pass, fast-forward it onto `dev`. Do not force-push.
Schema rebase: do not keep version numbers from the commits being replayed. On this base, upstream owns schema 40, 41, and 42. FirstMate is one migration at schema 43, the next number after upstream's latest. Do not put FirstMate into 40, 41, or 42. Do not split FirstMate into 43 and 44. Schema 44 is the heal for a recorded schema 42 that never added images_json.
Keep each `.gjc/skills/verify-*/` skill honest with `/maintain-verification-skill`. One verification skill per app, with one feature file per user-facing behavior. A pass on one feature file does not cover the others. Do not edit product code to make a failed drive look passed.
