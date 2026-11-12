# Fork

The canonical branch of this fork is `dev`. Deploy from `dev`, or from the working branch that will become `dev`. When that branch is finished and its tests pass, fast-forward it onto `dev`. Do not force-push.
On a schema rebase, do not preserve incoming version integers. Fold our table additions into one migration numbered as the next version after the onto commit's latest.
Keep each `.gjc/skills/verify-*/` skill honest with `/maintain-verification-skill`. One verification skill per app, with one feature file per user-facing behavior. A pass on one feature file does not cover the others. Do not edit product code to make a failed drive look passed.
