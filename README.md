# Paperless Extensions

A language-neutral home for small services that extend a Paperless-ngx
deployment without modifying Paperless itself.

## File Normalizer

Paperless-ngx consumes whatever filename it is handed, and that name is what
you later search for, skim past in a list, and recognise a document by.
Documents seldom arrive with a useful one. Scanners emit `scan_0001.pdf`,
phones emit `IMG_20260118_113052.jpg`, and browsers emit
`Bank Statement (2).PDF`. Renaming them by hand does not scale, and renaming
them inside the consume directory races Paperless's own watcher, which may
take a file while it is still being written.

File Normalizer stands in front of that directory. Documents are dropped into
an intake directory instead; each one is given a consistent name under a single
declared policy and published into the consume directory as a finished
document, so Paperless only ever sees a complete file under its final name.

It is built for an archive that is added to continuously rather than imported
in one batch. Intake and consumption may live on different filesystems, the
renaming work spreads across as many instances as the volume needs, and every
document either lands with a durable receipt or is left in a state an operator
can see and act on. Paperless-ngx is neither modified nor wrapped: it goes on
consuming its own directory, and it can be upgraded on its own schedule.
