"""Shared plumbing for literate-analysis repositories.

Three things live here so that three repositories do not each have their own:

  litmo.kinds     artifact sync — archive, mirror and fetch
  litmo/data/common.mk   the make targets every repository answers to
  litmo.config    the sync.toml schema that ties them together

The convention those enforce is one line long: **analysis modules do all the
computing, and documents do no arithmetic.** A document may read stored
artifacts from `out/` or call those modules and format what comes back.
Everything else — nine make targets, data/ and out/ kept out of git and in a
bucket, reports/ holding the .qmd — follows from wanting that to be checkable.
"""

__version__ = "0.1.0"
