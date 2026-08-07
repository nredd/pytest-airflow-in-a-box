"""Deliberately broken example Dag.

Bundled fixture data: proves import errors are reported, never silently
swallowed, by Dag bag parsing and Dag-file collection.
"""

from __future__ import annotations

raise RuntimeError("deliberately broken example Dag")
