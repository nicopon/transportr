#!/usr/bin/env bash
# scripts/lint-comments.sh - Refuse deliberation left in committed code
#
# Two comment habits this repository has decided against, both checkable:
#
#   1. Deliberation. A comment that asks a question, weighs options or says "for now" is
#      thinking, not documentation. Left in place it tells the next reader the matter is settled
#      when it is not, and it is indistinguishable from a note that was resolved.
#   2. Doc comments with no member under them. Documentation for an API that no longer exists
#      survives every search for the API it names.
#
# Resolve it, or move it to the working notes and leave a one-line pointer to the decision.
# Nothing here judges volume or style: only these two are mechanical enough to enforce.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

SOURCES=$(find src samples tests -name '*.cs' -not -path '*/obj/*' -not -path '*/bin/*')
FAILED=0

report() {
    FAILED=1
    echo "$2" | while IFS= read -r line; do [ -n "$line" ] && echo "  $line"; done
    echo ""
}

# 1. Open questions: a // comment whose last character is a question mark. End-of-line comments
#    count: the habit this refuses is no smaller for sitting after code, and that is where the
#    only violation the rule ever had to catch was hiding.
HITS=$(grep -nE '(^|[[:space:]])//[^/].*\?[[:space:]]*$' $SOURCES || true)
if [ -n "$HITS" ]; then
    echo "Deliberation left in code — a comment that asks a question:"
    report q "$HITS"
fi

# 2. Thinking aloud. Deliberately narrow: these openers introduce reasoning, not a statement.
HITS=$(grep -nEi '^[[:space:]]*//[[:space:]]*(wait,|let'"'"'s |for now[,.]? |actually,|or only |maybe |assuming |hmm|todo|fixme|hack)' $SOURCES || true)
if [ -n "$HITS" ]; then
    echo "Deliberation left in code — thinking aloud:"
    report t "$HITS"
fi

# 3. A /// block whose next non-blank, non-comment, non-attribute line closes a scope or starts
#    another doc block: documentation with nothing under it.
HITS=$(awk '
    /^[[:space:]]*\/\/\//   { if (!indoc) { indoc = 1; start = FNR } ; next }
    /^[[:space:]]*$/        { next }
    /^[[:space:]]*\[/       { next }
    /^[[:space:]]*\/\//     { next }
    {
        if (indoc && ($0 ~ /^[[:space:]]*}/)) printf "%s:%d: doc comment with no member under it\n", FILENAME, start
        indoc = 0
    }
    END { if (indoc) printf "%s:%d: doc comment at end of file with no member under it\n", FILENAME, start }
' $SOURCES || true)
if [ -n "$HITS" ]; then
    echo "Doc comment describing nothing:"
    report d "$HITS"
fi

if [ $FAILED -ne 0 ]; then
    echo "❌ lint-comments: see CLAUDE.md, 'No deliberation in committed code'."
    exit 1
fi

echo "✅ lint-comments: clean."
