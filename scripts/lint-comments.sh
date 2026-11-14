#!/usr/bin/env bash
# scripts/lint-comments.sh - Refuse deliberation left in committed code
#
# Three comment habits this repository has decided against, all checkable (and, last, a table of
# timings that must not cite a name the code does not have):
#
#   1. Deliberation. A comment that asks a question, weighs options or says "for now" is
#      thinking, not documentation. Left in place it tells the next reader the matter is settled
#      when it is not, and it is indistinguishable from a note that was resolved.
#   2. Doc comments with no member under them. Documentation for an API that no longer exists
#      survives every search for the API it names.
#   3. History. A comment that tells what the code used to do describes a version nobody reads
#      any more; the commit log keeps it.
#
# Resolve it, or move it to the working notes and leave a one-line pointer to the decision.
# Nothing here judges volume or style: only these are mechanical enough to enforce.

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

# 4. History. Three markers, no more: "old" and "previously" are too often a plain adjective
#    ("the old token"). "used to" is history only in the active voice; "is used to probe" says
#    what a thing is for, and "Used to verify" opening a member's doc is a purpose too. "^" appears
#    only at the start of an expression: grep implementations disagree on it mid-pattern.
AT='(^|[[:space:]])//(.*[^[:alnum:]_])?'
END='([^[:alnum:]_]|$)'
HITS=$(grep -nE "${AT}(DECISION:|[Ww]e now${END}|used to${END})" $SOURCES \
    | grep -vE "[^[:alnum:]_](is|are|was|were|be|been|being|get|gets|got) used to${END}" || true)
if [ -n "$HITS" ]; then
    echo "History left in code — a comment that tells what the code used to do:"
    report h "$HITS"
fi

# 5. The table of timings in ResilienceArchitecture.md cites each constant as `Type.Member`, never by
#    file and line: the type has to be declared in src/, and a file that declares it the member.
REFS=$(awk '/^## Key Timings/{t=1;next} /^##/{t=0} t && /^\| (client|hub) \|/' ResilienceArchitecture.md \
    | cut -d'|' -f3 | grep -oE '`[A-Z][A-Za-z0-9]*\.[A-Za-z_][A-Za-z0-9_]*`' | tr -d '`' | sort -u)
HITS=$([ -n "$REFS" ] || echo "no Type.Member found in the table of timings"
for ref in $REFS; do
    files=$(grep -rlE "(class|record|struct|interface|enum)[[:space:]]+${ref%%.*}([^[:alnum:]_]|$)" src --include='*.cs' | grep -vE '/(bin|obj)/')
    [ -n "$files" ] || { echo "$ref: no such type in src/"; continue; }
    grep -qE "^[[:space:]]*(public|private|internal|protected)[^=;(]*[[:space:]]${ref#*.}[[:space:]]*(=|;|\{|\(|=>)" $files \
        || echo "$ref: no such member in the file of its type"
done)
if [ -n "$HITS" ]; then
    echo "The table of timings cites a name that does not exist:"
    report n "$HITS"
fi

if [ $FAILED -ne 0 ]; then
    echo "❌ lint-comments: see CLAUDE.md, 'No deliberation in committed code'."
    exit 1
fi

echo "✅ lint-comments: clean."
