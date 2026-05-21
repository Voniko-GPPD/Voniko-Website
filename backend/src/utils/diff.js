const Diff = require('diff');

function createUnifiedDiff(oldContent, newContent, oldLabel = 'old', newLabel = 'new') {
  return Diff.createPatch(oldLabel, oldContent, newContent, oldLabel, newLabel);
}

function parseDiffToHunks(unifiedDiff) {
  const lines = unifiedDiff.split('\n');
  const hunks = [];
  let currentHunk = null;

  for (const line of lines) {
    if (line.startsWith('@@')) {
      if (currentHunk) hunks.push(currentHunk);
      currentHunk = { header: line, lines: [] };
    } else if (currentHunk) {
      currentHunk.lines.push(line);
    }
  }
  if (currentHunk) hunks.push(currentHunk);
  return hunks;
}

module.exports = { createUnifiedDiff, parseDiffToHunks };
