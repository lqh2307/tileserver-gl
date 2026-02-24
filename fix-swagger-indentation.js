#!/usr/bin/env node

import { readFileSync, writeFileSync } from 'fs';
import { globSync } from 'glob';

// Find all .js files in src/serves
const files = globSync('src/serves/**/*.js');

files.forEach((file) => {
  try {
    let content = readFileSync(file, 'utf8');
    let modified = false;

    // Fix: ' *         application/json:' followed by ' *             schema:' 
    // Should be: ' *         application/json:' followed by ' *           schema:'
    const pattern1 = /( \* +application\/json:)\n( \* +)schema:/g;
    if (pattern1.test(content)) {
      const lines = content.split('\n');
      for (let i = 0; i < lines.length - 1; i++) {
        const line = lines[i];
        const nextLine = lines[i + 1];

        if (line.match(/\s\*\s+application\/json:$/) && nextLine.match(/\s\*\s+schema:/)) {
          const indent = line.match(/(\s\*\s+)/)[1];
          const baseIndent = indent.replace(/\s+$/, ''); // Remove trailing spaces
          const schemaIndent = baseIndent + '  '; // Two more spaces than base

          // Replace the schema line to have correct indentation
          lines[i + 1] = schemaIndent + 'schema:';
          modified = true;
        }
      }
      content = lines.join('\n');
    }

    // Fix: ' *         application/json:' followed by ' *             schema:' 
    // (with different indentation levels)
    content = content.replace(
      /( \*\s+application\/json:)\n(\s+\* +)schema:/g,
      (match, p1, p2) => {
        const indent = p1.match(/(\*\s+)/)[1];
        return p1 + '\n' + indent + 'schema:';
      }
    );

    if (modified || content !== readFileSync(file, 'utf8')) {
      writeFileSync(file, content, 'utf8');
      console.log(`✓ Fixed: ${file}`);
    }
  } catch (error) {
    console.error(`✗ Error processing ${file}:`, error.message);
  }
});

console.log('\nDone!');
