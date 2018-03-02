const fs = require('fs');
const glob = require('glob');
const path = require('path');

const patched = `${__dirname}/patched-process-nextick-args.js`;
glob.sync('./**/process-nextick-args').forEach(pnaDirectoryPath => {
  const packageJson = require(path.join('../', pnaDirectoryPath, 'package.json'));
  const majorVersion = parseInt(packageJson.version[0], 10);

  if (majorVersion <= 1) {
    fs.copyFileSync(patched, path.join(pnaDirectoryPath, 'index.js'));
  }
});