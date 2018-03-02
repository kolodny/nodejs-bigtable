const fs = require('fs');
const glob = require('glob');

const patched = `${__dirname}/patched-process-nextick-args.js`;
glob.sync('./**/process-nextick-args/index.js').forEach(file => {
  fs.copyFileSync(patched, file);
});
