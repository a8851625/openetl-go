const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const root = path.resolve(__dirname, '..');
const modules = new Map();

function loadSource(relativePath) {
  const filename = path.resolve(root, relativePath);
  if (modules.has(filename)) return modules.get(filename).exports;
  const source = fs.readFileSync(filename, 'utf8');
  const compiled = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  const module = { exports: {} };
  modules.set(filename, module);
  const resolveDependency = (name) => {
    if (!name.startsWith('.')) throw new Error('Unsupported test dependency ' + name);
    const target = path.resolve(path.dirname(filename), name);
    return loadSource(path.relative(root, target.endsWith('.ts') ? target : target + '.ts'));
  };
  new Function('exports', 'require', 'module', compiled)(
    module.exports, resolveDependency, module,
  );
  return module.exports;
}

module.exports = { loadSource };
