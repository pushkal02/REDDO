/**
 * REDDO Platform Management CLI
 * Manages all K8s services: backing infra + application services + telemetry observer
 * Runs cross-platform on Node.js (Windows, macOS, Linux)
 *
 * Usage:
 *   node scripts/manage_infra.js start            — Start all services
 *   node scripts/manage_infra.js stop             — Stop all services
 *   node scripts/manage_infra.js restart          — Restart all services
 *   node scripts/manage_infra.js start infra      — Start only backing infra
 *   node scripts/manage_infra.js start app        — Start only application services
 *   node scripts/manage_infra.js stop app         — Stop only application services
 *   node scripts/manage_infra.js status           — Print pod status table
 */

const { execSync } = require('child_process');

// ── colour helpers ────────────────────────────────────────────────────────────
const C = {
  reset:  '\x1b[0m',
  red:    '\x1b[31m',
  green:  '\x1b[32m',
  yellow: '\x1b[33m',
  cyan:   '\x1b[36m',
  blue:   '\x1b[34m',
  bold:   '\x1b[1m',
  dim:    '\x1b[2m',
};
const log  = (msg, col = C.reset) => console.log(`${col}${msg}${C.reset}`);
const info = (msg) => log(`  ℹ  ${msg}`, C.cyan);
const ok   = (msg) => log(`  ✔  ${msg}`, C.green);
const warn = (msg) => log(`  ⚠  ${msg}`, C.yellow);
const err  = (msg) => log(`  ✖  ${msg}`, C.red);

// ── resource groups ───────────────────────────────────────────────────────────
// Backing infrastructure (databases, message broker)
const INFRA_DEPLOYMENTS   = ['deployment/rabbitmq', 'deployment/redis'];
const INFRA_STATEFULSETS  = ['statefulset/postgres'];

// Application microservices
const APP_DEPLOYMENTS     = [
  'deployment/gate',
  'deployment/broker',
  'deployment/worker',
  'deployment/cannon',
  'deployment/probe',
];

// ── helpers ───────────────────────────────────────────────────────────────────
function run(cmd, { silent = false } = {}) {
  try {
    execSync(cmd, { stdio: silent ? 'pipe' : 'inherit' });
    return true;
  } catch (e) {
    err(`Command failed: ${cmd}`);
    err(e.message);
    return false;
  }
}

function scale(resources, replicas) {
  const targets = resources.join(' ');
  run(`kubectl scale ${targets} --replicas=${replicas} -n reddo`);
}

function sleep(ms) {
  try {
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
  } catch {
    const t = Date.now();
    while (Date.now() - t < ms) { /* busy wait fallback */ }
  }
}

function waitForReady(label, timeoutSecs = 90) {
  info(`Waiting for all pods to be ready (timeout: ${timeoutSecs}s)...`);
  const deadline = Date.now() + timeoutSecs * 1000;
  while (Date.now() < deadline) {
    try {
      execSync(
        `kubectl wait --for=condition=Ready pod --all -n reddo --timeout=5s`,
        { stdio: 'pipe' }
      );
      ok(`All pods in "${label}" are Ready.`);
      return;
    } catch { sleep(2000); }
  }
  warn('Timed out waiting for pods — some may still be starting.');
}

// ── actions ───────────────────────────────────────────────────────────────────
function startInfra() {
  log('\n📦  Starting backing infrastructure...', C.bold + C.cyan);
  scale([...INFRA_DEPLOYMENTS, ...INFRA_STATEFULSETS], 1);
  ok('RabbitMQ, Redis, PostgreSQL scaled to 1 replica.');
}

function stopInfra() {
  log('\n📦  Stopping backing infrastructure...', C.bold + C.yellow);
  scale([...INFRA_DEPLOYMENTS, ...INFRA_STATEFULSETS], 0);
  ok('RabbitMQ, Redis, PostgreSQL scaled to 0.');
}

function startApp() {
  log('\n🚀  Starting application services...', C.bold + C.cyan);
  scale(APP_DEPLOYMENTS, 1);
  ok('Gate, Broker, Worker, Cannon, Probe scaled to 1 replica.');
}

function stopApp() {
  log('\n🚀  Stopping application services...', C.bold + C.yellow);
  scale(APP_DEPLOYMENTS, 0);
  ok('Gate, Broker, Worker, Cannon, Probe scaled to 0.');
}

function showStatus() {
  log('\n📊  Current cluster state:', C.bold + C.blue);
  run('kubectl get pods,hpa -n reddo');
}

// ── arg parsing ───────────────────────────────────────────────────────────────
const [,, action, group] = process.argv;
const validActions = ['start', 'stop', 'restart', 'status'];
const validGroups  = ['infra', 'app', undefined];

if (!validActions.includes(action)) {
  err('Invalid usage.');
  console.log(`
${C.bold}Usage:${C.reset}
  node scripts/manage_infra.js <action> [group]

${C.bold}Actions:${C.reset}
  start           Start all services (infra + app)
  stop            Stop all services (app first, then infra)
  restart         Full restart (stop everything, then start everything)
  status          Print current pod and HPA status

${C.bold}Groups (optional — omit to target all):${C.reset}
  infra           Only backing infra (RabbitMQ, Redis, PostgreSQL)
  app             Only app services (Gate, Broker, Worker, Cannon, Probe)

${C.bold}Examples:${C.reset}
  node scripts/manage_infra.js start
  node scripts/manage_infra.js start infra
  node scripts/manage_infra.js stop app
  node scripts/manage_infra.js restart
  node scripts/manage_infra.js status
`);
  process.exit(1);
}

if (group && !validGroups.includes(group)) {
  err(`Unknown group "${group}". Use "infra" or "app" (or omit for all).`);
  process.exit(1);
}

// ── dispatch ──────────────────────────────────────────────────────────────────
log(`\n${C.bold}🔧  REDDO Platform Manager — action: ${action.toUpperCase()}${group ? ' (' + group + ')' : ''}${C.reset}`);

switch (action) {

  case 'start':
    if (!group || group === 'infra') startInfra();
    if (!group || group === 'app')   { sleep(2000); startApp(); }
    if (!group) waitForReady('reddo');
    showStatus();
    break;

  case 'stop':
    // Always stop app first so it doesn't error trying to reach infra
    if (!group || group === 'app')   stopApp();
    if (!group || group === 'infra') { sleep(3000); stopInfra(); }
    showStatus();
    break;

  case 'restart':
    // Stop app, then infra
    stopApp();
    sleep(3000);
    stopInfra();
    info('Waiting 8s for all pods to terminate...');
    sleep(8000);
    // Start infra first, then app
    startInfra();
    info('Waiting 10s for infra to initialise before starting app services...');
    sleep(10000);
    startApp();
    waitForReady('reddo');
    // Rolling restart HPAs just to reset metrics windows
    info('Triggering rolling restart on HPAs...');
    run('kubectl rollout restart deployment/gate deployment/broker deployment/worker deployment/cannon deployment/probe -n reddo', { silent: false });
    showStatus();
    break;

  case 'status':
    showStatus();
    break;
}

log('\n✅  Done.\n', C.green + C.bold);
