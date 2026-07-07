/**
 * REDDO Port Forwarding Manager CLI
 * Runs cross-platform on Node.js (Windows, macOS, Linux)
 *
 * Forwards all REDDO service ports to localhost so the local
 * browser and tools can reach the K8s cluster without ingress.
 *
 * Usage: node scripts/port_forward.js
 *
 * Services forwarded:
 *   PostgreSQL   localhost:5432  → pod:5432
 *   Redis        localhost:6379  → pod:6379
 *   RabbitMQ     localhost:5672  → pod:5672   (AMQP)
 *                localhost:15672 → pod:15672  (Management UI)
 *   Gate         localhost:8080  → pod:8080   (API Gateway)
 *   Broker       localhost:8081  → pod:8081   (Java Saga Broker)
 *   Worker       localhost:8084  → pod:8084   (Node.js I/O Worker)
 *   Cannon       localhost:8083  → pod:8083   (Chaos Cannon API)
 *   Probe        localhost:8082  → pod:8082   (Telemetry Observer & Dashboard)
 */

const { spawn, execSync } = require('child_process');

const C = {
  reset:  '\x1b[0m',
  red:    '\x1b[31m',
  green:  '\x1b[32m',
  yellow: '\x1b[33m',
  cyan:   '\x1b[36m',
  purple: '\x1b[35m',
  bold:   '\x1b[1m',
};

// ── targets ───────────────────────────────────────────────────────────────────
// Each entry: { name: 'kind/name', ports: ['local:remote', ...] }
// 'kind' is deployment|statefulset|service — kubectl port-forward accepts all.
const targets = [
  // ── Backing infrastructure ──────────────────────────────────────────────────
  { label: 'PostgreSQL',      name: 'statefulset/postgres',   ports: ['5432:5432']          },
  { label: 'Redis',           name: 'deployment/redis',       ports: ['6379:6379']          },
  { label: 'RabbitMQ (AMQP)',  name: 'deployment/rabbitmq',   ports: ['5672:5672']          },
  { label: 'RabbitMQ (UI)',    name: 'deployment/rabbitmq',   ports: ['15672:15672']        },

  // ── Application services ────────────────────────────────────────────────────
  { label: 'Gate (API GW)',   name: 'service/gate',           ports: ['8080:8080']          },
  { label: 'Broker (Saga)',   name: 'service/broker',         ports: ['8081:8081']          },
  { label: 'Worker (Node)',   name: 'service/worker',         ports: ['8084:8084']          },
  { label: 'Cannon (Chaos)',  name: 'service/cannon',         ports: ['8083:8083']          },
  { label: 'Probe (HUD)',     name: 'service/probe',          ports: ['8082:8082']          },
];

// ── pre-flight checks ─────────────────────────────────────────────────────────
console.log(`\n${C.bold}${C.cyan}🔌  REDDO Port Forward Manager${C.reset}\n`);

try {
  const pods = execSync('kubectl get pods -n reddo --no-headers 2>&1', { encoding: 'utf8' });
  const running = pods.split('\n').filter(l => l.includes('Running'));
  if (running.length === 0) {
    console.warn(`${C.yellow}  ⚠  No Running pods found in namespace "reddo".${C.reset}`);
    console.warn(`${C.yellow}     Start services first:  node scripts/manage_infra.js start${C.reset}\n`);
  } else {
    console.log(`${C.green}  ✔  Found ${running.length} Running pod(s) in namespace "reddo".${C.reset}`);
  }
} catch (e) {
  console.error(`${C.red}  ✖  Cannot reach Kubernetes cluster: ${e.message}${C.reset}`);
  process.exit(1);
}

// ── spawn port-forward processes ──────────────────────────────────────────────
const procs = [];
let maxLabel = Math.max(...targets.map(t => t.label.length));

targets.forEach(({ label, name, ports }) => {
  ports.forEach(portPair => {
    const padded = label.padEnd(maxLabel);
    console.log(`  ${C.green}↪${C.reset}  ${C.bold}${padded}${C.reset}  ${portPair}  ${C.dim}(${name})${C.reset}`);

    const child = spawn('kubectl', ['port-forward', name, portPair, '-n', 'reddo'], {
      stdio: 'ignore',
    });

    child.on('error', e => {
      console.error(`${C.red}  ✖  Failed to start port-forward for ${name}: ${e.message}${C.reset}`);
    });

    child.on('exit', (code, signal) => {
      if (code !== 0 && signal !== 'SIGTERM' && signal !== 'SIGINT') {
        console.warn(`${C.yellow}  ⚠  port-forward exited for ${name} (code ${code}, signal ${signal})${C.reset}`);
      }
    });

    procs.push(child);
  });
});

console.log(`\n${C.purple}  Port forwarding active. Press Ctrl+C to terminate all.${C.reset}\n`);

// ── quick URL summary ─────────────────────────────────────────────────────────
console.log(`${C.dim}  Dashboard:      http://localhost:8082`);
console.log(`  API Gateway:    http://localhost:8080`);
console.log(`  Chaos Cannon:   http://localhost:8083`);
console.log(`  RabbitMQ UI:    http://localhost:15672  (guest / guest)${C.reset}\n`);

// ── graceful shutdown ─────────────────────────────────────────────────────────
const shutdown = (sig) => {
  console.log(`\n${C.yellow}  Terminating all port-forward processes (${sig})...${C.reset}`);
  procs.forEach(p => { try { p.kill(); } catch { /* already dead */ } });
  process.exit(0);
};

process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Keep alive
setInterval(() => {}, 1000);
