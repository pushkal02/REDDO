/**
 * REDDO Port Forwarding Manager CLI
 * Runs cross-platform on Node.js (Windows, macOS, Linux)
 * Usage: node scripts/port_forward.js
 */

const { spawn, execSync } = require('child_process');

// Define resources to port forward: [Type/Name, localPort:remotePort]
const targets = [
    { name: 'statefulset/postgres', ports: ['5432:5432'] },
    { name: 'deployment/redis', ports: ['6379:6379'] },
    { name: 'deployment/rabbitmq', ports: ['5672:5672', '15672:15672'] },
    { name: 'service/gate', ports: ['8080:8080'] },
    { name: 'service/cannon', ports: ['8083:8083'] }
];

// Check if namespace reddo has running pods first
try {
    const pods = execSync('kubectl get pods -n reddo --no-headers', { encoding: 'utf8' });
    if (!pods || pods.trim() === '') {
        console.error('\x1b[31mError: No pods found in namespace "reddo". Please start services first:\x1b[0m');
        console.error('  node scripts/manage_infra.js start');
        process.exit(1);
    }
} catch (error) {
    console.error(`\x1b[31mError checking Kubernetes cluster: ${error.message}\x1b[0m`);
    process.exit(1);
}

console.log('\x1b[36mStarting port forwarding for REDDO services...\x1b[0m');

const processes = [];

targets.forEach(target => {
    target.ports.forEach(portPair => {
        console.log(`\x1b[32mForwarding ${target.name} ports: ${portPair}\x1b[0m`);
        
        // Spawn kubectl port-forward as child process
        const child = spawn('kubectl', [
            'port-forward',
            target.name,
            portPair,
            '-n',
            'reddo'
        ], { stdio: 'ignore' });

        child.on('error', (err) => {
            console.error(`\x1b[31mFailed to start port-forward for ${target.name}: ${err.message}\x1b[0m`);
        });

        processes.push(child);
    });
});

console.log('\x1b[35mPort forwarding active. Press Ctrl+C to terminate all forwards.\x1b[0m');

// Handle graceful exit to clean up spawned background port-forward operations
process.on('SIGINT', () => {
    console.log('\n\x1b[33mTerminating all port-forwarding processes...\x1b[0m');
    processes.forEach(p => p.kill());
    process.exit(0);
});

process.on('SIGTERM', () => {
    processes.forEach(p => p.kill());
    process.exit(0);
});

// Keep script alive
setInterval(() => {}, 1000);
