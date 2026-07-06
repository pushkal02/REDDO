/**
 * REDDO Backing Services Management CLI
 * Runs cross-platform on Node.js (Windows, macOS, Linux)
 * Usage: node scripts/manage_infra.js [start|stop|restart]
 */

const { execSync } = require('child_process');

const action = process.argv[2];

if (!action || !['start', 'stop', 'restart'].includes(action)) {
    console.error('Usage: node scripts/manage_infra.js [start|stop|restart]');
    process.exit(1);
}

function runKubectl(replicas) {
    const cmd = `kubectl scale deployment/rabbitmq deployment/redis statefulset/postgres --replicas=${replicas} -n reddo`;
    try {
        execSync(cmd, { stdio: 'inherit' });
    } catch (error) {
        console.error(`\x1b[31mError scaling resources: ${error.message}\x1b[0m`);
        process.exit(1);
    }
}

function sleep(ms) {
    try {
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
    } catch (e) {
        const start = Date.now();
        while (Date.now() - start < ms) {}
    }
}

switch (action) {
    case 'stop':
        console.log('\x1b[33mStopping all backing services in namespace "reddo" (scaling to 0)... \x1b[0m');
        runKubectl(0);
        break;
    case 'start':
        console.log('\x1b[32mStarting all backing services in namespace "reddo" (scaling to 1)... \x1b[0m');
        runKubectl(1);
        break;
    case 'restart':
        console.log('\x1b[36mRestarting backing services in namespace "reddo"... \x1b[0m');
        console.log('\x1b[33mScaling down to 0... \x1b[0m');
        runKubectl(0);
        
        console.log('Waiting 5 seconds for pods to terminate...');
        sleep(5000);
        
        console.log('\x1b[32mScaling up to 1... \x1b[0m');
        runKubectl(1);
        break;
}
