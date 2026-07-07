import React, { useState, useEffect, useRef } from 'react';
import {
  Activity,
  Flame,
  ShieldCheck,
  AlertTriangle,
  RotateCw,
  Terminal,
  Maximize2,
  Minimize2,
  Trash2,
  Search,
  CheckCircle,
  Play,
  XSquare,
  Settings
} from 'lucide-react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  Filler
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  Filler
);

interface PodTelemetry {
  name: string;
  status: string;
  restarts: number;
  cpu: string;
  memory: string;
  age: string;
}

interface TelemetryPayload {
  pods: PodTelemetry[];
  rabbitmq: {
    java_queue_depth: number;
    node_queue_depth: number;
  };
  timestamp: string;
}

interface LogLine {
  pod: string;
  service: string;
  message: string;
  timestamp: string;
}

const components = ['gate', 'broker', 'worker', 'cannon', 'probe', 'rabbitmq', 'postgres', 'redis'];
const componentColors: Record<string, string> = {
  gate: '#ec4899',
  broker: '#a855f7',
  worker: '#06b6d4',
  cannon: '#ef4444',
  probe: '#3b82f6',
  rabbitmq: '#f59e0b',
  postgres: '#10b981',
  redis: '#64748b'
};

const maxDataPoints = 30;

export default function App() {
  const [activeTab, setActiveTab] = useState<'metrics' | 'logs'>('metrics');
  const [cannonURL, setCannonURL] = useState('http://localhost:8083');
  const [cannonStatus, setCannonStatus] = useState<any>({
    status: 'IDLE',
    progress: 0,
    total: 0,
    success_rate: 1.0,
    seed: 42561
  });
  const [isCannonConnected, setIsCannonConnected] = useState(true);
  const [isStreamActive, setIsStreamActive] = useState(true);
  const [isLogsActive, setIsLogsActive] = useState(true);
  const [masterEngineStatus, setMasterEngineStatus] = useState('SAGA ENGINE OPERATING NORMALLY');

  // Blaster parameters
  const [totalRequests, setTotalRequests] = useState(300);
  const [concurrency, setConcurrency] = useState(15);
  const [zombieHang, setZombieHang] = useState(5);
  const [fatalCrash, setFatalCrash] = useState(2);
  const [businessFail, setBusinessFail] = useState(5);
  const [seed, setSeed] = useState(42561);
  const [scheduleDelay, setScheduleDelay] = useState(0);
  const [templates, setTemplates] = useState({
    valid_dag: true,
    saga_rollback: true,
    saga_compensation: true
  });

  // Telemetry buffer states
  const [pods, setPods] = useState<PodTelemetry[]>([]);
  const [javaQueueDepth, setJavaQueueDepth] = useState(0);
  const [nodeQueueDepth, setNodeQueueDepth] = useState(0);

  // Dynamic metrics history
  const [history, setHistory] = useState<{
    javaQueue: number[];
    nodeQueue: number[];
    componentsCPU: Record<string, number[]>;
    componentsMem: Record<string, number[]>;
  }>(() => {
    const initCPU: Record<string, number[]> = {};
    const initMem: Record<string, number[]> = {};
    components.forEach(c => {
      initCPU[c] = Array(maxDataPoints).fill(0);
      initMem[c] = Array(maxDataPoints).fill(0);
    });
    return {
      javaQueue: Array(maxDataPoints).fill(0),
      nodeQueue: Array(maxDataPoints).fill(0),
      componentsCPU: initCPU,
      componentsMem: initMem
    };
  });

  // Collective log states
  const [logs, setLogs] = useState<LogLine[]>([]);
  const [logsFilter, setLogsFilter] = useState<string>('all');
  const [logsSearch, setLogsSearch] = useState('');
  const [logsAutoscroll, setLogsAutoscroll] = useState(true);

  // Full screen maximize charts state
  const [maximizedChart, setMaximizedChart] = useState<string | null>(null);

  // Refs
  const fullLogsEndRef = useRef<HTMLDivElement>(null);
  const localLogsEndRef = useRef<HTMLDivElement>(null);

  // Component mapping helper
  const getComponentFromPodName = (podName: string) => {
    if (podName.startsWith('postgres')) return 'postgres';
    if (podName.startsWith('gate-')) return 'gate';
    if (podName.startsWith('broker-')) return 'broker';
    if (podName.startsWith('worker-')) return 'worker';
    if (podName.startsWith('cannon-')) return 'cannon';
    if (podName.startsWith('probe-')) return 'probe';
    if (podName.startsWith('rabbitmq-')) return 'rabbitmq';
    if (podName.startsWith('redis-')) return 'redis';
    return null;
  };

  // Connect to Telemetry SSE Stream
  useEffect(() => {
    const sse = new EventSource('/api/v1/telemetry/stream');
    
    sse.onopen = () => {
      setIsStreamActive(true);
      logLocalEvent('Telemetry stream channel active.', 'system');
    };

    sse.onerror = () => {
      setIsStreamActive(false);
    };

    sse.onmessage = (event) => {
      try {
        const telemetry: TelemetryPayload = JSON.parse(event.data);
        
        // Aggregate multi-replica metrics
        const tickCPU: Record<string, number> = {};
        const tickMem: Record<string, number> = {};
        const tickStatus: Record<string, string> = {};
        const tickRestarts: Record<string, number> = {};
        const tickReplicas: Record<string, number> = {};

        components.forEach(c => {
          tickCPU[c] = 0;
          tickMem[c] = 0;
          tickStatus[c] = 'IDLE';
          tickRestarts[c] = 0;
          tickReplicas[c] = 0;
        });

        let engineCompromised = false;

        telemetry.pods.forEach(pod => {
          const comp = getComponentFromPodName(pod.name);
          if (comp) {
            const cpuVal = parseInt(pod.cpu.replace('m', '')) || 0;
            const memVal = parseInt(pod.memory.replace('Mi', '')) || 0;
            
            tickCPU[comp] += cpuVal;
            tickMem[comp] += memVal;
            tickReplicas[comp]++;
            tickRestarts[comp] += pod.restarts;

            if (pod.status.includes('CrashLoop') || pod.status.includes('Error') || pod.status.includes('OOM') || tickStatus[comp] === 'IDLE') {
              tickStatus[comp] = pod.status;
            }

            if (pod.status.includes('CrashLoop') || pod.status.includes('Error') || pod.status.includes('OOM')) {
              engineCompromised = true;
              logLocalEvent(`[Incident] Pod ${pod.name} in state: ${pod.status}! (Restarts: ${pod.restarts})`, 'error');
            }
          }
        });

        // Update current stats
        setPods(telemetry.pods);
        setJavaQueueDepth(telemetry.rabbitmq.java_queue_depth);
        setNodeQueueDepth(telemetry.rabbitmq.node_queue_depth);

        // Update master status banner
        if (engineCompromised) {
          setMasterEngineStatus('SAGA ENGINE IN DISTRESS ⚠️');
        } else if (cannonStatus.status === 'RUNNING') {
          setMasterEngineStatus('CHAOS CYCLONE ACTIVE 💥');
        } else {
          setMasterEngineStatus('SAGA ENGINE OPERATING NORMALLY');
        }

        // Push to dynamic history arrays
        setHistory(prev => {
          const nextJavaQueue = [...prev.javaQueue.slice(1), telemetry.rabbitmq.java_queue_depth];
          const nextNodeQueue = [...prev.nodeQueue.slice(1), telemetry.rabbitmq.node_queue_depth];
          
          const nextCPU: Record<string, number[]> = {};
          const nextMem: Record<string, number[]> = {};

          components.forEach(c => {
            nextCPU[c] = [...prev.componentsCPU[c].slice(1), tickCPU[c]];
            nextMem[c] = [...prev.componentsMem[c].slice(1), tickMem[c]];
          });

          return {
            javaQueue: nextJavaQueue,
            nodeQueue: nextNodeQueue,
            componentsCPU: nextCPU,
            componentsMem: nextMem
          };
        });

      } catch (err) {
        console.error('SSE parsing error', err);
      }
    };

    return () => sse.close();
  }, [cannonStatus.status]);

  // Connect to Logs SSE Stream
  useEffect(() => {
    const sse = new EventSource('/api/v1/logs/stream');
    
    sse.onopen = () => {
      setIsLogsActive(true);
      logLocalEvent('Realtime cluster logs stream active.', 'system');
    };

    sse.onerror = () => {
      setIsLogsActive(false);
    };

    sse.onmessage = (event) => {
      try {
        const logLine: LogLine = JSON.parse(event.data);
        logLine.timestamp = new Date().toLocaleTimeString();

        setLogs(prev => {
          const next = [...prev, logLine];
          if (next.length > 500) {
            next.shift();
          }
          return next;
        });

      } catch (err) {
        console.error('Logs SSE parse error', err);
      }
    };

    return () => sse.close();
  }, []);

  // Poll Chaos Cannon Status
  useEffect(() => {
    const pollStatus = async () => {
      try {
        const res = await fetch(`${cannonURL}/api/v1/chaos/status`);
        if (!res.ok) throw new Error();
        const data = await res.json();
        setCannonStatus(data);
        setIsCannonConnected(true);
      } catch (err) {
        setIsCannonConnected(false);
        setCannonStatus({ status: 'UNKNOWN', progress: 0, total: 0, success_rate: 1.0, seed: 0 });
      }
    };

    pollStatus();
    const interval = setInterval(pollStatus, 1000);
    return () => clearInterval(interval);
  }, [cannonURL]);

  // Autoscroll logic
  useEffect(() => {
    if (logsAutoscroll) {
      fullLogsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, activeTab, logsAutoscroll, logsFilter, logsSearch]);

  // Client Side Local Event Logging
  const [localLogs, setLocalLogs] = useState<string[]>([]);
  const logLocalEvent = (message: string, type: 'info' | 'error' | 'system' = 'info') => {
    const ts = new Date().toLocaleTimeString();
    const line = `[${ts}] [${type.toUpperCase()}] ${message}`;
    setLocalLogs(prev => {
      const next = [...prev, line];
      if (next.length > 40) next.shift();
      return next;
    });
    // Scroll local console
    setTimeout(() => {
      localLogsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, 50);
  };

  // Launch Chaos Cannon Blast
  const fireChaosCannon = async () => {
    const activeTemplates = Object.entries(templates)
      .filter(([_, active]) => active)
      .map(([name]) => name);

    if (activeTemplates.length === 0) {
      alert('Please check at least one workflow template to trigger load!');
      return;
    }

    const payload = {
      total_requests: totalRequests,
      concurrency: concurrency,
      zombie_hang_probability: zombieHang / 100.0,
      fatal_crash_probability: fatalCrash / 100.0,
      business_fail_probability: businessFail / 100.0,
      workflow_templates: activeTemplates,
      seed: seed,
      schedule_delay_seconds: scheduleDelay
    };

    logLocalEvent(`Dispatched Fire call. Seed: ${payload.seed}, Delay: ${payload.schedule_delay_seconds}s`, 'system');

    try {
      const res = await fetch(`${cannonURL}/api/v1/chaos/fire`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(`Server returned HTTP ${res.status}`);
      const data = await res.json();
      logLocalEvent(`Cannon trigger acknowledged: ${data.message}`, 'system');
    } catch (err: any) {
      logLocalEvent(`Cannon fire request failed: ${err.message}`, 'error');
    }
  };

  // Stop Chaos Blast
  const stopChaosBlast = async () => {
    logLocalEvent('Dispatched stop blast request...', 'system');
    try {
      const res = await fetch(`${cannonURL}/api/v1/chaos/stop`, { method: 'POST' });
      const data = await res.json();
      logLocalEvent(`Cannon stop call response: ${data.message}`, 'system');
    } catch (err: any) {
      logLocalEvent(`Cannon stop call failed: ${err.message}`, 'error');
    }
  };

  // Helper to generate distinct chart configurations
  const getCombinedQueueChartData = () => ({
    labels: chartLabels,
    datasets: [
      { label: 'Java Queue', data: history.javaQueue, borderColor: '#bd00ff', borderWidth: 2, tension: 0.3, pointRadius: 0, fill: false },
      { label: 'Node Queue', data: history.nodeQueue, borderColor: '#00f0ff', borderWidth: 2, tension: 0.3, pointRadius: 0, fill: false }
    ]
  });

  const getCombinedCPUChartData = () => ({
    labels: chartLabels,
    datasets: components.map(c => ({
      label: c,
      data: history.componentsCPU[c],
      borderColor: componentColors[c],
      borderWidth: 1.5,
      tension: 0.3,
      pointRadius: 0,
      fill: false
    }))
  });

  const getCombinedMemChartData = () => ({
    labels: chartLabels,
    datasets: components.map(c => ({
      label: c,
      data: history.componentsMem[c],
      borderColor: componentColors[c],
      borderWidth: 1.5,
      tension: 0.3,
      pointRadius: 0,
      fill: false
    }))
  });

  const getSingleServiceChartData = (compName: string) => ({
    labels: chartLabels,
    datasets: [
      { label: 'CPU (m)', data: history.componentsCPU[compName], borderColor: componentColors[compName], borderWidth: 1.8, tension: 0.3, pointRadius: 0, fill: false, yAxisID: 'yCPU' },
      { label: 'Mem (Mi)', data: history.componentsMem[compName], borderColor: '#bd00ff', borderWidth: 1.2, tension: 0.3, pointRadius: 0, borderDash: [2, 2], fill: false, yAxisID: 'yMem' }
    ]
  });

  const chartLabels = Array(maxDataPoints).fill('');

  const commonChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false } },
    scales: {
      x: { display: false },
      y: {
        grid: { color: 'rgba(255, 255, 255, 0.02)' },
        ticks: { color: '#6e7d95', font: { family: 'JetBrains Mono', size: 9 } }
      }
    }
  };

  const getSingleServiceChartOptions = () => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false } },
    scales: {
      x: { display: false },
      yCPU: {
        display: true,
        type: 'linear' as const,
        position: 'left' as const,
        grid: { color: 'rgba(255, 255, 255, 0.01)' },
        ticks: { color: '#6e7d95', font: { family: 'JetBrains Mono', size: 8 } }
      },
      yMem: {
        display: true,
        type: 'linear' as const,
        position: 'right' as const,
        grid: { display: false },
        ticks: { color: '#6e7d95', font: { family: 'JetBrains Mono', size: 8 } }
      }
    }
  });

  // Filtering collective logs
  const getFilteredLogs = () => {
    return logs.filter(log => {
      const matchFilter =
        logsFilter === 'all' ||
        (logsFilter === 'infra' && ['postgres', 'redis', 'rabbitmq'].includes(log.service)) ||
        log.service === logsFilter;

      const matchSearch =
        logsSearch.trim() === '' ||
        log.message.toLowerCase().includes(logsSearch.toLowerCase());

      return matchFilter && matchSearch;
    });
  };

  const logBadgeColors: Record<string, string> = {
    gate: 'bg-pink-500/10 border-pink-500/30 text-pink-400',
    broker: 'bg-purple-500/10 border-purple-500/30 text-purple-400',
    worker: 'bg-cyan-500/10 border-cyan-500/30 text-cyan-400',
    cannon: 'bg-red-500/10 border-red-500/30 text-red-400',
    probe: 'bg-blue-500/10 border-blue-500/30 text-blue-400',
    rabbitmq: 'bg-amber-500/10 border-amber-500/30 text-amber-400',
    postgres: 'bg-emerald-500/10 border-emerald-500/30 text-emerald-400',
    redis: 'bg-slate-500/10 border-slate-500/30 text-slate-400'
  };

  return (
    <div className="flex flex-col min-h-screen text-slate-100 font-sans">
      
      {/* TOP HEADER */}
      <header className="sticky top-0 z-40 flex items-center justify-between px-6 py-3 bg-cyber-black/95 backdrop-blur-md border-b border-white/5 shadow-md">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-cyber-purple to-cyber-cyan shadow-[0_0_12px_rgba(0,240,255,0.35)] relative after:content-[''] after:absolute after:top-1/4 after:left-1/4 after:w-1/2 after:h-1/2 after:bg-black after:rounded-full"></div>
          <div>
            <h1 className="text-lg font-black tracking-widest bg-gradient-to-r from-white via-slate-200 to-slate-400 bg-clip-text text-transparent">REDDO SAGA PLATFORM</h1>
            <p className="text-[10px] text-cyber-cyan font-mono tracking-wider">REALTIME OBSERVABILITY & CHAOS HUD</p>
          </div>
          <div className={`ml-4 px-3 py-0.5 rounded-full text-[10px] font-mono font-bold tracking-wider transition-all border ${
            masterEngineStatus.includes('NORMAL') 
              ? 'bg-cyber-green/5 border-cyber-green/20 text-cyber-green' 
              : 'bg-cyber-red/10 border-cyber-red/30 text-cyber-red status-pulse-red'
          }`}>
            {masterEngineStatus}
          </div>
        </div>

        {/* TABS SELECT */}
        <div className="flex bg-white/5 p-1 rounded-lg border border-white/5">
          <button 
            className={`px-4 py-1 rounded-md text-xs font-bold tracking-wider transition-all ${
              activeTab === 'metrics' 
                ? 'bg-cyber-cyan/15 text-cyber-cyan border border-cyber-cyan/20' 
                : 'text-slate-400 hover:text-slate-200'
            }`}
            onClick={() => setActiveTab('metrics')}
          >
            📊 Observability Deck
          </button>
          <button 
            className={`px-4 py-1 rounded-md text-xs font-bold tracking-wider transition-all ${
              activeTab === 'logs' 
                ? 'bg-cyber-cyan/15 text-cyber-cyan border border-cyber-cyan/20' 
                : 'text-slate-400 hover:text-slate-200'
            }`}
            onClick={() => setActiveTab('logs')}
          >
            📟 Log Terminal Deck
          </button>
        </div>

        {/* CONNECTION STATE */}
        <div className="flex items-center gap-4 text-xs font-mono">
          <div className="flex items-center gap-2 bg-white/2 px-2 py-1 rounded-lg border border-white/5 text-[11px]">
            <span className="text-slate-400 font-semibold uppercase">Cannon URL:</span>
            <input 
              type="text" 
              value={cannonURL} 
              onChange={(e) => setCannonURL(e.target.value)} 
              className="bg-black/50 border border-white/5 rounded px-2 py-0.5 w-36 text-white text-xs outline-none focus:border-cyber-cyan"
            />
          </div>
          <div className="flex items-center gap-2 bg-white/2 px-2.5 py-1 rounded-lg border border-white/5 text-[11px]">
            <div className={`w-2 h-2 rounded-full ${isCannonConnected ? 'bg-cyber-green shadow-[0_0_6px_#00ff66]' : 'bg-cyber-red shadow-[0_0_6px_#ff0055]'}`}></div>
            <span>CANNON: <b className={isCannonConnected ? 'text-cyber-green' : 'text-cyber-red'}>{isCannonConnected ? 'CONNECTED' : 'DISCONNECTED'}</b></span>
          </div>
          <div className="flex items-center gap-2 bg-white/2 px-2.5 py-1 rounded-lg border border-white/5 text-[11px]">
            <div className={`w-2 h-2 rounded-full ${isStreamActive ? 'bg-cyber-green shadow-[0_0_6px_#00ff66]' : 'bg-cyber-red shadow-[0_0_6px_#ff0055]'}`}></div>
            <span>STREAM: <b className={isStreamActive ? 'text-cyber-green' : 'text-cyber-red'}>{isStreamActive ? 'ACTIVE' : 'DISCONNECTED'}</b></span>
          </div>
        </div>
      </header>

      {/* DECK 1: METRICS OBSERVABILITY VIEW */}
      {activeTab === 'metrics' && (
        <main className="flex-1 grid grid-cols-[330px_1fr] gap-3 p-3 max-w-[1920px] mx-auto w-full overflow-hidden">
          
          {/* SIDEBAR: CHAOS CONSOLE */}
          <div className="flex flex-col gap-3.5 bg-cyber-card backdrop-blur-xl border border-white/5 rounded-xl p-4 shadow-xl relative overflow-hidden text-xs">
            <div className="flex justify-between items-center border-b border-white/5 pb-1">
              <h2 className="text-xs font-bold text-slate-400 uppercase tracking-widest">Chaos Console</h2>
              <Settings className="w-3.5 h-3.5 text-slate-500" />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <div className="flex flex-col gap-0.5">
                <label className="text-[10px] text-slate-400 font-bold uppercase">Workflows</label>
                <input 
                  type="number" 
                  value={totalRequests} 
                  onChange={(e) => setTotalRequests(parseInt(e.target.value) || 0)} 
                  className="bg-black/50 border border-white/5 rounded-lg px-2.5 py-1.5 text-white font-mono outline-none focus:border-cyber-cyan"
                />
              </div>
              <div className="flex flex-col gap-0.5">
                <label className="text-[10px] text-slate-400 font-bold uppercase">Concurrency</label>
                <input 
                  type="number" 
                  value={concurrency} 
                  onChange={(e) => setConcurrency(parseInt(e.target.value) || 0)} 
                  className="bg-black/50 border border-white/5 rounded-lg px-2.5 py-1.5 text-white font-mono outline-none focus:border-cyber-cyan"
                />
              </div>
            </div>

            <div className="flex flex-col gap-1">
              <label className="text-[10px] text-slate-400 font-bold uppercase">Workflow Template Mix</label>
              <div className="flex flex-col gap-1 bg-black/35 p-2 rounded-lg border border-white/5">
                <label className="flex items-center gap-2 cursor-pointer select-none text-slate-200">
                  <input 
                    type="checkbox" 
                    checked={templates.valid_dag} 
                    onChange={(e) => setTemplates(prev => ({ ...prev, valid_dag: e.target.checked }))} 
                    className="accent-cyber-cyan w-3.5 h-3.5"
                  />
                  <span>Valid Workflow Flow</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer select-none text-slate-200">
                  <input 
                    type="checkbox" 
                    checked={templates.saga_rollback} 
                    onChange={(e) => setTemplates(prev => ({ ...prev, saga_rollback: e.target.checked }))} 
                    className="accent-cyber-cyan w-3.5 h-3.5"
                  />
                  <span>Saga Rollback Test</span>
                </label>
                <label className="flex items-center gap-2 cursor-pointer select-none text-slate-200">
                  <input 
                    type="checkbox" 
                    checked={templates.saga_compensation} 
                    onChange={(e) => setTemplates(prev => ({ ...prev, saga_compensation: e.target.checked }))} 
                    className="accent-cyber-cyan w-3.5 h-3.5"
                  />
                  <span>Saga Compensation Test</span>
                </label>
              </div>
            </div>

            {/* Sliders */}
            <div className="flex flex-col gap-2.5">
              <div className="flex flex-col">
                <div className="flex justify-between text-[10px] font-bold uppercase mb-0.5">
                  <span className="text-slate-400 font-semibold">Zombie Hang</span>
                  <span className="text-cyber-cyan font-mono">{zombieHang}%</span>
                </div>
                <input 
                  type="range" 
                  min="0" 
                  max="100" 
                  value={zombieHang} 
                  onChange={(e) => setZombieHang(parseInt(e.target.value))} 
                  className="w-full h-1 bg-white/10 rounded-lg appearance-none cursor-pointer accent-cyber-cyan"
                />
              </div>

              <div className="flex flex-col">
                <div className="flex justify-between text-[10px] font-bold uppercase mb-0.5">
                  <span className="text-slate-400 font-semibold">Fatal Crash</span>
                  <span className="text-cyber-cyan font-mono">{fatalCrash}%</span>
                </div>
                <input 
                  type="range" 
                  min="0" 
                  max="100" 
                  value={fatalCrash} 
                  onChange={(e) => setFatalCrash(parseInt(e.target.value))} 
                  className="w-full h-1 bg-white/10 rounded-lg appearance-none cursor-pointer accent-cyber-cyan"
                />
              </div>

              <div className="flex flex-col">
                <div className="flex justify-between text-[10px] font-bold uppercase mb-0.5">
                  <span className="text-slate-400 font-semibold">Business Failure</span>
                  <span className="text-cyber-cyan font-mono">{businessFail}%</span>
                </div>
                <input 
                  type="range" 
                  min="0" 
                  max="100" 
                  value={businessFail} 
                  onChange={(e) => setBusinessFail(parseInt(e.target.value))} 
                  className="w-full h-1 bg-white/10 rounded-lg appearance-none cursor-pointer accent-cyber-cyan"
                />
              </div>
            </div>

            <div className="grid grid-cols-[1fr_auto] gap-2 items-end">
              <div className="flex flex-col gap-0.5">
                <label className="text-[10px] text-slate-400 font-bold uppercase" htmlFor="blast-seed">Seed</label>
                <input 
                  type="number" 
                  value={seed} 
                  onChange={(e) => setSeed(parseInt(e.target.value) || 0)} 
                  className="bg-black/50 border border-white/5 rounded-lg px-2.5 py-1 text-white font-mono outline-none focus:border-cyber-cyan text-xs"
                />
              </div>
              <button 
                className="bg-white/5 border border-white/5 hover:bg-white/10 text-white text-[10px] font-bold py-1.5 px-2.5 rounded-lg transition-all"
                onClick={() => setSeed(Math.floor(Math.random() * 90000) + 10000)}
              >
                🎲 Random
              </button>
            </div>

            <div className="flex flex-col gap-0.5">
              <label className="text-[10px] text-slate-400 font-bold uppercase">Delayed Start (Secs)</label>
              <input 
                type="number" 
                value={scheduleDelay} 
                onChange={(e) => setScheduleDelay(parseInt(e.target.value) || 0)} 
                className="bg-black/50 border border-white/5 rounded-lg px-2.5 py-1 text-white font-mono outline-none focus:border-cyber-cyan text-xs"
              />
            </div>

            {/* Action buttons */}
            <div className="flex flex-col gap-1.5 mt-1">
              <button 
                className="w-full py-2 bg-gradient-to-r from-cyber-red to-rose-700 text-white font-black tracking-widest text-[11px] rounded-lg shadow-[0_0_8px_rgba(255,0,85,0.25)] hover:shadow-[0_0_15px_rgba(255,0,85,0.55)] hover:-translate-y-0.5 transition-all duration-200 disabled:opacity-20 disabled:pointer-events-none"
                onClick={fireChaosCannon}
                disabled={!isCannonConnected || cannonStatus.status === 'RUNNING'}
              >
                FIRE CANNON 💥
              </button>
              <button 
                className="w-full py-1.5 bg-gradient-to-r from-cyber-amber to-amber-700 text-white font-bold text-[10px] rounded-lg hover:shadow-[0_0_10px_rgba(255,170,0,0.3)] disabled:opacity-10 disabled:pointer-events-none transition-all duration-200"
                onClick={stopChaosBlast}
                disabled={cannonStatus.status !== 'RUNNING' && cannonStatus.status !== 'SCHEDULED'}
              >
                CANCEL BLAST 🛑
              </button>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-2 gap-2 bg-black/40 p-2.5 rounded-lg border border-white/5 mt-auto">
              <div className="flex flex-col">
                <span className="text-[9px] text-slate-500 font-bold uppercase">Status</span>
                <span className={`text-[11px] font-mono font-black ${cannonStatus.status === 'RUNNING' ? 'text-cyber-red animate-pulse' : 'text-slate-300'}`}>{cannonStatus.status}</span>
              </div>
              <div className="flex flex-col">
                <span className="text-[9px] text-slate-500 font-bold uppercase">Success</span>
                <span className="text-[11px] font-mono font-black text-cyber-green">{(cannonStatus.success_rate * 100).toFixed(1)}%</span>
              </div>
              <div className="col-span-2 flex flex-col gap-1 pt-1.5 border-t border-white/5">
                <div className="flex justify-between items-center text-[9px] text-slate-500 font-bold uppercase">
                  <span>Progress</span>
                  <span className="font-mono text-slate-300">{cannonStatus.progress} / {cannonStatus.total}</span>
                </div>
                <div className="w-full h-1.5 bg-white/5 rounded-full overflow-hidden border border-white/5">
                  <div 
                    className="h-full bg-gradient-to-r from-cyber-cyan to-cyber-purple shadow-[0_0_6px_#00f0ff] transition-all duration-300"
                    style={{ width: `${cannonStatus.total > 0 ? (cannonStatus.progress / cannonStatus.total) * 100 : 0}%` }}
                  ></div>
                </div>
              </div>
            </div>

          </div>

          {/* MAIN DECK CONTAINER */}
          <div className="flex flex-col gap-3 overflow-hidden h-[calc(100vh-95px)]">
            
            {/* ROW 1: COMBINED OVERVIEW CHARTS */}
            <div className="grid grid-cols-3 gap-3">
              
              {/* Queue Depths */}
              <div className="bg-cyber-card border border-white/5 rounded-xl p-3 shadow-md flex flex-col gap-2 relative">
                <div className="flex justify-between items-center border-b border-white/5 pb-1">
                  <h3 className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Queue Metrics</h3>
                  <button onClick={() => setMaximizedChart('queue')} className="text-slate-500 hover:text-cyber-cyan">
                    <Maximize2 className="w-3 h-3" />
                  </button>
                </div>
                <div className="h-28 relative w-full">
                  <Line data={getCombinedQueueChartData()} options={commonChartOptions} />
                </div>
              </div>

              {/* Combined CPU */}
              <div className="bg-cyber-card border border-white/5 rounded-xl p-3 shadow-md flex flex-col gap-2 relative">
                <div className="flex justify-between items-center border-b border-white/5 pb-1">
                  <h3 className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Combined CPU (m)</h3>
                  <button onClick={() => setMaximizedChart('cpu')} className="text-slate-500 hover:text-cyber-cyan">
                    <Maximize2 className="w-3 h-3" />
                  </button>
                </div>
                <div className="h-28 relative w-full">
                  <Line data={getCombinedCPUChartData()} options={commonChartOptions} />
                </div>
              </div>

              {/* Combined Memory */}
              <div className="bg-cyber-card border border-white/5 rounded-xl p-3 shadow-md flex flex-col gap-2 relative">
                <div className="flex justify-between items-center border-b border-white/5 pb-1">
                  <h3 className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Combined Memory (Mi)</h3>
                  <button onClick={() => setMaximizedChart('mem')} className="text-slate-500 hover:text-cyber-cyan">
                    <Maximize2 className="w-3 h-3" />
                  </button>
                </div>
                <div className="h-28 relative w-full">
                  <Line data={getCombinedMemChartData()} options={commonChartOptions} />
                </div>
              </div>

            </div>

            {/* ROW 2: DETAILED INDIVIDUAL COMPONENT BREAKDOWNS (MAIN GRID) */}
            <div className="bg-cyber-card border border-white/5 rounded-xl p-3 shadow-md flex flex-col gap-3 flex-1 overflow-hidden">
              <div className="border-b border-white/5 pb-1 flex justify-between items-center">
                <h3 className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">
                  K8s Microservice Breakdowns (Separate Scales)
                </h3>
                <span className="text-[9px] font-mono text-slate-500">Self-adjusting data coordination metrics</span>
              </div>

              {/* 8 Component Cards */}
              <div className="grid grid-cols-4 gap-2.5 overflow-y-auto pr-1">
                {components.map(compName => {
                  let totalCPU = 0;
                  let totalMem = 0;
                  let replicas = 0;
                  let restarts = 0;
                  let status = 'IDLE';

                  pods.forEach(pod => {
                    const comp = getComponentFromPodName(pod.name);
                    if (comp === compName) {
                      totalCPU += parseInt(pod.cpu.replace('m', '')) || 0;
                      totalMem += parseInt(pod.memory.replace('Mi', '')) || 0;
                      replicas++;
                      restarts += pod.restarts;
                      if (pod.status.includes('CrashLoop') || pod.status.includes('Error') || pod.status.includes('OOM') || status === 'IDLE') {
                        status = pod.status;
                      }
                    }
                  });

                  const isErr = status.includes('Error') || status.includes('Crash') || status.includes('BackOff') || status.includes('Terminating');

                  return (
                    <div key={compName} className="bg-black/35 border border-white/5 rounded-xl p-3 flex flex-col gap-2 relative hover:border-white/15 transition-all shadow-md">
                      <div className="flex justify-between items-center">
                        <span className="text-xs font-bold text-slate-200 tracking-wider uppercase">{compName}</span>
                        <div className="flex items-center gap-1.5">
                          <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded border ${
                            isErr 
                              ? 'bg-cyber-red/10 border-cyber-red/20 text-cyber-red animate-pulse' 
                              : 'bg-cyber-green/5 border-cyber-green/20 text-cyber-green'
                          }`}>
                            {status.toUpperCase()}
                          </span>
                          <button onClick={() => setMaximizedChart(compName)} className="text-slate-500 hover:text-cyber-cyan">
                            <Maximize2 className="w-2.5 h-2.5" />
                          </button>
                        </div>
                      </div>

                      <div className="grid grid-cols-4 gap-0.5 text-[9px] text-slate-400 border-b border-white/5 pb-1.5 font-mono">
                        <div>CPU: <b className="text-slate-200">{totalCPU}m</b></div>
                        <div>Mem: <b className="text-slate-200">{totalMem}Mi</b></div>
                        <div>Reps: <b className="text-slate-200">{replicas}</b></div>
                        <div>Rst: <b className={restarts > 0 ? 'text-cyber-amber font-bold' : 'text-slate-200'}>{restarts}</b></div>
                      </div>

                      <div className="h-20 w-full relative">
                        <Line data={getSingleServiceChartData(compName)} options={getSingleServiceChartOptions()} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* ROW 3: LOCAL TERMINAL INFO LOGGER */}
            <div className="bg-cyber-card border border-white/5 rounded-xl p-3 shadow-md flex flex-col gap-2 relative">
              <div className="flex justify-between items-center border-b border-white/5 pb-1">
                <h3 className="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Console Terminal Logger</h3>
                <div className="flex items-center gap-2">
                  <span className="text-[9px] text-cyber-green font-mono">client.log</span>
                  <button onClick={() => setMaximizedChart('localLogs')} className="text-slate-500 hover:text-cyber-cyan">
                    <Maximize2 className="w-3 h-3" />
                  </button>
                </div>
              </div>
              <div className="bg-black/60 rounded-lg border border-white/5 p-3 font-mono text-[10px] text-cyber-green h-20 overflow-y-auto flex flex-col gap-1 shadow-[inset_0_0_12px_rgba(0,0,0,0.9)]">
                {localLogs.map((log, i) => (
                  <div 
                    key={i} 
                    className={`pl-2 border-l-2 ${
                      log.includes('ERROR') 
                        ? 'border-cyber-red text-cyber-red font-bold' 
                        : log.includes('SYSTEM') 
                          ? 'border-cyber-cyan text-cyber-cyan' 
                          : 'border-cyber-green text-cyber-green'
                    }`}
                  >
                    {log}
                  </div>
                ))}
                <div ref={localLogsEndRef} />
              </div>
            </div>

          </div>

        </main>
      )}

      {/* DECK 2: MASSIVE LOG TERMINAL DECK (FULL SCREEN BY DEFAULT) */}
      {activeTab === 'logs' && (
        <main className="flex-1 flex flex-col gap-3 p-3 max-w-[1920px] mx-auto w-full h-[calc(100vh-75px)] overflow-hidden">
          <div className="bg-cyber-card border border-white/5 rounded-xl p-4 flex flex-col gap-3 flex-1 overflow-hidden shadow-2xl relative">
            
            {/* Control Bar */}
            <div className="flex items-center justify-between gap-4 border-b border-white/5 pb-2.5 flex-wrap">
              <div className="flex items-center gap-3">
                <span className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Filter Log Stream:</span>
                <div className="flex bg-black/40 rounded-lg p-0.5 border border-white/5 gap-0.5">
                  {['all', 'gate', 'broker', 'worker', 'cannon', 'probe', 'infra'].map(filter => (
                    <button
                      key={filter}
                      className={`px-3 py-1 rounded text-[10px] font-bold transition-all ${
                        logsFilter === filter 
                          ? 'bg-cyber-cyan/15 text-cyber-cyan border border-cyber-cyan/25' 
                          : 'text-slate-400 hover:text-slate-200'
                      }`}
                      onClick={() => setLogsFilter(filter)}
                    >
                      {filter.toUpperCase()}
                    </button>
                  ))}
                </div>
              </div>

              <div className="flex items-center gap-3 flex-1 max-w-md">
                <div className="relative w-full">
                  <Search className="absolute left-2.5 top-2 w-3.5 h-3.5 text-slate-500" />
                  <input 
                    type="text" 
                    placeholder="Search logs (grep)..." 
                    value={logsSearch}
                    onChange={(e) => setLogsSearch(e.target.value)}
                    className="w-full bg-black/60 border border-white/5 rounded-lg pl-8 pr-3 py-1.5 text-xs text-white outline-none focus:border-cyber-cyan font-mono"
                  />
                </div>
                
                <label className="flex items-center gap-1.5 text-xs text-slate-400 select-none whitespace-nowrap cursor-pointer">
                  <input 
                    type="checkbox" 
                    checked={logsAutoscroll} 
                    onChange={(e) => setLogsAutoscroll(e.target.checked)} 
                    className="accent-cyber-cyan w-3.5 h-3.5"
                  />
                  <span>Auto-Scroll</span>
                </label>

                <button 
                  className="px-3 py-1.5 bg-white/5 border border-white/5 hover:bg-cyber-red/20 text-xs font-bold text-slate-200 rounded-lg whitespace-nowrap transition-all flex items-center gap-1.5"
                  onClick={() => setLogs([])}
                >
                  <Trash2 className="w-3.5 h-3.5 text-slate-400" />
                  <span>Clear</span>
                </button>
              </div>
            </div>

            {/* Massive Monospace terminal */}
            <div className="flex-1 bg-black/85 border border-white/5 rounded-xl p-4 font-mono text-xs overflow-y-auto flex flex-col gap-1.5 shadow-[inset_0_0_15px_rgba(0,0,0,0.9)]">
              <div className="text-cyber-green opacity-40 select-none text-center py-2 border-b border-white/5 border-dashed mb-1.5">
                === CLUSTER COLLECTIVE TRACE LOG AGGREGATOR ACTIVE ===
              </div>
              {getFilteredLogs().map((log, i) => (
                <div key={i} className="flex gap-3 hover:bg-white/5 py-0.5 px-2 rounded transition-colors items-baseline">
                  <span className="text-slate-500 select-none whitespace-nowrap">{log.timestamp}</span>
                  <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded border whitespace-nowrap ${logBadgeColors[log.service] || 'bg-slate-500/10 border-slate-500/30 text-slate-400'}`}>
                    {log.service.toUpperCase()}
                  </span>
                  <span className="text-slate-500 select-none text-[9px] whitespace-nowrap">{log.pod.slice(-10)}</span>
                  <span className="text-slate-300 break-all">{log.message}</span>
                </div>
              ))}
              <div ref={fullLogsEndRef} />
            </div>

          </div>
        </main>
      )}

      {/* FULL SCREEN MAXIMIZED OVERLAY MODAL FOR GRAPHS / LOGS */}
      {maximizedChart && (
        <div className="fixed inset-0 z-50 bg-cyber-black/98 backdrop-blur-md flex flex-col p-6 gap-4">
          <div className="flex justify-between items-center border-b border-white/10 pb-3">
            <h2 className="text-sm font-extrabold tracking-widest text-slate-200 uppercase">
              {maximizedChart === 'queue' ? 'RabbitMQ Queue Depths (Full Screen)' :
               maximizedChart === 'cpu' ? 'Combined CPU Utilization (Full Screen)' :
               maximizedChart === 'mem' ? 'Combined Memory Footprint (Full Screen)' :
               maximizedChart === 'localLogs' ? 'Client Logs (Full Screen)' :
               `Service ${maximizedChart.toUpperCase()} Metrics (Full Screen)`}
            </h2>
            <button 
              onClick={() => setMaximizedChart(null)} 
              className="px-4 py-2 bg-cyber-red/10 border border-cyber-red/35 text-cyber-red rounded-lg font-bold hover:bg-cyber-red/25 transition-all flex items-center gap-1.5 text-xs"
            >
              <Minimize2 className="w-3.5 h-3.5" />
              <span>Exit Full Screen</span>
            </button>
          </div>

          <div className="flex-1 w-full h-full relative p-4 bg-black/40 rounded-xl border border-white/5 shadow-2xl">
            {maximizedChart === 'queue' && (
              <Line data={getCombinedQueueChartData()} options={{
                ...commonChartOptions,
                plugins: { legend: { display: true, labels: { color: '#8a99ad', font: { family: 'Outfit', size: 12 } } } }
              }} />
            )}
            {maximizedChart === 'cpu' && (
              <Line data={getCombinedCPUChartData()} options={{
                ...commonChartOptions,
                plugins: { legend: { display: true, labels: { color: '#8a99ad', font: { family: 'Outfit', size: 12 } } } }
              }} />
            )}
            {maximizedChart === 'mem' && (
              <Line data={getCombinedMemChartData()} options={{
                ...commonChartOptions,
                plugins: { legend: { display: true, labels: { color: '#8a99ad', font: { family: 'Outfit', size: 12 } } } }
              }} />
            )}
            {maximizedChart === 'localLogs' && (
              <div className="w-full h-full bg-black/80 rounded-lg p-5 font-mono text-xs overflow-y-auto flex flex-col gap-1.5 shadow-[inset_0_0_15px_rgba(0,0,0,0.95)]">
                {localLogs.map((log, i) => (
                  <div key={i} className={`pl-2 border-l-2 ${log.includes('ERROR') ? 'border-cyber-red text-cyber-red' : log.includes('SYSTEM') ? 'border-cyber-cyan text-cyber-cyan' : 'border-cyber-green text-cyber-green'}`}>
                    {log}
                  </div>
                ))}
              </div>
            )}
            {!['queue', 'cpu', 'mem', 'localLogs'].includes(maximizedChart) && (
              <Line data={getSingleServiceChartData(maximizedChart)} options={{
                ...getSingleServiceChartOptions(),
                plugins: { legend: { display: true, labels: { color: '#8a99ad', font: { family: 'Outfit', size: 12 } } } }
              }} />
            )}
          </div>
        </div>
      )}

    </div>
  );
}
