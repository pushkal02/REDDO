import { useState, useEffect, useRef } from 'react';
import { Maximize2, Minimize2, Trash2, Search, Settings } from 'lucide-react';
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

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend, Filler);

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
  rabbitmq: { java_queue_depth: number; node_queue_depth: number };
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
  gate:     '#6366f1',
  broker:   '#9333ea',
  worker:   '#0ea5e9',
  cannon:   '#ef4444',
  probe:    '#f59e0b',
  rabbitmq: '#10b981',
  postgres: '#3b82f6',
  redis:    '#64748b'
};

const maxPts = 30;
const emptyHistory = () => {
  const cpu: Record<string, number[]> = {};
  const mem: Record<string, number[]> = {};
  components.forEach(c => { cpu[c] = Array(maxPts).fill(0); mem[c] = Array(maxPts).fill(0); });
  return { javaQ: Array(maxPts).fill(0), nodeQ: Array(maxPts).fill(0), cpu, mem };
};

const logBadge: Record<string, string> = {
  gate:     'bg-indigo-100 border-indigo-300 text-indigo-700',
  broker:   'bg-purple-100 border-purple-300 text-purple-700',
  worker:   'bg-sky-100 border-sky-300 text-sky-700',
  cannon:   'bg-red-100 border-red-300 text-red-700',
  probe:    'bg-amber-100 border-amber-300 text-amber-700',
  rabbitmq: 'bg-emerald-100 border-emerald-300 text-emerald-700',
  postgres: 'bg-blue-100 border-blue-300 text-blue-700',
  redis:    'bg-slate-100 border-slate-300 text-slate-600',
};

export default function App() {
  const [tab, setTab] = useState<'metrics'|'logs'>('metrics');
  const [cannonURL, setCannonURL] = useState('http://localhost:8083');
  const [cannonStatus, setCannonStatus] = useState<any>({ status: 'IDLE', progress: 0, total: 0, success_rate: 1.0 });
  const [cannonOk, setCannonOk] = useState(false);
  const [streamOk, setStreamOk] = useState(false);
  const [engineMsg, setEngineMsg] = useState('SAGA ENGINE NORMAL');
  const [engineErr, setEngineErr] = useState(false);

  // Blast params
  const [totalReqs, setTotalReqs] = useState(300);
  const [concurrency, setConcurrency] = useState(15);
  const [zombie, setZombie] = useState(5);
  const [fatal, setFatal] = useState(2);
  const [bizFail, setBizFail] = useState(5);
  const [seed, setSeed] = useState(42561);
  const [delay, setDelay] = useState(0);
  const [templates, setTemplates] = useState({ valid_dag: true, saga_rollback: true, saga_compensation: true });

  // Telemetry
  const [pods, setPods] = useState<PodTelemetry[]>([]);
  const [history, setHistory] = useState(emptyHistory);

  // Logs
  const [logs, setLogs] = useState<LogLine[]>([]);
  const [logFilter, setLogFilter] = useState('all');
  const [logSearch, setLogSearch] = useState('');
  const [autoScroll, setAutoScroll] = useState(true);
  const [localLogs, setLocalLogs] = useState<string[]>([]);

  // Modal
  const [modal, setModal] = useState<string|null>(null);

  const logsEndRef = useRef<HTMLDivElement>(null);
  const localEndRef = useRef<HTMLDivElement>(null);

  const podComp = (name: string) => {
    if (name.startsWith('gate-'))     return 'gate';
    if (name.startsWith('broker-'))   return 'broker';
    if (name.startsWith('worker-'))   return 'worker';
    if (name.startsWith('cannon-'))   return 'cannon';
    if (name.startsWith('probe-'))    return 'probe';
    if (name.startsWith('rabbitmq-')) return 'rabbitmq';
    if (name.startsWith('postgres'))  return 'postgres';
    if (name.startsWith('redis-'))    return 'redis';
    return null;
  };

  const sysLog = (msg: string, type: 'info'|'error'|'system' = 'info') => {
    const ts = new Date().toLocaleTimeString();
    const line = `[${ts}] [${type.toUpperCase()}] ${msg}`;
    setLocalLogs(p => { const n = [...p, line]; if (n.length > 50) n.shift(); return n; });
    setTimeout(() => localEndRef.current?.scrollIntoView({ behavior: 'smooth' }), 50);
  };

  // Telemetry SSE
  useEffect(() => {
    const sse = new EventSource('/api/v1/telemetry/stream');
    sse.onopen = () => { setStreamOk(true); sysLog('Telemetry stream active.', 'system'); };
    sse.onerror = () => setStreamOk(false);
    sse.onmessage = (ev) => {
      try {
        const t: TelemetryPayload = JSON.parse(ev.data);
        const tickCPU: Record<string,number> = {};
        const tickMem: Record<string,number> = {};
        components.forEach(c => { tickCPU[c] = 0; tickMem[c] = 0; });
        let err = false;
        t.pods.forEach(p => {
          const c = podComp(p.name);
          if (c) {
            tickCPU[c] += parseInt(p.cpu) || 0;
            tickMem[c] += parseInt(p.memory) || 0;
          }
          if (p.status.includes('CrashLoop') || p.status.includes('Error') || p.status.includes('OOM')) {
            err = true;
            sysLog(`Incident: Pod ${p.name} → ${p.status}`, 'error');
          }
        });
        setPods(t.pods);
        setEngineErr(err);
        setEngineMsg(err ? 'SAGA ENGINE DISTRESS ⚠️' : cannonStatus.status === 'RUNNING' ? 'CHAOS ACTIVE 💥' : 'SAGA ENGINE NORMAL');
        setHistory(prev => {
          const nextCPU: Record<string,number[]> = {};
          const nextMem: Record<string,number[]> = {};
          components.forEach(c => {
            nextCPU[c] = [...prev.cpu[c].slice(1), tickCPU[c]];
            nextMem[c] = [...prev.mem[c].slice(1), tickMem[c]];
          });
          return {
            javaQ: [...prev.javaQ.slice(1), t.rabbitmq.java_queue_depth],
            nodeQ: [...prev.nodeQ.slice(1), t.rabbitmq.node_queue_depth],
            cpu: nextCPU,
            mem: nextMem
          };
        });
      } catch { /**/ }
    };
    return () => sse.close();
  }, [cannonStatus.status]);

  // Logs SSE
  useEffect(() => {
    const sse = new EventSource('/api/v1/logs/stream');
    sse.onmessage = (ev) => {
      try {
        const l: LogLine = JSON.parse(ev.data);
        l.timestamp = new Date().toLocaleTimeString();
        setLogs(p => { const n = [...p, l]; if (n.length > 500) n.shift(); return n; });
      } catch { /**/ }
    };
    return () => sse.close();
  }, []);

  // Cannon poll
  useEffect(() => {
    const poll = async () => {
      try {
        const r = await fetch(`${cannonURL}/api/v1/chaos/status`);
        if (!r.ok) throw new Error();
        setCannonStatus(await r.json());
        setCannonOk(true);
      } catch { setCannonOk(false); setCannonStatus({ status: 'UNKNOWN', progress: 0, total: 0, success_rate: 1.0 }); }
    };
    poll();
    const id = setInterval(poll, 1000);
    return () => clearInterval(id);
  }, [cannonURL]);

  // Autoscroll
  useEffect(() => {
    if (autoScroll) logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs, tab, autoScroll, logFilter, logSearch]);

  // Fire cannon
  const fire = async () => {
    const tpls = Object.entries(templates).filter(([,v]) => v).map(([k]) => k);
    if (!tpls.length) { alert('Select at least one template!'); return; }
    const body = { total_requests: totalReqs, concurrency, zombie_hang_probability: zombie/100, fatal_crash_probability: fatal/100, business_fail_probability: bizFail/100, workflow_templates: tpls, seed, schedule_delay_seconds: delay };
    sysLog(`Fire! Seed=${seed} Delay=${delay}s`, 'system');
    try {
      const r = await fetch(`${cannonURL}/api/v1/chaos/fire`, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      sysLog(`Cannon: ${d.message}`, 'system');
    } catch (e: any) { sysLog(`Fire failed: ${e.message}`, 'error'); }
  };

  const stop = async () => {
    try {
      const r = await fetch(`${cannonURL}/api/v1/chaos/stop`, { method: 'POST' });
      const d = await r.json();
      sysLog(`Stop: ${d.message}`, 'system');
    } catch (e: any) { sysLog(`Stop failed: ${e.message}`, 'error'); }
  };

  // Chart helpers
  const labels = Array(maxPts).fill('');
  const baseOpts = (light: boolean) => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 } as any,
    plugins: { legend: { display: false } },
    scales: {
      x: { display: false },
      y: {
        grid: { color: light ? 'rgba(0,0,0,0.05)' : 'rgba(255,255,255,0.04)' },
        ticks: { color: light ? '#6b7280' : '#9ca3af', font: { family: 'JetBrains Mono', size: 9 } }
      }
    }
  });

  const fullOpts = (light: boolean) => ({
    ...baseOpts(light),
    plugins: { legend: { display: true, labels: { color: light ? '#374151' : '#9ca3af', font: { family: 'Outfit', size: 12 }, boxWidth: 12, padding: 14 } } }
  });

  const dualAxisOpts = (light: boolean) => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 } as any,
    plugins: { legend: { display: false } },
    scales: {
      x: { display: false },
      yCPU: {
        type: 'linear' as const, position: 'left' as const,
        grid: { color: light ? 'rgba(0,0,0,0.04)' : 'rgba(255,255,255,0.03)' },
        ticks: { color: light ? '#6b7280' : '#9ca3af', font: { family: 'JetBrains Mono', size: 8 } }
      },
      yMem: {
        type: 'linear' as const, position: 'right' as const,
        grid: { display: false },
        ticks: { color: light ? '#9ca3af' : '#6b7280', font: { family: 'JetBrains Mono', size: 8 } }
      }
    }
  });

  const queueData = () => ({ labels, datasets: [
    { label: 'Java Queue', data: history.javaQ, borderColor: '#9333ea', borderWidth: 2, tension: 0.35, pointRadius: 0, fill: false },
    { label: 'Node Queue', data: history.nodeQ, borderColor: '#0ea5e9', borderWidth: 2, tension: 0.35, pointRadius: 0, fill: false }
  ]});
  const cpuData = () => ({ labels, datasets: components.map(c => ({
    label: c, data: history.cpu[c], borderColor: componentColors[c], borderWidth: 1.5, tension: 0.35, pointRadius: 0, fill: false
  }))});
  const memData = () => ({ labels, datasets: components.map(c => ({
    label: c, data: history.mem[c], borderColor: componentColors[c], borderWidth: 1.5, tension: 0.35, pointRadius: 0, fill: false
  }))});
  const svcData = (comp: string) => ({ labels, datasets: [
    { label: 'CPU (m)', data: history.cpu[comp], borderColor: componentColors[comp], borderWidth: 2, tension: 0.35, pointRadius: 0, fill: false, yAxisID: 'yCPU' },
    { label: 'Mem (Mi)', data: history.mem[comp], borderColor: '#d1d5db', borderWidth: 1.5, tension: 0.35, pointRadius: 0, borderDash: [3,3], fill: false, yAxisID: 'yMem' }
  ]});

  const filteredLogs = logs.filter(l => {
    const srv = logFilter === 'all' || (logFilter === 'infra' ? ['postgres','redis','rabbitmq'].includes(l.service) : l.service === logFilter);
    const srch = !logSearch.trim() || l.message.toLowerCase().includes(logSearch.toLowerCase());
    return srv && srch;
  });

  // Pod aggregates for a component
  const compStats = (comp: string) => {
    let cpu = 0, mem = 0, reps = 0, rsts = 0, status = 'IDLE';
    pods.forEach(p => {
      if (podComp(p.name) === comp) {
        cpu += parseInt(p.cpu) || 0;
        mem += parseInt(p.memory) || 0;
        reps++;
        rsts += p.restarts;
        if (p.status.includes('CrashLoop') || p.status.includes('Error') || p.status.includes('OOM') || status === 'IDLE') status = p.status;
      }
    });
    return { cpu, mem, reps, rsts, status };
  };

  const pct = cannonStatus.total > 0 ? (cannonStatus.progress / cannonStatus.total) * 100 : 0;

  return (
    <div className="flex flex-col min-h-screen text-gray-900 font-sans text-sm">

      {/* ── HEADER ── */}
      <header className="sticky top-0 z-40 flex items-center justify-between px-5 py-2.5 bg-white/90 backdrop-blur-md border-b border-gray-200 shadow-sm">
        <div className="flex items-center gap-3">
          <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-indigo-500 to-blue-500 shadow-md flex items-center justify-center">
            <span className="text-white text-xs font-black">R</span>
          </div>
          <div>
            <h1 className="text-sm font-extrabold tracking-widest text-gray-800">REDDO SAGA PLATFORM</h1>
            <p className="text-[9px] text-indigo-500 font-mono tracking-widest uppercase">Realtime Observability & Chaos HUD</p>
          </div>
          <div className={`ml-3 px-2.5 py-0.5 rounded-full text-[10px] font-mono font-bold tracking-wider border transition-all ${
            engineErr ? 'bg-red-50 border-red-300 text-red-600 status-pulse-red' : 'bg-green-50 border-green-300 text-green-700'
          }`}>{engineMsg}</div>
        </div>

        {/* Tabs */}
        <div className="flex bg-gray-100 p-0.5 rounded-lg border border-gray-200 gap-0.5">
          {(['metrics','logs'] as const).map(t => (
            <button key={t} onClick={() => setTab(t)}
              className={`px-4 py-1.5 rounded-md text-xs font-semibold tracking-wide transition-all ${
                tab === t ? 'bg-white shadow text-indigo-600 border border-gray-200' : 'text-gray-500 hover:text-gray-800'
              }`}>
              {t === 'metrics' ? '📊 Observability Deck' : '📟 Log Terminal'}
            </button>
          ))}
        </div>

        {/* Connection chips */}
        <div className="flex items-center gap-2 text-[11px] font-mono">
          <div className="flex items-center gap-1.5 bg-gray-50 px-2.5 py-1 rounded-lg border border-gray-200">
            <span className="text-gray-500 font-semibold">CANNON:</span>
            <input value={cannonURL} onChange={e => setCannonURL(e.target.value)}
              className="bg-transparent border-none outline-none w-36 text-gray-700 text-[11px]" />
          </div>
          <Chip ok={cannonOk} label="CANNON" />
          <Chip ok={streamOk} label="STREAM" />
        </div>
      </header>

      {/* ── DECK 1: METRICS ── */}
      {tab === 'metrics' && (
        <main className="flex-1 grid grid-cols-[300px_1fr] gap-3 p-3 w-full overflow-hidden">

          {/* ── SIDEBAR ── */}
          <aside className="flex flex-col gap-3 bg-white border border-gray-200 rounded-xl p-4 shadow-sm overflow-y-auto">
            <div className="flex items-center justify-between border-b border-gray-100 pb-1.5">
              <h2 className="text-xs font-bold text-gray-500 uppercase tracking-widest">Chaos Console</h2>
              <Settings className="w-3.5 h-3.5 text-gray-400" />
            </div>

            <div className="grid grid-cols-2 gap-2">
              <Field label="Workflows" value={totalReqs} onChange={v => setTotalReqs(v)} />
              <Field label="Concurrency" value={concurrency} onChange={v => setConcurrency(v)} />
            </div>

            <div>
              <label className="block text-[10px] font-bold text-gray-500 uppercase mb-1">Templates</label>
              <div className="flex flex-col gap-1 bg-gray-50 p-2 rounded-lg border border-gray-200">
                {[
                  { key: 'valid_dag', label: 'Valid Workflow' },
                  { key: 'saga_rollback', label: 'Saga Rollback' },
                  { key: 'saga_compensation', label: 'Saga Compensation' },
                ].map(({ key, label }) => (
                  <label key={key} className="flex items-center gap-2 cursor-pointer select-none text-gray-700">
                    <input type="checkbox" checked={(templates as any)[key]}
                      onChange={e => setTemplates(p => ({ ...p, [key]: e.target.checked }))}
                      className="accent-indigo-500 w-3.5 h-3.5" />
                    <span className="text-xs">{label}</span>
                  </label>
                ))}
              </div>
            </div>

            <div className="flex flex-col gap-2">
              <Slider label="Zombie Hang" value={zombie} onChange={setZombie} color="indigo" />
              <Slider label="Fatal Crash" value={fatal} onChange={setFatal} color="red" />
              <Slider label="Business Fail" value={bizFail} onChange={setBizFail} color="amber" />
            </div>

            <div className="grid grid-cols-[1fr_auto] gap-2 items-end">
              <Field label="Seed" value={seed} onChange={setSeed} />
              <button onClick={() => setSeed(Math.floor(Math.random() * 90000) + 10000)}
                className="py-1.5 px-2.5 bg-gray-100 hover:bg-gray-200 border border-gray-200 text-gray-600 text-[10px] font-bold rounded-lg transition-all">
                🎲 Random
              </button>
            </div>
            <Field label="Delay Start (s)" value={delay} onChange={setDelay} />

            <div className="flex flex-col gap-1.5">
              <button onClick={fire}
                disabled={!cannonOk || cannonStatus.status === 'RUNNING'}
                className="w-full py-2.5 bg-gradient-to-r from-indigo-600 to-blue-600 text-white font-black tracking-widest text-[11px] rounded-xl shadow-md hover:shadow-indigo-200 hover:-translate-y-0.5 transition-all disabled:opacity-30 disabled:pointer-events-none">
                FIRE CANNON 💥
              </button>
              <button onClick={stop}
                disabled={cannonStatus.status !== 'RUNNING' && cannonStatus.status !== 'SCHEDULED'}
                className="w-full py-2 bg-gradient-to-r from-red-500 to-rose-600 text-white font-bold text-[10px] rounded-xl shadow hover:shadow-red-200 disabled:opacity-20 disabled:pointer-events-none transition-all">
                CANCEL BLAST 🛑
              </button>
            </div>

            {/* Status HUD */}
            <div className="grid grid-cols-2 gap-2 bg-gray-50 p-2.5 rounded-xl border border-gray-200 mt-auto">
              <StatCell label="Status" value={cannonStatus.status}
                className={cannonStatus.status === 'RUNNING' ? 'text-red-600 animate-pulse' : 'text-gray-700'} />
              <StatCell label="Success" value={`${(cannonStatus.success_rate * 100).toFixed(1)}%`}
                className="text-green-600" />
              <div className="col-span-2 flex flex-col gap-1 pt-1.5 border-t border-gray-200">
                <div className="flex justify-between text-[9px] font-bold text-gray-400 uppercase">
                  <span>Progress</span>
                  <span className="font-mono text-gray-600">{cannonStatus.progress} / {cannonStatus.total}</span>
                </div>
                <div className="w-full h-2 bg-gray-200 rounded-full overflow-hidden">
                  <div className="h-full bg-gradient-to-r from-indigo-500 to-blue-500 transition-all duration-300 rounded-full"
                    style={{ width: `${pct}%` }} />
                </div>
              </div>
            </div>

            {/* Local terminal */}
            <div>
              <div className="flex justify-between items-center mb-1">
                <span className="text-[10px] font-bold text-gray-500 uppercase tracking-wider">Client Log</span>
                <button onClick={() => setModal('localLogs')}><Maximize2 className="w-3 h-3 text-gray-400 hover:text-indigo-500" /></button>
              </div>
              <div className="bg-gray-900 rounded-xl p-3 font-mono text-[10px] h-32 overflow-y-auto flex flex-col gap-0.5 shadow-inner">
                {localLogs.map((l, i) => (
                  <div key={i} className={`pl-2 border-l-2 ${
                    l.includes('ERROR') ? 'border-red-400 text-red-300' :
                    l.includes('SYSTEM') ? 'border-indigo-400 text-indigo-300' :
                    'border-green-400 text-green-300'
                  }`}>{l}</div>
                ))}
                <div ref={localEndRef} />
              </div>
            </div>
          </aside>

          {/* ── MAIN METRICS AREA ── */}
          <div className="flex flex-col gap-3 overflow-hidden h-[calc(100vh-80px)]">

            {/* Row 1: Combined charts */}
            <div className="grid grid-cols-3 gap-3">
              <ChartCard title="Queue Depths" badge="RabbitMQ" onMax={() => setModal('queue')}>
                <div className="h-44"><Line data={queueData()} options={baseOpts(true) as any} /></div>
              </ChartCard>
              <ChartCard title="Combined CPU (m)" badge="All Services" onMax={() => setModal('cpu')}>
                <div className="h-44"><Line data={cpuData()} options={baseOpts(true) as any} /></div>
              </ChartCard>
              <ChartCard title="Combined Memory (Mi)" badge="All Services" onMax={() => setModal('mem')}>
                <div className="h-44"><Line data={memData()} options={baseOpts(true) as any} /></div>
              </ChartCard>
            </div>

            {/* Row 2: Per-service breakdowns */}
            <div className="bg-white border border-gray-200 rounded-xl p-3 shadow-sm flex flex-col gap-2 flex-1 overflow-hidden">
              <div className="flex justify-between items-center border-b border-gray-100 pb-1.5">
                <h3 className="text-[10px] font-bold text-gray-500 uppercase tracking-widest">Service Breakdowns (Individual Scales)</h3>
                <span className="text-[9px] font-mono text-gray-400">CPU left axis · Memory right axis (dashed)</span>
              </div>
              <div className="grid grid-cols-4 gap-3 overflow-y-auto pr-1 pb-1">
                {components.map(comp => {
                  const { cpu, mem, reps, rsts, status } = compStats(comp);
                  const isErr = status.includes('Error') || status.includes('Crash') || status.includes('BackOff');
                  return (
                    <div key={comp}
                      className="bg-gray-50 border border-gray-200 rounded-xl p-3 flex flex-col gap-2 hover:border-indigo-300 hover:shadow-sm transition-all">
                      <div className="flex justify-between items-center">
                        <span className="text-xs font-extrabold text-gray-700 uppercase tracking-wider">{comp}</span>
                        <div className="flex items-center gap-1">
                          <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded border ${
                            isErr ? 'bg-red-50 border-red-200 text-red-600 animate-pulse' : 'bg-green-50 border-green-200 text-green-700'
                          }`}>{status.slice(0, 10).toUpperCase()}</span>
                          <button onClick={() => setModal(comp)}><Maximize2 className="w-2.5 h-2.5 text-gray-400 hover:text-indigo-500" /></button>
                        </div>
                      </div>
                      <div className="grid grid-cols-4 text-[9px] font-mono text-gray-500 border-b border-gray-200 pb-1.5 gap-0.5">
                        <span>CPU <b className="text-gray-800">{cpu}m</b></span>
                        <span>Mem <b className="text-gray-800">{mem}Mi</b></span>
                        <span>×<b className="text-gray-800">{reps}</b></span>
                        <span>Rst <b className={rsts > 0 ? 'text-amber-600' : 'text-gray-800'}>{rsts}</b></span>
                      </div>
                      <div className="h-28 w-full">
                        <Line data={svcData(comp)} options={dualAxisOpts(true) as any} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

          </div>
        </main>
      )}

      {/* ── DECK 2: LOGS ── */}
      {tab === 'logs' && (
        <main className="flex-1 flex flex-col gap-3 p-3 w-full h-[calc(100vh-60px)] overflow-hidden">
          <div className="bg-white border border-gray-200 rounded-xl p-4 flex flex-col gap-3 flex-1 overflow-hidden shadow-sm">

            {/* Filter bar */}
            <div className="flex items-center justify-between gap-4 border-b border-gray-100 pb-2.5 flex-wrap">
              <div className="flex items-center gap-2">
                <span className="text-[10px] font-bold uppercase tracking-widest text-gray-500">Filter:</span>
                <div className="flex bg-gray-100 rounded-lg p-0.5 border border-gray-200 gap-0.5">
                  {['all','gate','broker','worker','cannon','probe','infra'].map(f => (
                    <button key={f} onClick={() => setLogFilter(f)}
                      className={`px-2.5 py-1 rounded-md text-[10px] font-bold transition-all ${
                        logFilter === f ? 'bg-white shadow text-indigo-600 border border-gray-200' : 'text-gray-500 hover:text-gray-700'
                      }`}>{f.toUpperCase()}</button>
                  ))}
                </div>
              </div>
              <div className="flex items-center gap-3 flex-1 max-w-md">
                <div className="relative w-full">
                  <Search className="absolute left-2.5 top-2 w-3.5 h-3.5 text-gray-400" />
                  <input placeholder="Grep logs..." value={logSearch} onChange={e => setLogSearch(e.target.value)}
                    className="w-full bg-gray-50 border border-gray-200 rounded-lg pl-8 pr-3 py-1.5 text-xs text-gray-700 outline-none focus:border-indigo-400 font-mono" />
                </div>
                <label className="flex items-center gap-1.5 text-xs text-gray-500 cursor-pointer select-none whitespace-nowrap">
                  <input type="checkbox" checked={autoScroll} onChange={e => setAutoScroll(e.target.checked)} className="accent-indigo-500 w-3.5 h-3.5" />
                  Auto-scroll
                </label>
                <button onClick={() => setLogs([])}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-100 hover:bg-red-50 border border-gray-200 text-xs font-bold text-gray-600 hover:text-red-600 rounded-lg transition-all">
                  <Trash2 className="w-3.5 h-3.5" /> Clear
                </button>
              </div>
            </div>

            {/* Log terminal */}
            <div className="flex-1 bg-gray-950 border border-gray-800 rounded-xl p-4 font-mono text-xs overflow-y-auto flex flex-col gap-1 shadow-inner">
              <div className="text-green-500 opacity-40 text-center py-2 border-b border-gray-800 border-dashed mb-1 select-none">
                === CLUSTER LOG AGGREGATOR ACTIVE ===
              </div>
              {filteredLogs.map((l, i) => (
                <div key={i} className="flex gap-3 hover:bg-white/5 py-0.5 px-2 rounded items-baseline">
                  <span className="text-gray-600 whitespace-nowrap select-none">{l.timestamp}</span>
                  <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded border whitespace-nowrap ${logBadge[l.service] || 'bg-gray-100 border-gray-300 text-gray-600'}`}>
                    {l.service.toUpperCase()}
                  </span>
                  <span className="text-gray-600 text-[9px] whitespace-nowrap select-none">{l.pod.slice(-12)}</span>
                  <span className="text-gray-200 break-all">{l.message}</span>
                </div>
              ))}
              <div ref={logsEndRef} />
            </div>
          </div>
        </main>
      )}

      {/* ── FULL SCREEN MODAL ── */}
      {modal && (
        <div className="fixed inset-0 z-50 bg-white/95 backdrop-blur-md flex flex-col p-6 gap-4">
          <div className="flex justify-between items-center border-b border-gray-200 pb-3">
            <h2 className="text-base font-extrabold tracking-widest text-gray-800 uppercase">
              {modal === 'queue' ? 'Queue Depths — Full Screen' :
               modal === 'cpu'   ? 'Combined CPU — Full Screen' :
               modal === 'mem'   ? 'Combined Memory — Full Screen' :
               modal === 'localLogs' ? 'Client Logs — Full Screen' :
               `${modal.toUpperCase()} Metrics — Full Screen`}
            </h2>
            <button onClick={() => setModal(null)}
              className="flex items-center gap-1.5 px-4 py-2 bg-gray-100 hover:bg-gray-200 border border-gray-300 text-gray-600 rounded-xl font-bold text-xs transition-all">
              <Minimize2 className="w-3.5 h-3.5" /> Exit Full Screen
            </button>
          </div>
          <div className="flex-1 w-full h-full relative p-2">
            {modal === 'queue' && <Line data={queueData()} options={fullOpts(true) as any} />}
            {modal === 'cpu'   && <Line data={cpuData()}   options={fullOpts(true) as any} />}
            {modal === 'mem'   && <Line data={memData()}   options={fullOpts(true) as any} />}
            {modal === 'localLogs' && (
              <div className="w-full h-full bg-gray-900 rounded-xl p-5 font-mono text-xs overflow-y-auto flex flex-col gap-1">
                {localLogs.map((l, i) => (
                  <div key={i} className={`pl-2 border-l-2 ${
                    l.includes('ERROR') ? 'border-red-400 text-red-300' :
                    l.includes('SYSTEM') ? 'border-indigo-400 text-indigo-300' :
                    'border-green-400 text-green-300'
                  }`}>{l}</div>
                ))}
              </div>
            )}
            {!['queue','cpu','mem','localLogs'].includes(modal) && (
              <Line data={svcData(modal)} options={{
                ...dualAxisOpts(true),
                plugins: { legend: { display: true, labels: { color: '#374151', font: { family: 'Outfit', size: 12 }, boxWidth: 12, padding: 14 } } }
              } as any} />
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ── tiny reusable components ─────────────────────────────────────────────────

function Chip({ ok, label }: { ok: boolean; label: string }) {
  return (
    <div className="flex items-center gap-1.5 bg-gray-50 px-2.5 py-1 rounded-lg border border-gray-200 text-[11px]">
      <div className={`w-2 h-2 rounded-full ${ok ? 'bg-green-500 shadow-[0_0_5px_#16a34a]' : 'bg-red-500 shadow-[0_0_5px_#dc2626]'}`} />
      <span>{label}: <b className={ok ? 'text-green-600' : 'text-red-600'}>{ok ? 'OK' : 'ERR'}</b></span>
    </div>
  );
}

function Field({ label, value, onChange }: { label: string; value: number; onChange: (v: number) => void }) {
  return (
    <div className="flex flex-col gap-0.5">
      <label className="text-[10px] font-bold text-gray-500 uppercase">{label}</label>
      <input type="number" value={value} onChange={e => onChange(parseInt(e.target.value) || 0)}
        className="bg-gray-50 border border-gray-200 rounded-lg px-2.5 py-1.5 text-gray-800 font-mono text-xs outline-none focus:border-indigo-400" />
    </div>
  );
}

function Slider({ label, value, onChange, color }: { label: string; value: number; onChange: (v: number) => void; color: string }) {
  const accent: Record<string, string> = { indigo: 'accent-indigo-500', red: 'accent-red-500', amber: 'accent-amber-500' };
  return (
    <div>
      <div className="flex justify-between text-[10px] font-bold uppercase mb-0.5">
        <span className="text-gray-500">{label}</span>
        <span className="font-mono text-gray-700">{value}%</span>
      </div>
      <input type="range" min={0} max={100} value={value} onChange={e => onChange(parseInt(e.target.value))}
        className={`w-full h-1.5 bg-gray-200 rounded-full appearance-none cursor-pointer ${accent[color]}`} />
    </div>
  );
}

function ChartCard({ title, badge, onMax, children }: { title: string; badge: string; onMax: () => void; children: React.ReactNode }) {
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-3 shadow-sm flex flex-col gap-2">
      <div className="flex justify-between items-center border-b border-gray-100 pb-1.5">
        <h3 className="text-[10px] font-bold text-gray-500 uppercase tracking-wider">{title}</h3>
        <div className="flex items-center gap-2">
          <span className="text-[9px] font-mono text-gray-400">{badge}</span>
          <button onClick={onMax}><Maximize2 className="w-3 h-3 text-gray-400 hover:text-indigo-500" /></button>
        </div>
      </div>
      {children}
    </div>
  );
}

function StatCell({ label, value, className }: { label: string; value: string; className?: string }) {
  return (
    <div>
      <div className="text-[9px] font-bold text-gray-400 uppercase">{label}</div>
      <div className={`text-xs font-extrabold font-mono ${className}`}>{value}</div>
    </div>
  );
}
