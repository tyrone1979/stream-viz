/* StreamViz browser front-end. No framework, no build step, no external assets. */
(() => {
  'use strict';

  const $ = (id) => document.getElementById(id);

  const VIRIDIS = [
    [13, 8, 35], [39, 24, 85], [59, 51, 120], [52, 82, 139], [41, 112, 142],
    [33, 144, 140], [54, 176, 122], [111, 202, 88], [184, 222, 52], [253, 231, 37]
  ];
  const MAGMA = [
    [5, 3, 20], [30, 16, 68], [79, 18, 123], [129, 37, 129], [181, 54, 122],
    [229, 80, 100], [251, 135, 97], [254, 194, 135], [252, 253, 191]
  ];

  function makeLut(stops) {
    const out = new Uint8Array(256 * 3);
    for (let i = 0; i < 256; i++) {
      const t = (i / 255) * (stops.length - 1);
      const a = Math.floor(t), b = Math.min(a + 1, stops.length - 1), f = t - a;
      for (let k = 0; k < 3; k++) {
        out[i * 3 + k] = Math.round(stops[a][k] + (stops[b][k] - stops[a][k]) * f);
      }
    }
    return out;
  }

  const LUT_WATER = makeLut(VIRIDIS);
  const LUT_ENERGY = makeLut(MAGMA);
  const off = document.createElement('canvas');
  const offctx = off.getContext('2d', { willReadFrequently: false });

  function fit(canvas) {
    const dpr = window.devicePixelRatio || 1;
    const w = Math.max(1, Math.round(canvas.clientWidth * dpr));
    const h = Math.max(1, Math.round(canvas.clientHeight * dpr));
    if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { ctx, w: canvas.clientWidth, h: canvas.clientHeight };
  }

  function drawHeat(id, view, palette) {
    const [rows, cols] = view.shape;
    if (off.width !== cols || off.height !== rows) { off.width = cols; off.height = rows; }
    const raw = atob(view.pixels);
    const img = offctx.createImageData(cols, rows);
    const n = rows * cols;
    for (let i = 0; i < n; i++) {
      const v = raw.charCodeAt(i) * 3;
      img.data[i * 4] = palette[v];
      img.data[i * 4 + 1] = palette[v + 1];
      img.data[i * 4 + 2] = palette[v + 2];
      img.data[i * 4 + 3] = 255;
    }
    offctx.putImageData(img, 0, 0);
    const { ctx, w, h } = fit($(id));
    ctx.clearRect(0, 0, w, h);
    ctx.imageSmoothingEnabled = true;
    ctx.drawImage(off, 0, 0, w, h);
  }

  function grid(ctx, w, h) {
    ctx.strokeStyle = 'rgba(120, 150, 185, 0.13)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (let i = 1; i < 4; i++) {
      const y = (h / 4) * i;
      ctx.moveTo(0, y); ctx.lineTo(w, y);
      const x = (w / 4) * i;
      ctx.moveTo(x, 0); ctx.lineTo(x, h);
    }
    ctx.stroke();
  }

  function drawTrace(id, data, color, ymin, ymax, zeroLine) {
    const { ctx, w, h } = fit($(id));
    ctx.clearRect(0, 0, w, h);
    grid(ctx, w, h);
    if (zeroLine) {
      const y = h - ((0 - ymin) / (ymax - ymin)) * h;
      ctx.strokeStyle = 'rgba(160, 185, 210, 0.28)';
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
    }
    if (!data || data.length < 2) return;
    const span = ymax - ymin;
    const yOf = (v) => {
      const p = (v - ymin) / span;
      return h - Math.max(0, Math.min(1, p)) * h;
    };
    ctx.beginPath();
    for (let i = 0; i < data.length; i++) {
      const x = (i / (data.length - 1)) * w;
      const y = yOf(data[i]);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.4;
    ctx.lineJoin = 'round';
    ctx.stroke();
    const last = yOf(data[data.length - 1]);
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(w - 2, last, 2.5, 0, Math.PI * 2); ctx.fill();
  }

  function fmtMs(ms) {
    if (!ms) return '—';
    return new Date(ms).toISOString().slice(11, 23);
  }

  let lastVersion = -1;
  let seeking = false;
  let toastTimer = null;

  function toast(message) {
    const el = $('toast');
    el.textContent = message;
    el.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => el.classList.remove('show'), 2600);
  }

  function render(s) {
    const status = s.status || 'offline';
    $('statusPill').className = 'pill ' + (status === 'running' ? 'running' : status === 'stopped' ? 'stopped' : '');
    $('statusText').textContent = status;
    $('endpoint').textContent = `udp ${s.config.udp_port} · ${s.config.refresh_hz} Hz · ${s.config.frame_capacity} frames`;

    const c = s.metrics.counters || {};
    $('mThroughput').textContent = Number(s.metrics.throughput_mbps || 0).toFixed(2);
    $('mDatagrams').textContent = c.udp_datagrams || 0;
    $('mParsed').textContent = c.parsed_packets || 0;
    $('mDrops').textContent = s.metrics.fifo_overflow_dropped || 0;
    $('mAudio').textContent = s.audio.count;
    $('mFrames').textContent = s.frames;
    const ing = s.metrics.ingest_latency_ms || {};
    const e2e = s.metrics.end_to_end_latency_ms || {};
    $('mIngest').textContent = ing.count ? `${ing.p95_ms} ms` : '—';
    $('mLatency').textContent = e2e.count ? `${e2e.p50_ms} / ${e2e.p95_ms} ms` : '— / —';

    if (s.view_version !== lastVersion) {
      lastVersion = s.view_version;
      drawHeat('c-water', s.views.spectrogram, LUT_WATER);
      drawHeat('c-energy', s.views.energy, LUT_ENERGY);
      drawTrace('c-wave', s.views.waveform, '#45d0c8', -1.5, 1.5, true);
      const spec = s.views.spectrum;
      drawTrace('c-spec', spec, '#ffb457', 0, Math.max(1.5, ...spec) * 1.08, false);
      $('versionBadge').textContent = `version ${s.view_version}`;
      $('frameInfo').textContent = `${s.frames} rendered frames · ${c.display_frames || 0} spectrum packets · poll 8 Hz`;
    }

    const [lo, hi] = s.audio.range;
    if (hi > lo) {
      const seek = $('seek');
      if (!seeking) {
        seek.min = lo; seek.max = hi;
        if (Number(seek.value) < lo || Number(seek.value) > hi) seek.value = Math.round((lo + hi) / 2);
      }
      $('tMin').textContent = fmtMs(lo);
      $('tMax').textContent = fmtMs(hi);
      $('audioInfo').textContent = `${s.audio.count} indexed WAV segments · drag to a source timestamp`;
    }

    const log = $('log');
    const lines = (s.log || []).slice().reverse();
    log.replaceChildren(...(lines.length ? lines : ['No events yet.']).map((line) => {
      const p = document.createElement('p');
      p.textContent = line;
      return p;
    }));
  }

  async function poll() {
    try {
      const r = await fetch('/api/state', { cache: 'no-store' });
      render(await r.json());
    } catch (err) {
      $('statusPill').className = 'pill offline';
      $('statusText').textContent = 'offline';
    }
  }

  async function post(path, body) {
    const r = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {})
    });
    return r.json();
  }

  $('startBtn').addEventListener('click', async () => {
    const r = await post('/api/control', {
      action: 'start',
      rate_mbps: Number($('rate').value),
      frame_capacity: Number($('capacity').value),
      refresh_hz: Number($('refresh').value),
      malformed_ratio: Number($('malformed').value)
    });
    toast(r.message || 'started');
  });

  $('stopBtn').addEventListener('click', async () => {
    const r = await post('/api/control', { action: 'stop' });
    toast(r.message || 'stopped');
  });

  $('figureBtn').addEventListener('click', async () => {
    const r = await post('/api/figure', {});
    toast(r.ok ? `exported ${r.name}` : (r.message || 'export failed'));
  });

  $('liveBtn').addEventListener('click', () => {
    const audio = $('audio');
    audio.src = `/api/audio?mode=live&t=${Date.now()}`;
    audio.play().catch(() => toast('audio ready — press play'));
  });

  $('seekBtn').addEventListener('click', () => {
    const audio = $('audio');
    audio.src = `/api/audio?mode=seek&ts=${Math.round(Number($('seek').value))}&t=${Date.now()}`;
    audio.play().catch(() => toast('audio ready — press play'));
    toast(`replay from ${fmtMs(Number($('seek').value))}`);
  });

  $('seek').addEventListener('input', () => { seeking = true; });
  $('seek').addEventListener('change', () => { seeking = false; });

  window.addEventListener('resize', () => { lastVersion = -1; });
  poll();
  setInterval(poll, 125);
})();
