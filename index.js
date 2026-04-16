import express from 'express';
import fetch from 'node-fetch';

const app = express();

// ── ENV ─────────────────────────────────────────────────────────
const KIRA_URL = process.env.KIRA_API_URL || 'https://cliente.monofloor.cloud/api';
const GH_TOKEN = process.env.GITHUB_TOKEN;
const GH_REPO = process.env.GITHUB_REPO || 'vitormonofloor/Monofloor_Files';
const GH_FILE = process.env.GITHUB_FILE || 'indicadores.html';
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const VITOR_CHAT_ID = process.env.VITOR_CHAT_ID || '8151246424';
const INTERVALO_HORAS = parseInt(process.env.INTERVALO_HORAS) || 6;

// ── STATE ───────────────────────────────────────────────────────
let ultimaExtracao = null;
let ultimoResumo = {};
let historicoExecucoes = [];

// ── KIRA EXTRACTION ─────────────────────────────────────────────

async function extrairDadosKIRA() {
  const inicio = Date.now();
  console.log('[AGENTE] Iniciando extração KIRA...');

  // 1. Buscar todos os projetos
  const rProj = await fetch(`${KIRA_URL}/projects?limit=500`);
  const allProjects = await rProj.json();
  const projects = Array.isArray(allProjects) ? allProjects : (allProjects.projects || []);

  // Filtrar ativos
  const finais = ['finalizado', 'concluido', 'cancelado'];
  const ativos = projects.filter(p => !finais.includes(p.status));

  console.log(`[AGENTE] ${ativos.length} projetos ativos de ${projects.length} total`);

  // 2. Extrair mensagens e ocorrências por projeto (batched)
  const batchSize = 5;
  const resultados = [];

  for (let i = 0; i < ativos.length; i += batchSize) {
    const batch = ativos.slice(i, i + batchSize);
    const batchResults = await Promise.all(batch.map(async (proj) => {
      const pid = proj.id;
      let msgs = [], ocs = [];

      try {
        const [rMsgs, rOcs] = await Promise.allSettled([
          fetch(`${KIRA_URL}/projects/${pid}/messages?source=all&limit=200`).then(r => r.ok ? r.json() : {}),
          fetch(`${KIRA_URL}/projects/${pid}/ocorrencias`).then(r => r.ok ? r.json() : []),
        ]);

        const msgData = rMsgs.status === 'fulfilled' ? rMsgs.value : {};
        msgs = msgData.messages || (Array.isArray(msgData) ? msgData : []);
        ocs = rOcs.status === 'fulfilled' ? (Array.isArray(rOcs.value) ? rOcs.value : rOcs.value.ocorrencias || []) : [];
      } catch (e) {
        console.error(`[AGENTE] Erro projeto ${pid}:`, e.message);
      }

      // Filtrar últimos 30 dias
      const agora = new Date();
      const dias30 = new Date(agora.getTime() - 30 * 86400000);
      const msgsRecentes = msgs.filter(m => {
        const d = m.timestamp || m.date || m.createdAt;
        return d && new Date(d) >= dias30;
      });

      const msgsTG = msgsRecentes.filter(m => (m.source || '').toLowerCase().includes('telegram'));
      const msgsWA = msgsRecentes.filter(m => (m.source || '').toLowerCase().includes('whatsapp'));

      // Atividade por dia
      const atividadePorDia = {};
      msgsRecentes.forEach(m => {
        const d = (m.timestamp || '').substring(0, 10);
        if (d) atividadePorDia[d] = (atividadePorDia[d] || 0) + 1;
      });

      // Autores
      const autores = new Set();
      msgsRecentes.forEach(m => { if (m.sender) autores.add(m.sender); });

      // Tipos de ocorrência
      const ocTipos = {};
      ocs.forEach(o => { const t = o.tipo || '?'; ocTipos[t] = (ocTipos[t] || 0) + 1; });

      return {
        id: pid,
        nome: proj.clienteNome || '?',
        cidade: proj.projetoCidade || '?',
        metragem: parseFloat(proj.projetoMetragem) || 0,
        cores: proj.projetoCores || [],
        tipoObra: proj.tipoObra || '?',
        status: proj.status || '?',
        fase: proj.faseAtual || '?',
        consultor: proj.consultorNome || '',
        dataExecPrevista: proj.dataExecucaoPrevista,
        createdAt: proj.createdAt,
        msgs30d: msgsRecentes.length,
        msgsTG: msgsTG.length,
        msgsWA: msgsWA.length,
        msgsTotal: msgs.length,
        diasAtivos: Object.keys(atividadePorDia).length,
        autores: autores.size,
        autoresLista: [...autores],
        atividadePorDia,
        totalOcs: ocs.length,
        ocTipos,
        ocsCriticas: ocs.filter(o => o.severidade === 'critica').length,
        ocsAltas: ocs.filter(o => o.severidade === 'alta').length,
        ocsAbertas: ocs.filter(o => o.status === 'aberta').length,
      };
    }));
    resultados.push(...batchResults);
    if (i + batchSize < ativos.length) await new Promise(r => setTimeout(r, 500));
  }

  // 3. Computar indicadores agregados
  const indicadores = computarIndicadores(resultados, projects);
  const duracao = ((Date.now() - inicio) / 1000).toFixed(1);

  console.log(`[AGENTE] Extração completa em ${duracao}s — ${resultados.length} projetos, ${indicadores.totalMsgs30d} msgs`);

  return { projetos: resultados, indicadores, meta: { geradoEm: new Date().toISOString(), duracao: parseFloat(duracao), totalProjetos: projects.length, projetosAtivos: ativos.length } };
}

// ── INDICATOR COMPUTATION ───────────────────────────────────────

function computarIndicadores(projetos, allProjects) {
  const agora = new Date();
  const finais = ['finalizado', 'concluido', 'cancelado'];

  // Totais
  const totalMsgs30d = projetos.reduce((s, p) => s + p.msgs30d, 0);
  const totalTG = projetos.reduce((s, p) => s + p.msgsTG, 0);
  const totalWA = projetos.reduce((s, p) => s + p.msgsWA, 0);
  const comMsgs = projetos.filter(p => p.msgs30d > 0).length;
  const semMsgs = projetos.filter(p => p.msgs30d === 0).length;
  const totalOcs = projetos.reduce((s, p) => s + p.totalOcs, 0);
  const ocsCriticas = projetos.reduce((s, p) => s + p.ocsCriticas, 0);
  const ocsAbertas = projetos.reduce((s, p) => s + p.ocsAbertas, 0);

  // Atrasados
  let atrasados = 0;
  projetos.forEach(p => {
    if (p.dataExecPrevista) {
      try { if (new Date(p.dataExecPrevista) < agora && !finais.includes(p.status)) atrasados++; } catch {}
    }
  });

  // Por consultor
  const porConsultor = {};
  projetos.forEach(p => {
    const c = (p.consultor || '').trim() || 'SEM';
    if (!porConsultor[c]) porConsultor[c] = { obras: 0, msgs: 0, ocs: 0, criticas: 0, atraso: 0 };
    porConsultor[c].obras++;
    porConsultor[c].msgs += p.msgs30d;
    porConsultor[c].ocs += p.totalOcs;
    porConsultor[c].criticas += p.ocsCriticas;
    if (p.dataExecPrevista) {
      try { if (new Date(p.dataExecPrevista) < agora && !finais.includes(p.status)) porConsultor[c].atraso++; } catch {}
    }
  });

  // Por região
  const porRegiao = {};
  projetos.forEach(p => {
    let r = (p.cidade || '?').toUpperCase();
    if (r.includes('SÃO PAULO') || r.includes('SAO PAULO')) r = 'SP';
    else if (r.includes('RIO')) r = 'RJ';
    else if (r.includes('CURITIBA')) r = 'CWB';
    else r = 'OUT';
    if (!porRegiao[r]) porRegiao[r] = { obras: 0, msgs: 0, ocs: 0 };
    porRegiao[r].obras++;
    porRegiao[r].msgs += p.msgs30d;
    porRegiao[r].ocs += p.totalOcs;
  });

  // Atividade por dia (global)
  const atividadeGlobal = {};
  projetos.forEach(p => {
    Object.entries(p.atividadePorDia || {}).forEach(([dia, count]) => {
      atividadeGlobal[dia] = (atividadeGlobal[dia] || 0) + count;
    });
  });

  // Top problemáticos
  const problematicos = projetos
    .filter(p => p.msgs30d >= 50 && p.totalOcs >= 5)
    .sort((a, b) => b.totalOcs - a.totalOcs)
    .slice(0, 15);

  // Silenciosos
  const silenciosos = projetos
    .filter(p => p.msgs30d === 0 && !finais.includes(p.status))
    .map(p => ({ nome: p.nome, status: p.status, fase: p.fase }));

  // Contradições
  const contradicoes = projetos.filter(p =>
    ((p.status || '') + (p.fase || '')).toLowerCase().includes('paus') && p.msgs30d > 20
  );

  // Tipos de ocorrência
  const tiposOcs = {};
  projetos.forEach(p => {
    Object.entries(p.ocTipos || {}).forEach(([t, c]) => {
      tiposOcs[t] = (tiposOcs[t] || 0) + c;
    });
  });

  // Top por msgs
  const topMsgs = [...projetos].sort((a, b) => b.msgs30d - a.msgs30d).slice(0, 20);

  // Reparos
  const reparos = allProjects.filter(p => (p.status || '').startsWith('reparo') || p.status === 'marcas_rolo_cera').length;
  const finalizados = allProjects.filter(p => finais.includes(p.status)).length;

  return {
    totalMsgs30d, totalTG, totalWA, comMsgs, semMsgs,
    totalOcs, ocsCriticas, ocsAbertas,
    atrasados, reparos, finalizados,
    porConsultor, porRegiao, atividadeGlobal,
    problematicos, silenciosos, contradicoes,
    tiposOcs, topMsgs,
    taxaAtraso: projetos.length ? ((atrasados / projetos.length) * 100).toFixed(1) : '0',
    taxaSilencio: projetos.length ? ((semMsgs / projetos.length) * 100).toFixed(1) : '0',
    taxaReparo: finalizados ? ((reparos / (finalizados + reparos)) * 100).toFixed(1) : '0',
  };
}

// ── DASHBOARD HTML GENERATION ───────────────────────────────────

function gerarDashboard(dados) {
  const { projetos, indicadores: I, meta } = dados;
  const dataFmt = new Date(meta.geradoEm).toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' });

  // Activity chart data
  const diasOrdenados = Object.entries(I.atividadeGlobal).sort((a, b) => a[0].localeCompare(b[0]));
  const maxMsgs = Math.max(...diasOrdenados.map(d => d[1]), 1);

  // Consultor rows
  const consRows = Object.entries(I.porConsultor)
    .sort((a, b) => b[1].obras - a[1].obras)
    .filter(([c]) => c !== 'SEM' && c !== '[]')
    .map(([c, d]) => {
      const nome = c.split(' ').slice(0, 2).join(' ');
      const ocsObra = d.obras ? (d.ocs / d.obras).toFixed(1) : '0';
      const pctAtraso = d.obras ? ((d.atraso / d.obras) * 100).toFixed(0) : '0';
      return `<tr><td>${nome}</td><td>${d.obras}</td><td>${d.msgs}</td><td>${d.ocs}</td><td>${ocsObra}</td><td>${pctAtraso}%</td></tr>`;
    }).join('');

  // Region rows
  const regRows = Object.entries(I.porRegiao)
    .sort((a, b) => b[1].obras - a[1].obras)
    .map(([r, d]) => {
      const avg = d.obras ? Math.round(d.msgs / d.obras) : 0;
      return `<tr><td>${r}</td><td>${d.obras}</td><td>${d.msgs}</td><td>${avg}</td><td>${d.ocs}</td></tr>`;
    }).join('');

  // Problem projects rows
  const probRows = I.problematicos.map(p =>
    `<tr><td>${p.nome.substring(0, 30)}</td><td>${p.msgs30d}</td><td class="oc-cell">${p.totalOcs}</td><td>${p.autores}</td><td>${(p.consultor || '').split(' ')[0]}</td><td><span class="badge ${p.status}">${p.fase.substring(0, 20)}</span></td></tr>`
  ).join('');

  // Ocorrência types
  const tipoRows = Object.entries(I.tiposOcs)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([t, c]) => `<tr><td>${t.replace(/_/g, ' ')}</td><td>${c}</td><td><div class="bar" style="width:${(c / I.totalOcs * 100).toFixed(0)}%"></div></td></tr>`)
    .join('');

  // Activity bars
  const actBars = diasOrdenados.map(([dia, count]) => {
    const pct = (count / maxMsgs * 100).toFixed(0);
    const label = dia.substring(5);
    const isWeekend = [0, 6].includes(new Date(dia + 'T12:00:00').getDay());
    return `<div class="act-col${isWeekend ? ' weekend' : ''}"><div class="act-bar" style="height:${pct}%"><span class="act-val">${count}</span></div><div class="act-label">${label}</div></div>`;
  }).join('');

  // Silent projects
  const silRows = I.silenciosos.slice(0, 15).map(s =>
    `<tr><td>${s.nome.substring(0, 30)}</td><td>${s.status}</td><td>${s.fase.substring(0, 25)}</td></tr>`
  ).join('');

  return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monofloor — Indicadores Operacionais</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0a0a;color:#e0e0e0;font-family:Inter,-apple-system,sans-serif;padding:20px;max-width:1400px;margin:0 auto}
h1{font-size:22px;font-weight:600;color:#c4a77d;margin-bottom:4px}
.sub{font-size:12px;color:#888;margin-bottom:24px}
.grid{display:grid;gap:16px;margin-bottom:24px}
.g2{grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.g4{grid-template-columns:repeat(auto-fit,minmax(150px,1fr))}
.g3{grid-template-columns:repeat(auto-fit,minmax(200px,1fr))}
.card{background:#141414;border:1px solid #222;border-radius:10px;padding:16px}
.card h3{font-size:13px;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px}
.kpi{text-align:center;padding:20px 12px}
.kpi .val{font-size:32px;font-weight:700;line-height:1}
.kpi .label{font-size:11px;color:#888;margin-top:6px}
.kpi.red .val{color:#ef4444}
.kpi.green .val{color:#22c55e}
.kpi.amber .val{color:#f59e0b}
.kpi.blue .val{color:#3b82f6}
.kpi.gold .val{color:#c4a77d}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:#888;font-weight:500;padding:8px 10px;border-bottom:1px solid #333}
td{padding:8px 10px;border-bottom:1px solid #1a1a1a}
tr:hover{background:#1a1a1a}
.oc-cell{color:#ef4444;font-weight:600}
.badge{display:inline-block;font-size:10px;padding:2px 8px;border-radius:4px;background:#222;color:#aaa}
.badge.em_execucao{background:#1a3a1a;color:#4ade80}
.badge.reparo{background:#3a1a1a;color:#f87171}
.bar{height:8px;background:#c4a77d;border-radius:4px;min-width:2px}
.act-wrap{display:flex;align-items:flex-end;gap:2px;height:120px;padding:0 4px}
.act-col{flex:1;display:flex;flex-direction:column;align-items:center;min-width:0}
.act-col.weekend .act-bar{background:#333}
.act-bar{width:100%;background:#c4a77d;border-radius:3px 3px 0 0;min-height:2px;display:flex;align-items:flex-start;justify-content:center;transition:height .5s}
.act-val{font-size:8px;color:#0a0a0a;font-weight:600;padding-top:2px;display:none}
.act-col:hover .act-val{display:block}
.act-label{font-size:9px;color:#555;margin-top:4px;white-space:nowrap}
.alert-box{background:#1a1010;border:1px solid #ef4444;border-radius:8px;padding:12px 16px;margin-bottom:16px}
.alert-box h4{color:#ef4444;font-size:14px;margin-bottom:6px}
.alert-box p{font-size:12px;color:#ccc}
.logo{display:flex;align-items:center;gap:12px;margin-bottom:24px}
.logo-text{font-size:28px;font-weight:300;letter-spacing:6px;color:#c4a77d;text-transform:uppercase}
.logo-tag{font-size:9px;letter-spacing:3px;color:#666;text-transform:uppercase}
.section{margin-bottom:32px}
.section-title{font-size:15px;font-weight:600;color:#c4a77d;margin-bottom:12px;padding-bottom:6px;border-bottom:1px solid #222}
</style>
</head>
<body>
<div class="logo">
  <div>
    <div class="logo-text">monofloor</div>
    <div class="logo-tag">Premium Unique Surfaces™</div>
  </div>
</div>

<h1>Painel de Indicadores Operacionais</h1>
<div class="sub">Atualizado: ${dataFmt} · Extração: ${meta.duracao}s · ${meta.projetosAtivos} projetos ativos de ${meta.totalProjetos}</div>

${I.ocsCriticas > 0 ? `<div class="alert-box"><h4>⚠ ${I.ocsCriticas} ocorrências CRÍTICAS sem resolução</h4><p>${I.ocsAbertas} de ${I.totalOcs} ocorrências abertas (${I.totalOcs > 0 ? ((I.ocsAbertas/I.totalOcs)*100).toFixed(0) : 0}%). Taxa de resolução: ${I.totalOcs > 0 ? (((I.totalOcs - I.ocsAbertas)/I.totalOcs)*100).toFixed(0) : 0}%.</p></div>` : ''}

<div class="section">
<div class="grid g4">
  <div class="card kpi gold"><div class="val">${I.totalMsgs30d.toLocaleString('pt-BR')}</div><div class="label">Mensagens 30d</div></div>
  <div class="card kpi blue"><div class="val">${I.comMsgs}</div><div class="label">Projetos com msgs</div></div>
  <div class="card kpi ${parseFloat(I.taxaSilencio) > 15 ? 'amber' : 'green'}"><div class="val">${I.taxaSilencio}%</div><div class="label">Taxa silêncio</div></div>
  <div class="card kpi ${I.totalOcs > 100 ? 'red' : 'amber'}"><div class="val">${I.totalOcs}</div><div class="label">Ocorrências abertas</div></div>
  <div class="card kpi ${parseFloat(I.taxaAtraso) > 20 ? 'red' : parseFloat(I.taxaAtraso) > 15 ? 'amber' : 'green'}"><div class="val">${I.taxaAtraso}%</div><div class="label">Taxa atraso</div></div>
  <div class="card kpi blue"><div class="val">${I.totalTG}</div><div class="label">Telegram</div></div>
  <div class="card kpi green"><div class="val">${I.totalWA}</div><div class="label">WhatsApp</div></div>
  <div class="card kpi red"><div class="val">${I.ocsCriticas}</div><div class="label">Críticas</div></div>
</div>
</div>

<div class="section">
<div class="section-title">Atividade Diária (30d)</div>
<div class="card"><div class="act-wrap">${actBars}</div></div>
</div>

<div class="section">
<div class="grid g2">
  <div class="card">
    <h3>Por Consultor</h3>
    <table><tr><th>Consultor</th><th>Obras</th><th>Msgs</th><th>Ocs</th><th>Ocs/obra</th><th>Atraso</th></tr>${consRows}</table>
  </div>
  <div class="card">
    <h3>Por Região</h3>
    <table><tr><th>Região</th><th>Obras</th><th>Msgs</th><th>Msgs/obra</th><th>Ocs</th></tr>${regRows}</table>
  </div>
</div>
</div>

<div class="section">
<div class="section-title">Projetos Problemáticos (alta msg + alta ocorrência)</div>
<div class="card">
  <table><tr><th>Projeto</th><th>Msgs</th><th>Ocs</th><th>Autores</th><th>Consultor</th><th>Fase</th></tr>${probRows}</table>
</div>
</div>

<div class="section">
<div class="grid g2">
  <div class="card">
    <h3>Tipos de Ocorrência</h3>
    <table><tr><th>Tipo</th><th>Qtd</th><th></th></tr>${tipoRows}</table>
  </div>
  <div class="card">
    <h3>Projetos Silenciosos (${I.silenciosos.length})</h3>
    <table><tr><th>Projeto</th><th>Status</th><th>Fase</th></tr>${silRows}</table>
  </div>
</div>
</div>

<div style="text-align:center;padding:20px;color:#444;font-size:11px">
  Agente Indicadores Monofloor v1.0 · Atualização automática a cada ${INTERVALO_HORAS}h · Dados: KIRA API
</div>
</body></html>`;
}

// ── GITHUB PUBLISH ──────────────────────────────────────────────

async function publicarGitHub(html) {
  if (!GH_TOKEN) { console.log('[AGENTE] GH_TOKEN não configurado — publish ignorado'); return false; }

  try {
    // Get current file SHA (if exists)
    let sha = null;
    try {
      const r = await fetch(`https://api.github.com/repos/${GH_REPO}/contents/${GH_FILE}`, {
        headers: { 'Authorization': `token ${GH_TOKEN}`, 'Accept': 'application/vnd.github.v3+json' },
      });
      if (r.ok) { const d = await r.json(); sha = d.sha; }
    } catch {}

    // Create/update file
    const body = {
      message: `📊 Indicadores atualizados — ${new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' })}`,
      content: Buffer.from(html, 'utf-8').toString('base64'),
    };
    if (sha) body.sha = sha;

    const r = await fetch(`https://api.github.com/repos/${GH_REPO}/contents/${GH_FILE}`, {
      method: 'PUT',
      headers: { 'Authorization': `token ${GH_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (r.ok) {
      console.log(`[AGENTE] Dashboard publicado em ${GH_REPO}/${GH_FILE}`);
      return true;
    } else {
      const err = await r.text();
      console.error('[AGENTE] Erro GitHub:', err);
      return false;
    }
  } catch (e) {
    console.error('[AGENTE] Erro GitHub:', e.message);
    return false;
  }
}

// ── TELEGRAM NOTIFICATION ───────────────────────────────────────

async function notificarTelegram(indicadores, meta) {
  if (!TG_TOKEN || !VITOR_CHAT_ID) return;

  const I = indicadores;
  const emojiAtraso = parseFloat(I.taxaAtraso) > 20 ? '🔴' : parseFloat(I.taxaAtraso) > 15 ? '🟡' : '🟢';

  let msg = `📊 *Indicadores Atualizados*\n`;
  msg += `_${new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' })}_\n\n`;
  msg += `💬 *${I.totalMsgs30d.toLocaleString('pt-BR')}* msgs (30d) | TG: ${I.totalTG} WA: ${I.totalWA}\n`;
  msg += `📋 *${I.totalOcs}* ocorrências (${I.ocsCriticas} críticas)\n`;
  msg += `${emojiAtraso} Atraso: *${I.taxaAtraso}%* | Silêncio: *${I.taxaSilencio}%*\n`;
  msg += `🔨 ${I.comMsgs} projetos ativos | ${I.semMsgs} silenciosos\n\n`;

  if (I.problematicos.length) {
    msg += `🔴 *Top problemáticos:*\n`;
    I.problematicos.slice(0, 5).forEach(p => {
      msg += `• ${p.nome.substring(0, 25)}: ${p.totalOcs} ocs\n`;
    });
  }

  msg += `\n📈 [Ver painel](https://vitormonofloor.github.io/Monofloor_Files/${GH_FILE})`;

  try {
    await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: VITOR_CHAT_ID, text: msg, parse_mode: 'Markdown', disable_web_page_preview: true }),
    });
    console.log('[AGENTE] Notificação Telegram enviada');
  } catch (e) {
    console.error('[AGENTE] Erro Telegram:', e.message);
  }
}

// ── MAIN CYCLE ──────────────────────────────────────────────────

async function cicloCompleto() {
  try {
    console.log('\n═══════════════════════════════════════');
    console.log(`[AGENTE] Ciclo iniciado — ${new Date().toISOString()}`);

    // 1. Extrair
    const dados = await extrairDadosKIRA();

    // 2. Gerar dashboard
    const html = gerarDashboard(dados);

    // 3. Publicar
    const publicado = await publicarGitHub(html);

    // 4. Notificar
    await notificarTelegram(dados.indicadores, dados.meta);

    // 5. Salvar estado
    ultimaExtracao = dados;
    ultimoResumo = {
      timestamp: dados.meta.geradoEm,
      msgs: dados.indicadores.totalMsgs30d,
      ocs: dados.indicadores.totalOcs,
      projetos: dados.meta.projetosAtivos,
      publicado,
    };
    historicoExecucoes.push(ultimoResumo);
    if (historicoExecucoes.length > 50) historicoExecucoes = historicoExecucoes.slice(-50);

    console.log(`[AGENTE] Ciclo completo ✓`);
    console.log('═══════════════════════════════════════\n');
  } catch (e) {
    console.error('[AGENTE] ERRO no ciclo:', e.message);
  }
}

// ── SCHEDULER ───────────────────────────────────────────────────

function getBRHour() {
  const now = new Date();
  const brTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }));
  return brTime.getHours();
}

function startScheduler() {
  // Run every hour, execute on target hours
  const targetHours = [];
  for (let h = 0; h < 24; h += INTERVALO_HORAS) targetHours.push(h);

  setInterval(() => {
    const h = getBRHour();
    const m = new Date().getMinutes();
    if (targetHours.includes(h) && m < 5) {
      cicloCompleto();
    }
  }, 5 * 60 * 1000);

  console.log(`[AGENTE] Scheduler: ciclo a cada ${INTERVALO_HORAS}h (horários BR: ${targetHours.join('h, ')}h)`);
}

// ── API ENDPOINTS ───────────────────────────────────────────────

app.get('/', (req, res) => {
  res.json({
    agente: 'Indicadores Monofloor',
    version: '1.0.0',
    status: 'online',
    intervalo: `${INTERVALO_HORAS}h`,
    ultimaExtracao: ultimoResumo.timestamp || 'nunca',
    historico: historicoExecucoes.length + ' execuções',
    dashboard: `https://vitormonofloor.github.io/Monofloor_Files/${GH_FILE}`,
  });
});

app.get('/api/dados', (req, res) => {
  if (!ultimaExtracao) return res.json({ error: 'Nenhuma extração realizada ainda. Aguarde o primeiro ciclo ou acesse /api/executar.' });
  res.json(ultimaExtracao);
});

app.get('/api/historico', (req, res) => {
  res.json(historicoExecucoes);
});

app.get('/api/executar', async (req, res) => {
  res.json({ status: 'Ciclo iniciado. Aguarde ~90 segundos.' });
  cicloCompleto();
});

// ── START ───────────────────────────────────────────────────────

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`\n🤖 Agente Indicadores Monofloor v1.0 — port ${PORT}`);
  console.log(`   KIRA: ${KIRA_URL}`);
  console.log(`   GitHub: ${GH_REPO}/${GH_FILE}`);
  console.log(`   Telegram: ${VITOR_CHAT_ID ? '✓' : '⚠️ não configurado'}`);
  console.log(`   Intervalo: ${INTERVALO_HORAS}h\n`);

  startScheduler();

  // Primeira execução 60s após boot
  setTimeout(() => {
    console.log('[AGENTE] Primeira execução automática...');
    cicloCompleto();
  }, 60000);
});
