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

      // Detalhes das ocorrências (para drill-down)
      const ocsDetalhes = ocs.slice(0, 30).map(o => ({
        titulo: o.titulo || '?',
        tipo: o.tipo || '?',
        severidade: o.severidade || '?',
        status: o.status || '?',
        criado: o.createdAt || null,
        descricao: (o.descricao || '').substring(0, 200),
      }));

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
        ocsDetalhes,
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
  const agora = new Date();
  const finais = ['finalizado', 'concluido', 'cancelado'];

  // ── PREPARAR DADOS PARA DRILL-DOWN ──

  // 1. Projetos silenciosos (detalhes)
  const silenciososDetalhes = projetos
    .filter(p => p.msgs30d === 0 && !finais.includes(p.status))
    .map(p => ({ nome: p.nome, status: p.status, fase: p.fase, consultor: p.consultor, cidade: p.cidade }));

  // 2. Todas as ocorrências consolidadas
  const todasOcs = [];
  projetos.forEach(p => {
    (p.ocsDetalhes || []).forEach(o => {
      todasOcs.push({ ...o, projeto: p.nome, consultor: p.consultor });
    });
  });

  // 3. Ocorrências críticas (drill-down)
  const ocsCriticasDetalhes = todasOcs
    .filter(o => o.severidade === 'critica')
    .sort((a, b) => (b.criado || '').localeCompare(a.criado || ''));

  // 4. Projetos atrasados (detalhes)
  const atrasadosDetalhes = projetos
    .filter(p => {
      if (!p.dataExecPrevista || finais.includes(p.status)) return false;
      try { return new Date(p.dataExecPrevista) < agora; } catch { return false; }
    })
    .map(p => {
      const dias = Math.floor((agora - new Date(p.dataExecPrevista)) / 86400000);
      return { nome: p.nome, diasAtraso: dias, status: p.status, fase: p.fase, consultor: p.consultor, previsto: p.dataExecPrevista };
    })
    .sort((a, b) => b.diasAtraso - a.diasAtraso);

  // 5. Projetos com mensagens (detalhes)
  const ativosDetalhes = projetos
    .filter(p => p.msgs30d > 0)
    .sort((a, b) => b.msgs30d - a.msgs30d)
    .slice(0, 50)
    .map(p => ({ nome: p.nome, msgs: p.msgs30d, tg: p.msgsTG, wa: p.msgsWA, dias: p.diasAtivos, autores: p.autores, fase: p.fase }));

  // 6. Ocorrências por severidade
  const ocsPorSev = { critica: [], alta: [], media: [], baixa: [] };
  todasOcs.forEach(o => {
    if (ocsPorSev[o.severidade]) ocsPorSev[o.severidade].push(o);
  });

  // Activity chart
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

  // Problem projects
  const probRows = I.problematicos.map(p =>
    `<tr><td>${p.nome.substring(0, 30)}</td><td>${p.msgs30d}</td><td class="oc-cell">${p.totalOcs}</td><td>${p.autores}</td><td>${(p.consultor || '').split(' ')[0]}</td><td><span class="badge ${p.status}">${p.fase.substring(0, 20)}</span></td></tr>`
  ).join('');

  // Tipos
  const tipoRows = Object.entries(I.tiposOcs)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([t, c]) => `<tr><td>${t.replace(/_/g, ' ')}</td><td>${c}</td><td><div class="bar" style="width:${(c / I.totalOcs * 100).toFixed(0)}%"></div></td></tr>`)
    .join('');

  const actBars = diasOrdenados.map(([dia, count]) => {
    const pct = (count / maxMsgs * 100).toFixed(0);
    const label = dia.substring(5);
    const isWeekend = [0, 6].includes(new Date(dia + 'T12:00:00').getDay());
    return `<div class="act-col${isWeekend ? ' weekend' : ''}"><div class="act-bar" style="height:${pct}%"><span class="act-val">${count}</span></div><div class="act-label">${label}</div></div>`;
  }).join('');

  const silRows = I.silenciosos.slice(0, 15).map(s =>
    `<tr><td>${s.nome.substring(0, 30)}</td><td>${s.status}</td><td>${s.fase.substring(0, 25)}</td></tr>`
  ).join('');

  // ── PRÉ-RENDERIZAR TABELAS DOS MODAIS ──

  const modalMsgsTable = ativosDetalhes.map(p =>
    `<tr><td>${p.nome.substring(0, 35)}</td><td class="num">${p.msgs}</td><td class="num">${p.tg}</td><td class="num">${p.wa}</td><td class="num">${p.dias}/30</td><td class="num">${p.autores}</td><td>${p.fase.substring(0, 22)}</td></tr>`
  ).join('');

  const modalSilTable = silenciososDetalhes.map(s =>
    `<tr><td>${s.nome.substring(0, 35)}</td><td><span class="tag">${s.status}</span></td><td>${(s.fase || '?').substring(0, 25)}</td><td>${(s.consultor || '').split(' ').slice(0, 2).join(' ')}</td></tr>`
  ).join('');

  const modalOcsTable = todasOcs.slice(0, 100).sort((a, b) => {
    const sevOrder = { critica: 0, alta: 1, media: 2, baixa: 3, '?': 4 };
    return (sevOrder[a.severidade] || 5) - (sevOrder[b.severidade] || 5);
  }).map(o =>
    `<tr><td><span class="sev-${o.severidade}">${o.severidade}</span></td><td>${(o.titulo || '?').substring(0, 50)}</td><td>${o.projeto.substring(0, 25)}</td><td>${(o.tipo || '?').replace(/_/g, ' ')}</td></tr>`
  ).join('');

  const modalCritTable = ocsCriticasDetalhes.map(o =>
    `<tr><td>${(o.titulo || '?').substring(0, 55)}</td><td>${o.projeto.substring(0, 22)}</td><td>${(o.tipo || '?').replace(/_/g, ' ')}</td><td>${o.criado ? new Date(o.criado).toLocaleDateString('pt-BR') : '?'}</td></tr>`
  ).join('');

  const modalAtrasoTable = atrasadosDetalhes.map(p =>
    `<tr><td>${p.nome.substring(0, 32)}</td><td class="num sev-critica">${p.diasAtraso}d</td><td>${p.fase.substring(0, 22)}</td><td><span class="tag">${p.status}</span></td><td>${(p.consultor || '').split(' ').slice(0, 2).join(' ')}</td></tr>`
  ).join('');

  const totalAtivos = projetos.length;

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
.card{background:#141414;border:1px solid #222;border-radius:10px;padding:16px}
.card h3{font-size:13px;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px}
.kpi{text-align:center;padding:20px 12px;transition:transform .15s,border-color .15s}
.kpi.clickable{cursor:pointer}
.kpi.clickable:hover{transform:translateY(-2px);border-color:#c4a77d}
.kpi.clickable::after{content:"";position:absolute;top:8px;right:10px;width:14px;height:14px;border:1px solid #444;border-radius:50%;display:flex;align-items:center;justify-content:center}
.kpi{position:relative}
.kpi.clickable::before{content:"i";position:absolute;top:8px;right:10px;width:14px;height:14px;font-size:9px;color:#666;font-style:italic;text-align:center;line-height:14px;border:1px solid #333;border-radius:50%}
.kpi.clickable:hover::before{color:#c4a77d;border-color:#c4a77d}
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
.num{text-align:right;font-variant-numeric:tabular-nums}
tr:hover{background:#1a1a1a}
.oc-cell{color:#ef4444;font-weight:600}
.badge,.tag{display:inline-block;font-size:10px;padding:2px 8px;border-radius:4px;background:#222;color:#aaa}
.badge.em_execucao{background:#1a3a1a;color:#4ade80}
.badge.reparo{background:#3a1a1a;color:#f87171}
.sev-critica{color:#ef4444;font-weight:600;text-transform:uppercase;font-size:11px}
.sev-alta{color:#f59e0b;font-weight:600;text-transform:uppercase;font-size:11px}
.sev-media{color:#fbbf24;font-size:11px}
.sev-baixa{color:#888;font-size:11px}
.bar{height:8px;background:#c4a77d;border-radius:4px;min-width:2px}
.act-wrap{display:flex;align-items:flex-end;gap:2px;height:120px;padding:0 4px}
.act-col{flex:1;display:flex;flex-direction:column;align-items:center;min-width:0}
.act-col.weekend .act-bar{background:#333}
.act-bar{width:100%;background:#c4a77d;border-radius:3px 3px 0 0;min-height:2px;display:flex;align-items:flex-start;justify-content:center;transition:height .5s}
.act-val{font-size:8px;color:#0a0a0a;font-weight:600;padding-top:2px;display:none}
.act-col:hover .act-val{display:block}
.act-label{font-size:9px;color:#555;margin-top:4px;white-space:nowrap}
.alert-box{background:#1a1010;border:1px solid #ef4444;border-radius:8px;padding:12px 16px;margin-bottom:16px;cursor:pointer;transition:border-color .15s}
.alert-box:hover{border-color:#ff6b6b}
.alert-box h4{color:#ef4444;font-size:14px;margin-bottom:6px}
.alert-box p{font-size:12px;color:#ccc}
.logo{display:flex;align-items:center;gap:12px;margin-bottom:24px}
.logo-text{font-size:28px;font-weight:300;letter-spacing:6px;color:#c4a77d;text-transform:uppercase}
.logo-tag{font-size:9px;letter-spacing:3px;color:#666;text-transform:uppercase}
.section{margin-bottom:32px}
.section-title{font-size:15px;font-weight:600;color:#c4a77d;margin-bottom:12px;padding-bottom:6px;border-bottom:1px solid #222}
/* MODAL */
.modal-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.85);z-index:100;padding:40px 20px;overflow-y:auto;backdrop-filter:blur(4px)}
.modal-overlay.open{display:block}
.modal{max-width:1000px;margin:0 auto;background:#141414;border:1px solid #333;border-radius:12px;padding:24px}
.modal-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #222}
.modal-title{font-size:20px;color:#c4a77d;font-weight:600;margin-bottom:4px}
.modal-subtitle{font-size:12px;color:#888}
.modal-close{background:none;border:1px solid #333;color:#888;width:32px;height:32px;border-radius:50%;cursor:pointer;font-size:18px;line-height:1}
.modal-close:hover{color:#fff;border-color:#c4a77d}
.def-box{background:#0f0f0f;border:1px solid #222;border-radius:8px;padding:14px;margin-bottom:20px}
.def-box h4{font-size:11px;color:#c4a77d;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.def-box p{font-size:13px;color:#ccc;line-height:1.5;margin-bottom:6px}
.def-box code{background:#1a1a1a;padding:2px 6px;border-radius:3px;color:#c4a77d;font-family:monospace;font-size:12px}
.drill-title{font-size:13px;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px}
.modal table{font-size:12px}
.modal th{font-size:10px;text-transform:uppercase}
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
<div class="sub">Atualizado: ${dataFmt} · Extração: ${meta.duracao}s · ${meta.projetosAtivos} projetos ativos de ${meta.totalProjetos} · <span style="color:#c4a77d">clique nos cards para detalhes</span></div>

${I.ocsCriticas > 0 ? `<div class="alert-box" data-modal="criticas"><h4>⚠ ${I.ocsCriticas} ocorrências CRÍTICAS sem resolução</h4><p>${I.ocsAbertas} de ${I.totalOcs} ocorrências abertas (${I.totalOcs > 0 ? ((I.ocsAbertas/I.totalOcs)*100).toFixed(0) : 0}%). Taxa de resolução: ${I.totalOcs > 0 ? (((I.totalOcs - I.ocsAbertas)/I.totalOcs)*100).toFixed(0) : 0}%. Clique para ver detalhes.</p></div>` : ''}

<div class="section">
<div class="grid g4">
  <div class="card kpi gold clickable" data-modal="msgs"><div class="val">${I.totalMsgs30d.toLocaleString('pt-BR')}</div><div class="label">Mensagens 30d</div></div>
  <div class="card kpi blue clickable" data-modal="ativos"><div class="val">${I.comMsgs}</div><div class="label">Projetos com msgs</div></div>
  <div class="card kpi ${parseFloat(I.taxaSilencio) > 15 ? 'amber' : 'green'} clickable" data-modal="silencio"><div class="val">${I.taxaSilencio}%</div><div class="label">Taxa silêncio</div></div>
  <div class="card kpi ${I.totalOcs > 100 ? 'red' : 'amber'} clickable" data-modal="ocorrencias"><div class="val">${I.totalOcs}</div><div class="label">Ocorrências abertas</div></div>
  <div class="card kpi ${parseFloat(I.taxaAtraso) > 20 ? 'red' : parseFloat(I.taxaAtraso) > 15 ? 'amber' : 'green'} clickable" data-modal="atraso"><div class="val">${I.taxaAtraso}%</div><div class="label">Taxa atraso</div></div>
  <div class="card kpi blue"><div class="val">${I.totalTG}</div><div class="label">Telegram</div></div>
  <div class="card kpi green"><div class="val">${I.totalWA}</div><div class="label">WhatsApp</div></div>
  <div class="card kpi red clickable" data-modal="criticas"><div class="val">${I.ocsCriticas}</div><div class="label">Críticas</div></div>
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
  Argos v1.1 · Atualização automática a cada ${INTERVALO_HORAS}h · Dados: KIRA API
</div>

<!-- MODAIS -->

<div class="modal-overlay" id="modal-msgs">
  <div class="modal">
    <div class="modal-header">
      <div><div class="modal-title">Mensagens — últimos 30 dias</div><div class="modal-subtitle">${I.totalMsgs30d.toLocaleString('pt-BR')} mensagens detectadas</div></div>
      <button class="modal-close" data-close>×</button>
    </div>
    <div class="def-box">
      <h4>Definição</h4>
      <p>Total de mensagens trocadas nos grupos de Telegram e WhatsApp de obra detectadas pela <code>KIRA</code>.</p>
      <h4 style="margin-top:12px">Critério</h4>
      <p>Janela: <code>últimos 30 dias</code> corridos a partir de agora. Fonte: campo <code>timestamp</code> de cada mensagem. Inclui Telegram (${I.totalTG}) e WhatsApp (${I.totalWA}).</p>
    </div>
    <div class="drill-title">Top 50 projetos com mais mensagens</div>
    <table><tr><th>Projeto</th><th class="num">Msgs</th><th class="num">TG</th><th class="num">WA</th><th class="num">Dias</th><th class="num">Autores</th><th>Fase</th></tr>${modalMsgsTable}</table>
  </div>
</div>

<div class="modal-overlay" id="modal-ativos">
  <div class="modal">
    <div class="modal-header">
      <div><div class="modal-title">Projetos com mensagens (30d)</div><div class="modal-subtitle">${I.comMsgs} projetos de ${totalAtivos} ativos</div></div>
      <button class="modal-close" data-close>×</button>
    </div>
    <div class="def-box">
      <h4>Definição</h4>
      <p>Projetos que receberam pelo menos uma mensagem no Telegram ou WhatsApp nos últimos 30 dias.</p>
      <h4 style="margin-top:12px">Critério</h4>
      <p>Quantidade de projetos com <code>msgs30d &gt; 0</code>. Total analisado: ${totalAtivos} projetos ativos (excluindo finalizados, concluídos e cancelados).</p>
    </div>
    <div class="drill-title">Top 50 mais ativos</div>
    <table><tr><th>Projeto</th><th class="num">Msgs</th><th class="num">TG</th><th class="num">WA</th><th class="num">Dias</th><th class="num">Autores</th><th>Fase</th></tr>${modalMsgsTable}</table>
  </div>
</div>

<div class="modal-overlay" id="modal-silencio">
  <div class="modal">
    <div class="modal-header">
      <div><div class="modal-title">Taxa de Silêncio</div><div class="modal-subtitle">${I.taxaSilencio}% — ${I.semMsgs} de ${totalAtivos} projetos ativos sem comunicação</div></div>
      <button class="modal-close" data-close>×</button>
    </div>
    <div class="def-box">
      <h4>Definição</h4>
      <p>Percentual de projetos ativos <strong>sem qualquer mensagem</strong> no Telegram ou WhatsApp nos últimos 30 dias.</p>
      <h4 style="margin-top:12px">Fórmula</h4>
      <p><code>(projetos sem msgs nos últimos 30d) ÷ (total de projetos ativos) × 100</code></p>
      <h4 style="margin-top:12px">Critério</h4>
      <p>Um projeto é considerado "silencioso" quando <code>msgs30d == 0</code>. Projetos finalizados, concluídos e cancelados são excluídos do cálculo.</p>
      <h4 style="margin-top:12px">Meta</h4>
      <p>Manter abaixo de <code>10%</code>. Silêncio prolongado pode indicar problemas de acompanhamento, perda de visibilidade ou projetos abandonados.</p>
    </div>
    <div class="drill-title">Projetos silenciosos (${silenciososDetalhes.length})</div>
    <table><tr><th>Projeto</th><th>Status</th><th>Fase</th><th>Consultor</th></tr>${modalSilTable}</table>
  </div>
</div>

<div class="modal-overlay" id="modal-ocorrencias">
  <div class="modal">
    <div class="modal-header">
      <div><div class="modal-title">Ocorrências Abertas</div><div class="modal-subtitle">${I.totalOcs} ocorrências registradas · ${I.ocsCriticas} críticas · ${I.ocsAbertas} ainda abertas</div></div>
      <button class="modal-close" data-close>×</button>
    </div>
    <div class="def-box">
      <h4>Definição</h4>
      <p>Ocorrências operacionais registradas pela <code>KIRA</code> a partir da análise das mensagens dos grupos. Cada ocorrência tem <strong>tipo</strong> (falha_comunicacao, desvio_qualidade, atraso, etc) e <strong>severidade</strong> (crítica, alta, média, baixa).</p>
      <h4 style="margin-top:12px">Critério</h4>
      <p>Todas as ocorrências registradas nos projetos ativos, independente de data. Status "aberta" significa que ainda não foi resolvida.</p>
      <h4 style="margin-top:12px">Distribuição por severidade</h4>
      <p>🔴 Crítica: <code>${ocsPorSev.critica.length}</code> · 🟠 Alta: <code>${ocsPorSev.alta.length}</code> · 🟡 Média: <code>${ocsPorSev.media.length}</code> · 🟢 Baixa: <code>${ocsPorSev.baixa.length}</code></p>
    </div>
    <div class="drill-title">Ocorrências ordenadas por severidade (top 100)</div>
    <table><tr><th>Sev</th><th>Título</th><th>Projeto</th><th>Tipo</th></tr>${modalOcsTable}</table>
  </div>
</div>

<div class="modal-overlay" id="modal-criticas">
  <div class="modal">
    <div class="modal-header">
      <div><div class="modal-title">Ocorrências Críticas</div><div class="modal-subtitle">${I.ocsCriticas} ocorrências com severidade crítica sem resolução</div></div>
      <button class="modal-close" data-close>×</button>
    </div>
    <div class="def-box">
      <h4>Definição</h4>
      <p>Ocorrências marcadas com severidade <code>critica</code> pela KIRA. Representam situações de risco alto: clientes aguardando há semanas, defeitos graves em obra, quebras de comunicação prolongadas.</p>
      <h4 style="margin-top:12px">Prioridade</h4>
      <p>Todas as críticas devem ser tratadas imediatamente. Taxa de resolução atual: <code>0%</code>.</p>
    </div>
    <div class="drill-title">Críticas abertas (mais recentes primeiro)</div>
    <table><tr><th>Título</th><th>Projeto</th><th>Tipo</th><th>Criada em</th></tr>${modalCritTable}</table>
  </div>
</div>

<div class="modal-overlay" id="modal-atraso">
  <div class="modal">
    <div class="modal-header">
      <div><div class="modal-title">Taxa de Atraso</div><div class="modal-subtitle">${I.taxaAtraso}% — ${atrasadosDetalhes.length} projetos ultrapassaram a data prevista</div></div>
      <button class="modal-close" data-close>×</button>
    </div>
    <div class="def-box">
      <h4>Definição</h4>
      <p>Percentual de projetos ativos cuja <code>dataExecucaoPrevista</code> já passou mas ainda não foram finalizados.</p>
      <h4 style="margin-top:12px">Fórmula</h4>
      <p><code>(projetos atrasados) ÷ (total ativos) × 100</code></p>
      <h4 style="margin-top:12px">Critério</h4>
      <p>Um projeto está atrasado quando <code>dataExecucaoPrevista &lt; hoje</code> <strong>E</strong> status não é <code>finalizado</code>, <code>concluido</code> ou <code>cancelado</code>.</p>
      <h4 style="margin-top:12px">Meta</h4>
      <p>Manter abaixo de <code>15%</code>. Acima de <code>20%</code> é sinal vermelho.</p>
    </div>
    <div class="drill-title">Projetos atrasados (mais atrasados primeiro)</div>
    <table><tr><th>Projeto</th><th class="num">Atraso</th><th>Fase</th><th>Status</th><th>Consultor</th></tr>${modalAtrasoTable}</table>
  </div>
</div>

<script>
document.querySelectorAll('[data-modal]').forEach(el => {
  el.addEventListener('click', () => {
    const id = 'modal-' + el.dataset.modal;
    const m = document.getElementById(id);
    if (m) { m.classList.add('open'); document.body.style.overflow = 'hidden'; }
  });
});
document.querySelectorAll('[data-close]').forEach(btn => {
  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    btn.closest('.modal-overlay').classList.remove('open');
    document.body.style.overflow = '';
  });
});
document.querySelectorAll('.modal-overlay').forEach(ov => {
  ov.addEventListener('click', (e) => {
    if (e.target === ov) { ov.classList.remove('open'); document.body.style.overflow = ''; }
  });
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    document.querySelectorAll('.modal-overlay.open').forEach(m => m.classList.remove('open'));
    document.body.style.overflow = '';
  }
});
</script>
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
