import { useState, useMemo, useRef, useEffect } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis, CartesianGrid, LineChart, Line, ComposedChart, Area, AreaChart } from "recharts";

// ═══════════════════════════════════════════════════════
// THEME & CONSTANTS
// ═══════════════════════════════════════════════════════
const T = {
  green: "#1B7A3D", greenDark: "#0d3f1a", greenLight: "#e6f2eb",
  blue: "#2E86AB", orange: "#E8871E", purple: "#7B2D8E",
  red: "#C4342D", gold: "#D4A843", bg: "#f4f7f4",
  text: "#1a2e1a", textLight: "#6a7a6a", border: "#e0e4e0",
};

const TSBIO_COLORS = {
  "Altamira": T.green, "Juruá-Tefé": T.blue, "Macapá": T.orange,
  "Portel": T.purple, "Rio Branco-Brasiléia": T.red, "Salgado-Bragantino": T.gold,
};
const TSBIO_LIST = Object.keys(TSBIO_COLORS);

// ═══════════════════════════════════════════════════════
// DATA HELPERS
// ═══════════════════════════════════════════════════════

/** Parse Brazilian number format: "1.245,26" → 1245.26, "0,94" → 0.94 */
function pn(s) {
  if (typeof s === "number") return s;
  if (s == null || s === "") return 0;
  return parseFloat(String(s).replace(/\./g, "").replace(",", ".")) || 0;
}

/**
 * Build the normalized municipality array (M) from the fetched JSON data.
 * Uses dim_socioeconomica.json as the primary data source.
 */
function buildMunicipios(munJson, socioJson) {
  const I = socioJson.indicadores;
  const get = (id) => I[id]?.dados || {};

  const densD      = get("populacao_densidade_demografica");
  const popResD    = get("populacao_populacao_residente");
  const situD      = get("populacao_situacao_do_domicilio");
  const racaD      = get("populacao_populacao_por_cor_ou_raca");
  const alfabD     = get("populacao_alfabetizacao");
  const idMedD     = get("populacao_idade_mediana_da_populacao");
  const sexoD      = get("populacao_populacao_por_sexo");
  const idhmD      = get("indices_indice_de_desenvolvimento_municipal_dhm");
  const giniD      = get("indices_indice_de_gini");
  const rendaD     = get("trabalho_e_renda_rendimento_domiciliar_mensal_per_capita");
  const pibD       = get("economia_pib");
  const posOcupD   = get("trabalho_e_renda_posicao_na_ocupacao");
  const nivelInstrD= get("educacao_nivel_de_instrucao");
  const quiIndD    = get("populacao_populacao_quilombola_e_indigenas");
  const aguaD      = get("domicilios_abastecimento_de_agua_pela_rede_geral");
  const esgotoD    = get("domicilios_conexao_a_rede_de_esgoto");
  const lixoD      = get("domicilios_coleta_de_lixo");
  const internetD  = get("domicilios_uso_de_internet_dentro_do_domicilio");
  // New indicators for sub-tabs
  const alfTabD    = get("educacao_taxa_de_alfabetizacao_por_sexo_e_cor_ou_raca");
  const carteiraD  = get("trabalho_e_renda_carteira_assinada");
  const cnpjD      = get("trabalho_e_renda_cnpj");
  const indPopD    = get("indigenas_populacao_indigena");
  const indEtnD    = get("indigenas_numero_de_etnias_indigenas");
  const indLngD    = get("indigenas_numero_de_linguas_indigenas");
  const indVarD    = get("indigenas_variacao_populacional_dos_indigenas");
  const indUrbD    = get("indigenas_populacao_indigena_urbana_e_rural");
  const domTipoD   = get("domicilios_tipos_de_domicilio");
  const domMatD    = get("domicilios_tipo_de_material_das_paredes_externas");
  const domPermD   = get("domicilios_domicilios_particulares_permanentes");
  const domLavD    = get("domicilios_presenca_de_maquina_de_lavar_roupas");
  const domQtdD    = get("domicilios_quantidade_de_domicilios");

  // cod_ibge → uf lookup from municipios.json
  const codToUf = {};
  (munJson.municipios || []).forEach((m) => { codToUf[m.cod_ibge] = m.uf; });

  return Object.entries(densD).map(([munName, densRow]) => {
    const cod   = String(Math.round(densRow.cod_municipio || 0)).padStart(7, "0");
    const tsbio = densRow.territorio_nome || "?";
    const uf    = codToUf[cod] || "?";

    // ── Population (Censo series) ──
    const pr    = popResD[munName] || {};
    const pAnos = pr.ano_da_pesquisa || [];
    const pPops = pr.populacao || [];
    const i22   = pAnos.indexOf(2022);
    const i10   = pAnos.indexOf(2010);
    const p22   = i22 >= 0 ? (pPops[i22] || 0) : 0;
    const p10   = i10 >= 0 ? (pPops[i10] || 0) : 0;

    // ── Urban / Rural ──
    const sd    = situD[munName] || {};
    const sdPop = sd.populacao_pessoas || [0, 0];
    const totSit= (sdPop[0] || 0) + (sdPop[1] || 0);
    const urb   = totSit > 0 ? +((sdPop[0] / totSit) * 100).toFixed(1) : 0;
    const rur   = totSit > 0 ? +((sdPop[1] / totSit) * 100).toFixed(1) : 0;

    // ── Race / Color (2022) ──
    // order: [branca, preta, amarela, parda, indígena]
    const rc    = racaD[munName] || {};
    const rcPop = rc["2022_populacao_pessoas"] || [0, 0, 0, 0, 0];
    const totRc = rcPop.reduce((s, v) => s + (v || 0), 0);
    const pct   = (i) => totRc > 0 ? +((rcPop[i] || 0) / totRc * 100).toFixed(1) : 0;

    // ── Literacy ──
    const al    = alfabD[munName] || {};
    const alPop = al.populacao_pessoas || [0, 1];
    const totAl = (alPop[0] || 0) + (alPop[1] || 0);
    const alfab = totAl > 0 ? +((alPop[0] || 0) / totAl * 100).toFixed(1) : 0;

    // ── Sex ──
    const sx    = sexoD[munName] || {};
    const sxPop = sx.populacao_pessoas || [0, 0];
    const totSx = (sxPop[0] || 0) + (sxPop[1] || 0);
    const masc  = totSx > 0 ? +((sxPop[0] || 0) / totSx * 100).toFixed(1) : 50;
    const fem   = totSx > 0 ? +((sxPop[1] || 0) / totSx * 100).toFixed(1) : 50;

    // ── IDHM (2010) ──
    const ih    = idhmD[munName] || {};
    const ihAnos= ih.ano || [];
    const ihIdx = ihAnos.indexOf(2010);
    const getIh = (f) => ihIdx >= 0
      ? ((ih[f] || [])[ihIdx] || 0)
      : ((ih[f] || []).slice(-1)[0] || 0);

    // ── Gini (1991, 2000, 2010) ──
    const gi    = giniD[munName] || {};

    // ── Renda per capita ──
    const re    = rendaD[munName] || {};

    // ── PIB Agropecuária VAB (most recent year, R$ 1.000) ──
    const pb      = pibD[munName] || {};
    const pbAgros = pb["valor_adicionado_bruto_da_agropecuaria_a_precos_correntes_r_1_000"] || [];
    const pibAgro = pbAgros.length > 0 ? (pbAgros[pbAgros.length - 1] || 0) * 1000 : 0;

    // ── Employment position (Censo 2022) ──
    const oc     = posOcupD[munName] || {};
    const privOc = oc.setor_privado || 0;
    const pubOc  = (oc.setor_publico || 0) + (oc.empresa_estatal || 0) + (oc.militar || 0);
    const propOc = oc.conta_propria || 0;
    const domOc  = oc.trabalhador_domestico || 0;
    const empOc  = oc.empregador || 0;
    const totOc  = privOc + pubOc + propOc + domOc + empOc
                 + (oc.nao_remunerado_em_ajuda_a_morador_do_domicilio_ou_parente || 0);

    // ── Education level ──
    const ni    = nivelInstrD[munName] || {};
    const niSI  = ni.sem_instrucao_e_fundamental_incompleto || 0;
    const niFun = ni.fundamental_completo_e_medio_incompleto || 0;
    const niMed = ni.medio_completo_e_superior_incompleto || 0;
    const niSup = ni.superior_completo || 0;
    const totNi = niSI + niFun + niMed + niSup;

    // ── Quilombola / Indigenous ──
    const qi = quiIndD[munName] || {};

    // ── Literacy by sex and race (indices: 0=Branca,1=Preta,2=Amarela,3=Parda,4=Indígena,5=total) ──
    const at    = alfTabD[munName] || {};
    const atH   = at.homens_perc  || [];
    const atM   = at.mulheres_perc || [];
    const alfabH  = pn(atH.length > 5 ? atH[5] : atH[atH.length - 1]);
    const alfabM  = pn(atM.length > 5 ? atM[5] : atM[atM.length - 1]);
    const alfBranca = (pn(atH[0]) + pn(atM[0])) / 2;
    const alfPreta  = (pn(atH[1]) + pn(atM[1])) / 2;
    const alfParda  = (pn(atH[3]) + pn(atM[3])) / 2;
    const alfIndig  = (pn(atH[4]) + pn(atM[4])) / 2;

    // ── Carteira assinada (formal employment rate) ──
    const ca    = carteiraD[munName] || {};
    const caFor = (ca.com_carteira || []).reduce((s, v) => s + (v || 0), 0);
    const caInf = (ca.sem_carteira || []).reduce((s, v) => s + (v || 0), 0);
    const caTot = caFor + caInf;
    const comCartPct = caTot > 0 ? +(caFor / caTot * 100).toFixed(1) : 0;

    // ── CNPJ (self-employed formalization) ──
    const cn    = cnpjD[munName] || {};
    const cnFor = (cn.com_cnpj || []).reduce((s, v) => s + (v || 0), 0);
    const cnInf = (cn.sem_cnpj || []).reduce((s, v) => s + (v || 0), 0);
    const cnTot = cnFor + cnInf;
    const comCnpjPct = cnTot > 0 ? +(cnFor / cnTot * 100).toFixed(1) : 0;

    // ── Indigenous population indicators ──
    const ip        = indPopD[munName] || {};
    const pessoasInd = pn(ip.pessoas_indigenas);
    const ie        = indEtnD[munName] || {};
    const etniasInd  = pn(ie.etnia_s);
    const il        = indLngD[munName] || {};
    const linguasInd = pn(il.lingua_s);
    const iub       = indUrbD[munName] || {};
    const iubPop    = iub.populacao_indigena_pessoas || [0, 0];
    const indUrb    = iubPop[0] || 0;
    const indRur    = iubPop[1] || 0;
    const iv        = indVarD[munName] || {};
    const ivPop     = iv.populacao_indigena_pessoas || [0, 0];
    const ivAnos    = iv.ano || [];
    const iI10      = ivAnos.indexOf(2010);
    const iI22      = ivAnos.indexOf(2022);
    const ind2010   = iI10 >= 0 ? (ivPop[iI10] || 0) : (ivPop[0] || 0);
    const ind2022   = iI22 >= 0 ? (ivPop[iI22] || 0) : (ivPop[1] || 0);

    // ── Domicílios ──
    const dq       = domQtdD[munName] || {};
    const domTotal = pn(dq.domicilios);
    const dt       = domTipoD[munName] || {};
    const pctDT    = (f) => domTotal > 0 ? +(pn(dt[f]) / domTotal * 100).toFixed(1) : 0;
    const dm       = domMatD[munName] || {};
    const pctDM    = (f) => domTotal > 0 ? +(pn(dm[f]) / domTotal * 100).toFixed(1) : 0;
    const dp       = domPermD[munName] || {};
    const dpOc     = pn(dp.ocupados);
    const dpVg     = pn(dp.nao_ocupados_vagos);
    const dpOs     = pn(dp.nao_ocupados_de_uso_ocasional);
    const dpTot    = dpOc + dpVg + dpOs;
    const dl       = domLavD[munName] || {};

    return {
      c:       cod,
      n:       munName,
      u:       uf,
      t:       tsbio,
      p22,     p10,
      dens:    pn(densRow.habitantes_por_km2),
      urb,     rur,
      idMed:   pn((idMedD[munName] || {}).idade_mediana),
      masc,    fem,
      br:      pct(0), pr: pct(1), am: pct(2), pa: pct(3), in: pct(4),
      pUC:     qi.pessoas_indigenas || 0,
      pFav:    qi.pessoas_quilombolas || 0,
      idhm:    getIh("idhm"),
      idhm_e:  getIh("idhm_e"),
      idhm_l:  getIh("idhm_l"),
      idhm_r:  getIh("idhm_r"),
      gini91:  pn(gi["1991"]),
      gini00:  pn(gi["2000"]),
      gini10:  pn(gi["2010"]),
      rendaPC: pn(re.r),
      pibAgro, pibInd: 0, pibServ: 0, pibAdm: 0, pibPC: 0,
      semInst: totNi > 0 ? +(niSI  / totNi * 100).toFixed(1) : 0,
      fundComp:totNi > 0 ? +(niFun / totNi * 100).toFixed(1) : 0,
      medComp: totNi > 0 ? +(niMed / totNi * 100).toFixed(1) : 0,
      supComp: totNi > 0 ? +(niSup / totNi * 100).toFixed(1) : 0,
      alfab,   anoEst: 0,
      alfabH,  alfabM,
      alfBranca, alfPreta, alfParda, alfIndig,
      carteira:  totOc > 0 ? +(privOc / totOc * 100).toFixed(1) : 0,
      contaProp: totOc > 0 ? +(propOc / totOc * 100).toFixed(1) : 0,
      setorPub:  totOc > 0 ? +(pubOc  / totOc * 100).toFixed(1) : 0,
      setorPriv: totOc > 0 ? +(privOc / totOc * 100).toFixed(1) : 0,
      trabDomPct:totOc > 0 ? +(domOc  / totOc * 100).toFixed(1) : 0,
      empPct:    totOc > 0 ? +(empOc  / totOc * 100).toFixed(1) : 0,
      comCartPct, comCnpjPct,
      agua:    pn((aguaD[munName]    || {}).do_total_geral),
      esgoto:  pn((esgotoD[munName]  || {}).do_total_geral),
      lixo:    pn((lixoD[munName]    || {}).do_total_geral),
      internet:pn((internetD[munName]|| {}).do_total_geral),
      pessoasInd, etniasInd, linguasInd,
      indUrb, indRur, ind2010, ind2022,
      domTotal,
      domCasaPct:      pctDT("casa"),
      domVilaPct:      pctDT("casa_de_vila_ou_condominio"),
      domAptoPct:      pctDT("apartamento"),
      domCorticoPct:   pctDT("cortico"),
      domDegradadaPct: pctDT("estrutura_degradada_ou_inacabada"),
      domAlvRevPct:    pctDM("alvenaria_ou_taipa_c_revestimento"),
      domAlvSemPct:    pctDM("alvenaria_sem_revestimento"),
      domTaipaPct:     pctDM("taipa_sem_revestimento"),
      domMadNovaPct:   pctDM("madeira_para_construcao"),
      domMadReapPct:   pctDM("madeira_reaproveitada"),
      domOcupPct:      dpTot > 0 ? +(dpOc / dpTot * 100).toFixed(1) : 0,
      domVagoPct:      dpTot > 0 ? +(dpVg / dpTot * 100).toFixed(1) : 0,
      domOcasionalPct: dpTot > 0 ? +(dpOs / dpTot * 100).toFixed(1) : 0,
      domLavadora:     pn(dl.do_total_geral),
    };
  });
}

// ═══════════════════════════════════════════════════════
// DIMENSION & SUBTAB CONFIG
// ═══════════════════════════════════════════════════════
const DIMS = [
  {id:"socio", l:"Socioeconômica",       i:"👥", on:true},
  {id:"amb",   l:"Ambiental",            i:"🌿", on:true},
  {id:"prod",  l:"Produtiva",            i:"🌾", on:true},
  {id:"infra", l:"Infraestrutura",       i:"🏗️", on:true},
  {id:"pol",   l:"Políticas Públicas",   i:"📋", on:true},
  {id:"vuln",  l:"Vulnerabilidades",     i:"⚠️", on:true},
];

const SUBTABS = [
  {id:"demo",  l:"Perfil Demográfico",   i:"📊"},
  {id:"econ",  l:"Economia & Índices",   i:"💰"},
  {id:"edu",   l:"Educação",             i:"📚"},
  {id:"trab",  l:"Trabalho & Renda",     i:"👷"},
  {id:"povos", l:"Povos Tradicionais",   i:"🏕️"},
  {id:"dom",   l:"Domicílios",           i:"🏠"},
];

const SUBTABS_AMB = [
  {id:"desm", l:"Desmatamento & Degradação", i:"🛰️"},
  {id:"lulc", l:"Uso do Solo & Queimadas",   i:"🗺️"},
  {id:"fund", l:"Ordenamento Fundiário",      i:"🏛️"},
];

const SUBTABS_PROD = [
  {id:"agro", l:"Perfil Agropecuário",    i:"🌾"},
  {id:"pam",  l:"Produção Agrícola",       i:"📊"},
  {id:"pecu", l:"Pecuária & Extrativismo", i:"🐄"},
  {id:"coop", l:"Cooperativas",            i:"🤝"},
];

const PROD_COLORS = {
  "Acai":"#7B2D8E","Cacau":"#8B4513","Banana":"#D4A843","Mandioca":"#E8871E",
  "Pimenta-do-reino":"#C4342D","Soja":"#1B7A3D","Milho":"#F5C518","Arroz":"#2E86AB",
  "Feijao":"#8B7355","Borracha":"#4A4A4A","Castanha-do-brasil":"#6B4226",
  "Acai (fruto)":"#7B2D8E","Borracha (latex)":"#4A4A4A",
};
const REBANHO_COLORS = {
  "Bovino":"#8B4513","Bubalino":"#A0522D","Suino":"#E8871E","Aves":"#D4A843",
  "Equino":"#6B4226","Caprino":"#C4342D","Ovino":"#7B2D8E",
};

const SUBTABS_INFRA = [
  {id:"san",   l:"Saneamento Básico",         i:"💧"},
  {id:"entor", l:"Entorno dos Domicílios",    i:"🏘️"},
  {id:"trans", l:"Transporte & Conectividade",i:"🚌"},
];
const INFRA_COLORS = {
  agua:"#1565C0", esgoto:"#8D6E63", lixo:"#FF8F00",
  transporte:"#546E7A", digital:"#7B1FA2",
};

const SUBTABS_POL = [
  {id:"prog",  l:"Programas Sociais",      i:"💰"},
  {id:"socio", l:"Sociobioeco & Crédito",  i:"🌿"},
  {id:"gov",   l:"Governança Municipal",   i:"🏛️"},
  {id:"povos", l:"Povos Tradicionais",     i:"🪶"},
];
const POL_COLORS = {
  prog:"#1565C0", sociobio:"#1B7A3D", gov:"#546E7A",
  quilombola:"#7B1FA2", indigena:"#E8871E",
};

const SUBTABS_VUL = [
  {id:"over",    l:"Visão Geral",           i:"🗺️"},
  {id:"saude",   l:"Saúde",                 i:"🏥"},
  {id:"alim",    l:"Segurança Alimentar",   i:"🍽️"},
  {id:"desast",  l:"Desastres",             i:"🌊"},
  {id:"hidrico", l:"Recursos Hídricos",     i:"💧"},
  {id:"biodiv",  l:"Biodiversidade",         i:"🦜"},
  {id:"energ",   l:"Seg. Energética",       i:"⚡"},
];
const VUL_COLORS = {
  saude:"#C4342D", alimentar:"#E8871E", desastres:"#2E86AB",
  hidrico:"#1565C0", biodiversidade:"#1B7A3D", energia:"#7B2D8E",
  muitobaixo:"#1B7A3D", baixo:"#7BC67E", medio:"#D4A843",
  alto:"#FF8A65", muitoalto:"#D32F2F",
};

const DETER_COLORS_MAP = {
  "CICATRIZ_DE_QUEIMADA":"#FF8C00","CORTE_SELETIVO":"#8B4513",
  "CS_DESORDENADO":"#FF6347","CS_GEOMETRICO":"#FF4500",
  "DEGRADACAO":"#FFA500","DESMATAMENTO_CR":"#C4342D",
  "DESMATAMENTO_VEG":"#9B2226","MINERACAO":"#7B2D8E",
};
const DETER_SHORT = {
  "CICATRIZ_DE_QUEIMADA":"Queimada","CORTE_SELETIVO":"Corte Seletivo",
  "CS_DESORDENADO":"CS Desordenado","CS_GEOMETRICO":"CS Geométrico",
  "DEGRADACAO":"Degradação","DESMATAMENTO_CR":"Desmatamento CR",
  "DESMATAMENTO_VEG":"Desm. Veg.","MINERACAO":"Mineração",
};
const LULC_N1_COLORS = {
  "Floresta":"#1B7A3D","Agropecuária":"#E8871E",
  "Formação Natural não Florestal":"#8cb4d8",
  "Área não vegetada":"#C4342D","Corpos D'água":"#2E86AB",
};

// ═══════════════════════════════════════════════════════
// REUSABLE COMPONENTS
// ═══════════════════════════════════════════════════════
const FB = ({f}) => <span style={{display:"inline-flex",alignItems:"center",gap:4,padding:"2px 8px",borderRadius:16,background:"rgba(27,122,61,0.08)",color:T.green,fontSize:10,fontWeight:600,border:"1px solid rgba(27,122,61,0.15)"}}>📊 {f}</span>;

const CC = ({title,fonte,periodo,children,h=260}) => (
  <div style={{background:"#fff",borderRadius:12,padding:"16px 18px 12px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,display:"flex",flexDirection:"column",gap:8}}>
    <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",flexWrap:"wrap",gap:6}}>
      <h3 style={{margin:0,fontSize:13.5,fontWeight:700,color:T.text,lineHeight:1.3}}>{title}</h3>
      <FB f={fonte}/>
    </div>
    <div style={{height:h,minHeight:0}}>{children}</div>
    {periodo&&<div style={{fontSize:10,color:T.textLight,borderTop:"1px solid #f0f2f0",paddingTop:6}}>Período: {periodo}</div>}
  </div>
);

const KPI = ({l,v,u,i,c}) => (
  <div style={{background:"#fff",borderRadius:12,padding:"14px 16px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,display:"flex",alignItems:"center",gap:12}}>
    <div style={{width:42,height:42,borderRadius:10,background:`${c}15`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:20,flexShrink:0}}>{i}</div>
    <div>
      <div style={{fontSize:20,fontWeight:800,color:c||T.text,lineHeight:1.1}}>{v}<span style={{fontSize:12,fontWeight:500,marginLeft:2}}>{u}</span></div>
      <div style={{fontSize:11,color:T.textLight,marginTop:1,fontWeight:500}}>{l}</div>
    </div>
  </div>
);

const CTip = ({active,payload,label})=>{
  if(!active||!payload?.length)return null;
  return <div style={{background:T.greenDark,color:"#fff",padding:"8px 12px",borderRadius:8,fontSize:11,boxShadow:"0 4px 12px rgba(0,0,0,0.2)",maxWidth:280}}>
    <div style={{fontWeight:700,marginBottom:3}}>{label}</div>
    {payload.map((p,i)=><div key={i} style={{display:"flex",alignItems:"center",gap:5,marginTop:1}}>
      <span style={{width:7,height:7,borderRadius:7,background:p.color,flexShrink:0}}/><span>{p.name}: <b>{typeof p.value==="number"?p.value.toLocaleString("pt-BR"):p.value}</b></span>
    </div>)}
  </div>;
};

const MultiSel = ({label,opts,sel,onChange,cmap})=>{
  const[open,setOpen]=useState(false);const ref=useRef(null);
  useEffect(()=>{const h=e=>{if(ref.current&&!ref.current.contains(e.target))setOpen(false)};document.addEventListener("mousedown",h);return()=>document.removeEventListener("mousedown",h)},[]);
  const all=sel.length===opts.length;
  const tog=v=>onChange(sel.includes(v)?sel.filter(x=>x!==v):[...sel,v]);
  const txt=all?`Todos (${opts.length})`:sel.length===0?"Nenhum":sel.length<=2?sel.join(", "):`${sel.length} selecionados`;
  return <div ref={ref} style={{position:"relative",minWidth:190}}>
    <label style={{fontSize:10,fontWeight:700,color:"#5a6a5a",textTransform:"uppercase",letterSpacing:1,marginBottom:3,display:"block"}}>{label}</label>
    <button onClick={()=>setOpen(!open)} style={{width:"100%",padding:"7px 12px",borderRadius:8,border:"1.5px solid #c8d0c8",background:"#fff",cursor:"pointer",textAlign:"left",fontSize:12,fontWeight:600,color:T.text,display:"flex",justifyContent:"space-between",alignItems:"center",fontFamily:"inherit"}}>
      <span style={{overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{txt}</span>
      <span style={{transform:open?"rotate(180deg)":"rotate(0)",transition:"transform 0.2s",fontSize:9,marginLeft:6}}>▼</span>
    </button>
    {open&&<div style={{position:"absolute",top:"100%",left:0,right:0,background:"#fff",borderRadius:8,border:"1.5px solid #c8d0c8",marginTop:3,zIndex:100,maxHeight:260,overflowY:"auto",boxShadow:"0 8px 20px rgba(0,0,0,0.12)"}}>
      <div onClick={()=>onChange(all?[]:opts.map(o=>o.value))} style={{padding:"7px 12px",cursor:"pointer",fontSize:11,fontWeight:700,borderBottom:"1px solid #f0f2f0",color:T.green,display:"flex",alignItems:"center",gap:6}}>
        <span style={{width:14,height:14,borderRadius:3,border:`2px solid ${all?T.green:"#c8d0c8"}`,background:all?T.green:"#fff",display:"inline-flex",alignItems:"center",justifyContent:"center",color:"#fff",fontSize:9}}>{all?"✓":""}</span>Selecionar todos
      </div>
      {opts.map(o=>{const ch=sel.includes(o.value);const dc=cmap?.[o.value];return <div key={o.value} onClick={()=>tog(o.value)} style={{padding:"6px 12px",cursor:"pointer",fontSize:11,fontWeight:500,display:"flex",alignItems:"center",gap:6,background:ch?"rgba(27,122,61,0.03)":"transparent"}}>
        <span style={{width:14,height:14,borderRadius:3,border:`2px solid ${ch?T.green:"#c8d0c8"}`,background:ch?T.green:"#fff",display:"inline-flex",alignItems:"center",justifyContent:"center",color:"#fff",fontSize:9,flexShrink:0}}>{ch?"✓":""}</span>
        {dc&&<span style={{width:7,height:7,borderRadius:7,background:dc,flexShrink:0}}/>}{o.label}
      </div>})}
    </div>}
  </div>;
};

// ═══════════════════════════════════════════════════════
// MAIN DASHBOARD
// ═══════════════════════════════════════════════════════
export default function Dashboard(){
  const[dim,setDim]=useState("socio");
  const[sub,setSub]=useState("demo");
  const[selT,setSelT]=useState(TSBIO_LIST);
  const[selM,setSelM]=useState([]);

  // ── Data loading ──
  const[munData,setMunData]=useState([]);
  const[loading,setLoading]=useState(true);
  const[loadErr,setLoadErr]=useState(null);
  const[ambData,setAmbData]=useState(null);
  const[prodData,setProdData]=useState(null);
  const[infraData,setInfraData]=useState(null);
  const[polData,setPolData]=useState(null);
  const[vulData,setVulData]=useState(null);

  useEffect(()=>{
    Promise.all([
      fetch("/data/municipios.json").then(r=>r.json()),
      fetch("/data/dim_socioeconomica.json").then(r=>r.json()),
      fetch("/data/dim_ambiental_v2.json").then(r=>r.json()),
      fetch("/data/dim_produtiva_v2.json").then(r=>r.json()),
      fetch("/data/dim_infraestrutura_v2.json").then(r=>r.json()),
      fetch("/data/dim_politicas_v2.json").then(r=>r.json()),
      fetch("/data/dim_vulnerabilidades_v2.json").then(r=>r.json()),
    ])
    .then(([munJson,socioJson,ambJson,prodJson,infraJson,polJson,vulJson])=>{
      setMunData(buildMunicipios(munJson,socioJson));
      setAmbData(ambJson);
      setProdData(prodJson);
      setInfraData(infraJson);
      setPolData(polJson);
      setVulData(vulJson);
      setLoading(false);
    })
    .catch(err=>{
      setLoadErr(err.message);
      setLoading(false);
    });
  },[]);

  const fM=useMemo(()=>munData.filter(m=>selT.includes(m.t)),[selT,munData]);
  const mOpts=useMemo(()=>fM.map(m=>({value:m.c,label:`${m.n} (${m.u})`})),[fM]);
  useEffect(()=>setSelM([]),[selT]);
  const D=useMemo(()=>selM.length>0?fM.filter(m=>selM.includes(m.c)):fM,[fM,selM]);

  // Aggregate by TSBio
  const byT=useMemo(()=>{
    const g={};D.forEach(m=>{if(!g[m.t])g[m.t]=[];g[m.t].push(m)});
    return Object.entries(g).map(([t,ms])=>{
      const n=ms.length;
      const sum=(k)=>ms.reduce((s,m)=>s+(m[k]||0),0);
      const avg=(k)=>n>0?+(sum(k)/n).toFixed(2):0;
      return{t,n,
        pop:sum("p22"),pop10:sum("p10"),dens:avg("dens"),urb:avg("urb"),rur:avg("rur"),
        idhm:avg("idhm"),idhm_e:avg("idhm_e"),idhm_l:avg("idhm_l"),idhm_r:avg("idhm_r"),
        gini10:avg("gini10"),rendaPC:Math.round(avg("rendaPC")),alfab:avg("alfab"),anoEst:avg("anoEst"),
        pibAgro:sum("pibAgro"),pibInd:sum("pibInd"),pibServ:sum("pibServ"),pibAdm:sum("pibAdm"),
        semInst:avg("semInst"),fundComp:avg("fundComp"),medComp:avg("medComp"),supComp:avg("supComp"),
        alfabH:avg("alfabH"),alfabM:avg("alfabM"),
        alfBranca:avg("alfBranca"),alfPreta:avg("alfPreta"),alfParda:avg("alfParda"),alfIndig:avg("alfIndig"),
        carteira:avg("carteira"),contaProp:avg("contaProp"),setorPub:avg("setorPub"),setorPriv:avg("setorPriv"),
        trabDomPct:avg("trabDomPct"),empPct:avg("empPct"),
        comCartPct:avg("comCartPct"),comCnpjPct:avg("comCnpjPct"),
        agua:avg("agua"),esgoto:avg("esgoto"),lixo:avg("lixo"),internet:avg("internet"),
        domLavadora:avg("domLavadora"),
        domCasaPct:avg("domCasaPct"),domVilaPct:avg("domVilaPct"),domAptoPct:avg("domAptoPct"),
        domCorticoPct:avg("domCorticoPct"),domDegradadaPct:avg("domDegradadaPct"),
        domAlvRevPct:avg("domAlvRevPct"),domAlvSemPct:avg("domAlvSemPct"),domTaipaPct:avg("domTaipaPct"),
        domMadNovaPct:avg("domMadNovaPct"),domMadReapPct:avg("domMadReapPct"),
        domOcupPct:avg("domOcupPct"),domVagoPct:avg("domVagoPct"),domOcasionalPct:avg("domOcasionalPct"),
        pessoasInd:sum("pessoasInd"),etniasInd:sum("etniasInd"),linguasInd:sum("linguasInd"),
        indUrb:sum("indUrb"),indRur:sum("indRur"),
        ind2010:sum("ind2010"),ind2022:sum("ind2022"),
        pUC:sum("pUC"),pFav:sum("pFav"),
        in_avg:avg("in"),qi_avg:avg("pUC"),
      };
    });
  },[D]);

  const totPop=D.reduce((s,m)=>s+(m.p22||0),0);
  const avgIDHM=D.length?+(D.reduce((s,m)=>s+(m.idhm||0),0)/D.length).toFixed(3):0;
  const avgGini=D.length?+(D.reduce((s,m)=>s+(m.gini10||0),0)/D.length).toFixed(2):0;
  const avgRenda=D.length?Math.round(D.reduce((s,m)=>s+(m.rendaPC||0),0)/D.length):0;
  const avgAlfab=D.length?+(D.reduce((s,m)=>s+(m.alfab||0),0)/D.length).toFixed(1):0;
  const sh=(s)=>s.length>13?s.slice(0,12)+"…":s;

  // ─── PERFIL DEMOGRÁFICO ───
  const renderDemo=()=>{
    const popBar=byT.map(t=>({name:sh(t.t),full:t.t,Pop2022:t.pop,Pop2010:t.pop10})).sort((a,b)=>b.Pop2022-a.Pop2022);
    const cresc=byT.map(t=>({name:sh(t.t),full:t.t,"Cresc. (%)":t.pop10>0?+((t.pop-t.pop10)/t.pop10*100).toFixed(1):0}));
    const urbRur=byT.map(t=>({name:sh(t.t),full:t.t,"Urbana":t.urb,"Rural":t.rur}));
    const top12=[...D].sort((a,b)=>(b.p22||0)-(a.p22||0)).slice(0,12).map(m=>({name:sh(m.n),full:m.n,Pop:m.p22,fill:TSBIO_COLORS[m.t]}));
    const raca=byT.map(t=>{const ms=D.filter(m=>m.t===t.t);const n=ms.length||1;return{name:sh(t.t),Branca:+(ms.reduce((s,m)=>s+(m.br||0),0)/n).toFixed(1),Preta:+(ms.reduce((s,m)=>s+(m.pr||0),0)/n).toFixed(1),Parda:+(ms.reduce((s,m)=>s+(m.pa||0),0)/n).toFixed(1),Indígena:+(ms.reduce((s,m)=>s+(m.in||0),0)/n).toFixed(1)}});
    const densRank=[...D].sort((a,b)=>(b.dens||0)-(a.dens||0)).slice(0,10).map(m=>({name:sh(m.n),full:m.n,"hab/km²":m.dens,fill:TSBIO_COLORS[m.t]}));
    const popTotal10=D.reduce((s,m)=>s+(m.p10||0),0);const crescTotal=popTotal10>0?((totPop-popTotal10)/popTotal10*100).toFixed(1):"—";

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="População Total (2022)" v={totPop.toLocaleString("pt-BR")} i="👥" c={T.green}/>
        <KPI l="Crescimento 2010→2022" v={crescTotal!=="—"?`+${crescTotal}`:crescTotal} u="%" i="📈" c={T.blue}/>
        <KPI l="Taxa Alfabetização" v={avgAlfab} u="%" i="📚" c={T.purple}/>
        <KPI l="Nº Municípios" v={D.length} i="🏛️" c={T.orange}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="População por TSBio — 2010 vs 2022" fonte="IBGE Censo 2022" periodo="2010, 2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={popBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Pop2010" fill="#b0c4b0" radius={[4,4,0,0]} name="2010"/>
              <Bar dataKey="Pop2022" radius={[4,4,0,0]} name="2022">{popBar.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Crescimento Intercensitário (%)" fonte="IBGE Censo 2022" periodo="2010→2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={cresc} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Cresc. (%)" radius={[4,4,0,0]}>{cresc.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Situação do Domicílio — Urbano vs Rural (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={urbRur} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis type="number" domain={[0,100]} tick={{fontSize:10}}/><YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Urbana" stackId="a" fill={T.blue} radius={[0,0,0,0]}/>
              <Bar dataKey="Rural" stackId="a" fill={T.green} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Composição por Cor/Raça (% média)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={raca} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Parda" stackId="a" fill="#D4A843"/>
              <Bar dataKey="Branca" stackId="a" fill="#8cb4d8"/>
              <Bar dataKey="Preta" stackId="a" fill="#6a4c93"/>
              <Bar dataKey="Indígena" stackId="a" fill={T.red}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Top 12 Municípios por População" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={top12} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis type="number" tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/><YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Pop" radius={[0,4,4,0]}>{top12.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Top 10 Densidade Demográfica (hab/km²)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={densRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis type="number" tick={{fontSize:10}}/><YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="hab/km²" radius={[0,4,4,0]}>{densRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
      </div>
    </div>;
  };

  // ─── ECONOMIA & ÍNDICES ───
  const renderEcon=()=>{
    const agroBar=byT.map(t=>({name:sh(t.t),full:t.t,"Agro VAB (R$ mil)":+(t.pibAgro/1000).toFixed(0)})).sort((a,b)=>b["Agro VAB (R$ mil)"]-a["Agro VAB (R$ mil)"]);
    const radarData=byT.map(t=>({t:t.t,name:sh(t.t),"Educação":+(t.idhm_e*100).toFixed(0),"Longevidade":+(t.idhm_l*100).toFixed(0),"Renda":+(t.idhm_r*100).toFixed(0)}));
    const idhmBar=byT.map(t=>({name:sh(t.t),full:t.t,IDHM:t.idhm,"IDHM-E":t.idhm_e,"IDHM-L":t.idhm_l,"IDHM-R":t.idhm_r}));
    const rendaRank=[...D].sort((a,b)=>(b.rendaPC||0)-(a.rendaPC||0)).slice(0,12).map(m=>({name:sh(m.n),full:m.n,"R$ per capita":m.rendaPC,fill:TSBIO_COLORS[m.t]}));
    const giniData=byT.map(t=>{const ms=D.filter(m=>m.t===t.t);const n=ms.length||1;return{name:sh(t.t),full:t.t,"1991":+(ms.reduce((s,m)=>s+(m.gini91||0),0)/n).toFixed(2),"2000":+(ms.reduce((s,m)=>s+(m.gini00||0),0)/n).toFixed(2),"2010":+(ms.reduce((s,m)=>s+(m.gini10||0),0)/n).toFixed(2)}});
    const rendaTsbio=byT.map(t=>{const ms=D.filter(m=>m.t===t.t);return{name:sh(t.t),full:t.t,"Renda per capita (R$)":Math.round(ms.reduce((s,m)=>s+(m.rendaPC||0),0)/(ms.length||1))}});
    const totalAgro=D.reduce((s,m)=>s+(m.pibAgro||0),0);

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="VAB Agropecuário Total (R$ mil)" v={(totalAgro/1000).toLocaleString("pt-BR",{maximumFractionDigits:0})} i="🌾" c={T.green}/>
        <KPI l="IDHM Médio (2010)" v={avgIDHM} i="📈" c={T.blue}/>
        <KPI l="Gini Médio (2010)" v={avgGini} i="⚖️" c={T.orange}/>
        <KPI l="Renda PC Média (R$)" v={`R$${avgRenda.toLocaleString("pt-BR")}`} i="💵" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Valor Adicionado Bruto — Agropecuária por TSBio (R$ mil)" fonte="IBGE PIB Municipal" periodo="Mais recente disponível">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={agroBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tickFormatter={v=>`${(v/1000).toFixed(0)}M`} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Agro VAB (R$ mil)" radius={[4,4,0,0]}>{agroBar.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="IDHM Desagregado por Dimensão" fonte="Atlas do Desenvolvimento Humano" periodo="2010">
          <ResponsiveContainer width="100%" height={250}>
            <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="70%">
              <PolarGrid stroke="#ddd"/><PolarAngleAxis dataKey="name" tick={{fontSize:10}}/>
              <PolarRadiusAxis angle={90} domain={[20,90]} tick={{fontSize:9}}/>
              <Radar name="Educação" dataKey="Educação" stroke={T.blue} fill={T.blue} fillOpacity={0.15} strokeWidth={2}/>
              <Radar name="Longevidade" dataKey="Longevidade" stroke={T.green} fill={T.green} fillOpacity={0.15} strokeWidth={2}/>
              <Radar name="Renda" dataKey="Renda" stroke={T.orange} fill={T.orange} fillOpacity={0.15} strokeWidth={2}/>
              <Legend wrapperStyle={{fontSize:10}}/>
              <Tooltip/>
            </RadarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="IDHM e Componentes por TSBio" fonte="Atlas do Desenvolvimento Humano" periodo="2010">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={idhmBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0.2,0.85]} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="IDHM" fill={T.text} radius={[4,4,0,0]}/>
              <Bar dataKey="IDHM-E" fill={T.blue} radius={[4,4,0,0]}/>
              <Bar dataKey="IDHM-L" fill={T.green} radius={[4,4,0,0]}/>
              <Bar dataKey="IDHM-R" fill={T.orange} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Evolução do Índice de Gini" fonte="Atlas DH Censo" periodo="1991, 2000, 2010">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={giniData} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0.4,0.7]} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="1991" fill="#b0c4b0" radius={[4,4,0,0]}/>
              <Bar dataKey="2000" fill={T.orange} radius={[4,4,0,0]}/>
              <Bar dataKey="2010" fill={T.red} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Rendimento Domiciliar Per Capita — Top 12" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={rendaRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis type="number" tickFormatter={v=>`R$${v}`} tick={{fontSize:10}}/><YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="R$ per capita" radius={[0,4,4,0]}>{rendaRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Rendimento Domiciliar Médio por TSBio (R$)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={rendaTsbio} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/><XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tickFormatter={v=>`R$${v}`} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Renda per capita (R$)" radius={[4,4,0,0]}>{rendaTsbio.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
      </div>
    </div>;
  };

  // ─── EDUCAÇÃO ───
  const renderEdu=()=>{
    const avgAlfabH = D.length ? +(D.reduce((s,m)=>s+(m.alfabH||0),0)/D.length).toFixed(1) : 0;
    const avgAlfabM = D.length ? +(D.reduce((s,m)=>s+(m.alfabM||0),0)/D.length).toFixed(1) : 0;
    const avgSupComp = D.length ? +(D.reduce((s,m)=>s+(m.supComp||0),0)/D.length).toFixed(1) : 0;

    // Stacked education level by TSBio
    const eduLevel = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Sem instrução":+t.semInst.toFixed(1),
      "Fundamental":+t.fundComp.toFixed(1),
      "Médio":+t.medComp.toFixed(1),
      "Superior":+t.supComp.toFixed(1),
    }));

    // Literacy by sex per TSBio
    const alfabSex = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Homens":+t.alfabH.toFixed(1),
      "Mulheres":+t.alfabM.toFixed(1),
    }));

    // Literacy by race per TSBio (avg men+women)
    const alfabRaca = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Branca":+t.alfBranca.toFixed(1),
      "Parda":+t.alfParda.toFixed(1),
      "Preta":+t.alfPreta.toFixed(1),
      "Indígena":+t.alfIndig.toFixed(1),
    }));

    // Top 12 % superior completo
    const supRank = [...D].sort((a,b)=>(b.supComp||0)-(a.supComp||0)).slice(0,12)
      .map(m=>({name:sh(m.n),full:m.n,"Superior (%)":m.supComp,fill:TSBIO_COLORS[m.t]}));

    // Bottom 12 % sem instrução
    const semInstRank = [...D].sort((a,b)=>(b.semInst||0)-(a.semInst||0)).slice(0,12)
      .map(m=>({name:sh(m.n),full:m.n,"Sem Instrução (%)":m.semInst,fill:TSBIO_COLORS[m.t]}));

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Taxa Alfabetização Média" v={avgAlfab} u="%" i="📚" c={T.green}/>
        <KPI l="Alfabetizados — Homens" v={avgAlfabH} u="%" i="👨" c={T.blue}/>
        <KPI l="Alfabetizadas — Mulheres" v={avgAlfabM} u="%" i="👩" c={T.purple}/>
        <KPI l="Com Superior Completo" v={avgSupComp} u="%" i="🎓" c={T.orange}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Nível de Instrução por TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={eduLevel} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Sem instrução" stackId="a" fill={T.red}/>
              <Bar dataKey="Fundamental" stackId="a" fill={T.orange}/>
              <Bar dataKey="Médio" stackId="a" fill={T.blue}/>
              <Bar dataKey="Superior" stackId="a" fill={T.green} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Taxa de Alfabetização por Sexo e TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={alfabSex} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis domain={[60,100]} tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Homens" fill={T.blue} radius={[4,4,0,0]}/>
              <Bar dataKey="Mulheres" fill={T.purple} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Alfabetização por Cor/Raça e TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={alfabRaca} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis domain={[50,100]} tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Branca" fill="#8cb4d8" radius={[4,4,0,0]}/>
              <Bar dataKey="Parda" fill={T.gold} radius={[4,4,0,0]}/>
              <Bar dataKey="Preta" fill="#6a4c93" radius={[4,4,0,0]}/>
              <Bar dataKey="Indígena" fill={T.red} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Nível de Instrução por TSBio — Horizontal (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={eduLevel} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Sem instrução" stackId="a" fill={T.red}/>
              <Bar dataKey="Fundamental" stackId="a" fill={T.orange}/>
              <Bar dataKey="Médio" stackId="a" fill={T.blue}/>
              <Bar dataKey="Superior" stackId="a" fill={T.green} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Top 12 — Maior % com Superior Completo" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={supRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Superior (%)" radius={[0,4,4,0]}>{supRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Top 12 — Maior % Sem Instrução / Fund. Incompleto" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={semInstRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Sem Instrução (%)" radius={[0,4,4,0]}>{semInstRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
      </div>
    </div>;
  };

  // ─── TRABALHO & RENDA ───
  const renderTrab=()=>{
    const avgCarteira = D.length ? +(D.reduce((s,m)=>s+(m.comCartPct||0),0)/D.length).toFixed(1) : 0;
    const avgConta    = D.length ? +(D.reduce((s,m)=>s+(m.contaProp||0),0)/D.length).toFixed(1) : 0;
    const avgCnpj     = D.length ? +(D.reduce((s,m)=>s+(m.comCnpjPct||0),0)/D.length).toFixed(1) : 0;

    // Occupation positions stacked by TSBio
    const ocupStack = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Setor Privado":+t.setorPriv.toFixed(1),
      "Setor Público":+t.setorPub.toFixed(1),
      "Conta Própria":+t.contaProp.toFixed(1),
      "Trab. Doméstico":+t.trabDomPct.toFixed(1),
      "Empregador":+t.empPct.toFixed(1),
    }));

    // Carteira assinada (formal employment) by TSBio
    const carteiraBar = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Com Carteira (%)":+t.comCartPct.toFixed(1),
      "Sem Carteira (%)":+(100-t.comCartPct).toFixed(1),
    }));

    // CNPJ by TSBio
    const cnpjBar = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Com CNPJ (%)":+t.comCnpjPct.toFixed(1),
      "Sem CNPJ (%)":+(100-t.comCnpjPct).toFixed(1),
    }));

    // Renda per capita ranking
    const rendaRank = [...D].sort((a,b)=>(b.rendaPC||0)-(a.rendaPC||0)).slice(0,12)
      .map(m=>({name:sh(m.n),full:m.n,"R$ per capita":m.rendaPC,fill:TSBIO_COLORS[m.t]}));

    // Top 12 conta própria
    const contaRank = [...D].sort((a,b)=>(b.contaProp||0)-(a.contaProp||0)).slice(0,12)
      .map(m=>({name:sh(m.n),full:m.n,"Conta Própria (%)":m.contaProp,fill:TSBIO_COLORS[m.t]}));

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Renda per Capita Média" v={`R$${avgRenda.toLocaleString("pt-BR")}`} i="💵" c={T.green}/>
        <KPI l="Com Carteira Assinada" v={avgCarteira} u="%" i="📝" c={T.blue}/>
        <KPI l="Conta Própria" v={avgConta} u="%" i="💼" c={T.orange}/>
        <KPI l="Com CNPJ" v={avgCnpj} u="%" i="🏢" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Posição na Ocupação por TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={ocupStack} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Setor Privado" stackId="a" fill={T.blue}/>
              <Bar dataKey="Setor Público" stackId="a" fill={T.green}/>
              <Bar dataKey="Conta Própria" stackId="a" fill={T.orange}/>
              <Bar dataKey="Trab. Doméstico" stackId="a" fill={T.purple}/>
              <Bar dataKey="Empregador" stackId="a" fill={T.gold} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Posição na Ocupação por TSBio — Horizontal (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={ocupStack} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Setor Privado" stackId="a" fill={T.blue}/>
              <Bar dataKey="Setor Público" stackId="a" fill={T.green}/>
              <Bar dataKey="Conta Própria" stackId="a" fill={T.orange}/>
              <Bar dataKey="Trab. Doméstico" stackId="a" fill={T.purple}/>
              <Bar dataKey="Empregador" stackId="a" fill={T.gold} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Carteira Assinada — Formal vs Informal por TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={carteiraBar} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Com Carteira (%)" stackId="a" fill={T.green}/>
              <Bar dataKey="Sem Carteira (%)" stackId="a" fill={T.red} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="CNPJ — Formalização de Autônomos por TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={cnpjBar} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Com CNPJ (%)" stackId="a" fill={T.blue}/>
              <Bar dataKey="Sem CNPJ (%)" stackId="a" fill="#bbb" radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Rendimento Domiciliar Per Capita — Top 12" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={rendaRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>`R$${v}`} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="R$ per capita" radius={[0,4,4,0]}>{rendaRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Top 12 — % Trabalhadores por Conta Própria" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={contaRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Conta Própria (%)" radius={[0,4,4,0]}>{contaRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
      </div>
    </div>;
  };

  // ─── POVOS TRADICIONAIS ───
  const renderPovos=()=>{
    const totalInd  = D.reduce((s,m)=>s+(m.pessoasInd||0),0);
    const totalQui  = D.reduce((s,m)=>s+(m.pFav||0),0);
    const totalEtn  = byT.reduce((s,t)=>s+(t.etniasInd||0),0);
    const totalLng  = byT.reduce((s,t)=>s+(t.linguasInd||0),0);

    // Indigenous population by TSBio
    const indPop = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Pop. Indígena":t.pessoasInd,
    })).sort((a,b)=>b["Pop. Indígena"]-a["Pop. Indígena"]);

    // Ethnic + linguistic diversity by TSBio
    const diversidade = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Etnias":t.etniasInd,
      "Línguas":t.linguasInd,
    }));

    // Urban vs rural indigenous by TSBio
    const indUrbRur = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Urbana":t.indUrb,
      "Rural":t.indRur,
    }));

    // Population change 2010→2022 indigenous by TSBio
    const indVar = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "2010":t.ind2010,
      "2022":t.ind2022,
    }));

    // Top 10 municipalities by indigenous population
    const indMunRank = [...D].filter(m=>m.pessoasInd>0)
      .sort((a,b)=>(b.pessoasInd||0)-(a.pessoasInd||0)).slice(0,10)
      .map(m=>({name:sh(m.n),full:m.n,"Pop. Indígena":m.pessoasInd,fill:TSBIO_COLORS[m.t]}));

    // Quilombola population by TSBio
    const quiPop = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Pop. Quilombola":t.pFav,
    })).filter(t=>t["Pop. Quilombola"]>0);

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Pop. Indígena Total" v={totalInd.toLocaleString("pt-BR")} i="🏕️" c={T.orange}/>
        <KPI l="Etnias Identificadas" v={totalEtn} i="🌿" c={T.green}/>
        <KPI l="Línguas Indígenas" v={totalLng} i="🗣️" c={T.blue}/>
        <KPI l="Pop. Quilombola" v={totalQui.toLocaleString("pt-BR")} i="🏘️" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="População Indígena por TSBio" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={indPop} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Pop. Indígena" radius={[4,4,0,0]}>{indPop.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.orange}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Diversidade Etno-Linguística por TSBio" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={diversidade} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Etnias" fill={T.green} radius={[4,4,0,0]}/>
              <Bar dataKey="Línguas" fill={T.blue} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Pop. Indígena Urbana vs Rural por TSBio" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={indUrbRur} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Urbana" stackId="a" fill={T.blue}/>
              <Bar dataKey="Rural" stackId="a" fill={T.green} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Variação Pop. Indígena 2010 → 2022 por TSBio" fonte="IBGE Censo 2022" periodo="2010, 2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={indVar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="2010" fill="#b0c4b0" radius={[4,4,0,0]}/>
              <Bar dataKey="2022" fill={T.orange} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Top 10 Municípios — Pop. Indígena" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={indMunRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Pop. Indígena" radius={[0,4,4,0]}>{indMunRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        {quiPop.length > 0 && (
          <CC title="Pop. Quilombola por TSBio" fonte="IBGE Censo 2022" periodo="2022">
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={quiPop} margin={{left:8,right:16,top:8,bottom:8}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
                <Tooltip content={<CTip/>}/>
                <Bar dataKey="Pop. Quilombola" radius={[4,4,0,0]}>{quiPop.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.purple}/>)}</Bar>
              </BarChart>
            </ResponsiveContainer>
          </CC>
        )}
      </div>
    </div>;
  };

  // ─── DOMICÍLIOS ───
  const renderDom=()=>{
    const avgAgua    = D.length ? +(D.reduce((s,m)=>s+(m.agua||0),0)/D.length).toFixed(1) : 0;
    const avgEsgoto  = D.length ? +(D.reduce((s,m)=>s+(m.esgoto||0),0)/D.length).toFixed(1) : 0;
    const avgLixo    = D.length ? +(D.reduce((s,m)=>s+(m.lixo||0),0)/D.length).toFixed(1) : 0;
    const avgInternet= D.length ? +(D.reduce((s,m)=>s+(m.internet||0),0)/D.length).toFixed(1) : 0;

    // Sanitation triad by TSBio
    const saneamento = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Água Rede Geral":+t.agua.toFixed(1),
      "Coleta de Lixo":+t.lixo.toFixed(1),
      "Esgoto/Fossa Sépt.":+t.esgoto.toFixed(1),
    }));

    // Domicile types by TSBio
    const tiposDom = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Casa":+t.domCasaPct.toFixed(1),
      "Vila/Cond.":+t.domVilaPct.toFixed(1),
      "Apartamento":+t.domAptoPct.toFixed(1),
      "Cortiço":+t.domCorticoPct.toFixed(1),
      "Degradado":+t.domDegradadaPct.toFixed(1),
    }));

    // Wall materials by TSBio
    const materiais = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Alvenaria c/ Rev.":+t.domAlvRevPct.toFixed(1),
      "Alvenaria s/ Rev.":+t.domAlvSemPct.toFixed(1),
      "Taipa s/ Rev.":+t.domTaipaPct.toFixed(1),
      "Madeira Nova":+t.domMadNovaPct.toFixed(1),
      "Madeira Reaprov.":+t.domMadReapPct.toFixed(1),
    }));

    // Internet + washing machine by TSBio
    const acessoTec = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Internet (%)":+t.internet.toFixed(1),
      "Máq. Lavar (%)":+t.domLavadora.toFixed(1),
    }));

    // Occupancy status by TSBio
    const ocupacao = byT.map(t=>({
      name:sh(t.t), full:t.t,
      "Ocupados":+t.domOcupPct.toFixed(1),
      "Vagos":+t.domVagoPct.toFixed(1),
      "Uso Ocasional":+t.domOcasionalPct.toFixed(1),
    }));

    // Bottom sanitation ranking
    const aguaRank = [...D].sort((a,b)=>(a.agua||0)-(b.agua||0)).slice(0,10)
      .map(m=>({name:sh(m.n),full:m.n,"Água Rede (%)":m.agua,fill:TSBIO_COLORS[m.t]}));

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Água — Rede Geral Média" v={avgAgua} u="%" i="💧" c={T.blue}/>
        <KPI l="Coleta de Lixo Média" v={avgLixo} u="%" i="🗑️" c={T.green}/>
        <KPI l="Esgoto / Fossa Sépt." v={avgEsgoto} u="%" i="♻️" c={T.orange}/>
        <KPI l="Internet Domiciliar" v={avgInternet} u="%" i="🌐" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Tríade de Saneamento por TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={saneamento} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Água Rede Geral" fill={T.blue} radius={[4,4,0,0]}/>
              <Bar dataKey="Coleta de Lixo" fill={T.green} radius={[4,4,0,0]}/>
              <Bar dataKey="Esgoto/Fossa Sépt." fill={T.orange} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Tipos de Domicílio por TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={tiposDom} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Casa" stackId="a" fill={T.green}/>
              <Bar dataKey="Vila/Cond." stackId="a" fill={T.blue}/>
              <Bar dataKey="Apartamento" stackId="a" fill={T.gold}/>
              <Bar dataKey="Cortiço" stackId="a" fill={T.orange}/>
              <Bar dataKey="Degradado" stackId="a" fill={T.red} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Material das Paredes Externas por TSBio (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={materiais} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Alvenaria c/ Rev." stackId="a" fill={T.green}/>
              <Bar dataKey="Alvenaria s/ Rev." stackId="a" fill="#8cb4d8"/>
              <Bar dataKey="Taipa s/ Rev." stackId="a" fill={T.gold}/>
              <Bar dataKey="Madeira Nova" stackId="a" fill={T.orange}/>
              <Bar dataKey="Madeira Reaprov." stackId="a" fill={T.red} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Acesso à Tecnologia — Internet e Máq. de Lavar (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={acessoTec} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Internet (%)" fill={T.blue} radius={[4,4,0,0]}/>
              <Bar dataKey="Máq. Lavar (%)" fill={T.purple} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Situação dos Dom. Particulares Permanentes (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={ocupacao} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Ocupados" stackId="a" fill={T.green}/>
              <Bar dataKey="Vagos" stackId="a" fill={T.red}/>
              <Bar dataKey="Uso Ocasional" stackId="a" fill={T.gold} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="10 Municípios com Menor Acesso à Água — Rede Geral (%)" fonte="IBGE Censo 2022" periodo="2022">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={aguaRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Água Rede (%)" radius={[0,4,4,0]}>{aguaRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
      </div>
    </div>;
  };

  // ─── AMBIENTAL — Desmatamento & Degradação ───
  const renderAmDesm=()=>{
    if(!ambData)return null;
    const{prodes,deter}=ambData;
    const prodesChartData=prodes.anos.map((ano,i)=>{
      const e={ano};
      selT.forEach(t=>{e[sh(t)]=+(prodes.por_tsbio[t]?.series[i]||0).toFixed(1);});
      return e;
    });
    const deterByT=selT.filter(t=>deter.por_tsbio[t]).map(t=>{
      const e={name:sh(t),full:t};
      deter.classes.forEach(c=>{e[DETER_SHORT[c]||c]=deter.por_tsbio[t]?.por_classe[c]||0;});
      return e;
    });
    const deterPie=Object.entries(deter.por_classe).sort((a,b)=>b[1]-a[1])
      .map(([name,value])=>({name:DETER_SHORT[name]||name,value,rawName:name}));
    const deterAnual=deter.por_ano.map(({ano,km2})=>({ano,"Área (km²)":km2}));
    const prodesRank=prodes.ranking_municipios.filter(m=>selT.includes(m.tsbio)).slice(0,10)
      .map(m=>({name:sh(m.nome),full:m.nome,"Desmat. (km²)":m.area_km2,fill:TSBIO_COLORS[m.tsbio]||T.red}));
    const deterRank=deter.ranking_municipios.filter(m=>selT.includes(m.tsbio)).slice(0,10)
      .map(m=>({name:sh(m.nome),full:m.nome,"Alertas (km²)":m.total_km2,fill:TSBIO_COLORS[m.tsbio]||T.red}));
    const dShort=Object.values(DETER_SHORT);

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l={`Desmatamento PRODES ${prodes.ultimo_ano}`} v={prodes.total_ultimo_km2.toLocaleString("pt-BR",{maximumFractionDigits:0})} u="km²" i="🌳" c={T.red}/>
        <KPI l="Variação vs Ano Anterior" v={(prodes.var_pct_vs_anterior>0?"+":"")+prodes.var_pct_vs_anterior} u="%" i="📈" c={prodes.var_pct_vs_anterior>0?T.red:T.green}/>
        <KPI l="Total Alertas DETER (km²)" v={deter.total_km2.toLocaleString("pt-BR",{maximumFractionDigits:0})} u=" km²" i="⚠️" c={T.orange}/>
        <KPI l="Municípios com Alerta DETER" v={deter.n_municipios_com_alerta} i="🏛️" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Desmatamento PRODES — Série Histórica por TSBio (km²)" fonte="INPE/PRODES" periodo="2007–2025" h={280}>
          <ResponsiveContainer width="100%" height={260}>
            <AreaChart data={prodesChartData} margin={{left:8,right:8,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="ano" tick={{fontSize:9}}/>
              <YAxis tickFormatter={v=>`${(v/1000).toFixed(0)}k`} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              {selT.map(t=><Area key={t} type="monotone" dataKey={sh(t)} stackId="1" stroke={TSBIO_COLORS[t]} fill={TSBIO_COLORS[t]} fillOpacity={0.6}/>)}
            </AreaChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Alertas DETER por TSBio e Classe (km² acumulado)" fonte="INPE/DETER" periodo="Histórico">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={deterByT} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>`${(v/1000).toFixed(0)}k`} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              {deter.classes.map(c=><Bar key={c} dataKey={DETER_SHORT[c]||c} stackId="a" fill={DETER_COLORS_MAP[c]||T.orange}/>)}
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="DETER — Distribuição por Classe de Alerta" fonte="INPE/DETER" periodo="Histórico">
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie data={deterPie} cx="50%" cy="50%" innerRadius={55} outerRadius={90} dataKey="value" nameKey="name"
                label={({name,percent})=>`${name.slice(0,10)}: ${(percent*100).toFixed(0)}%`} labelLine={false}>
                {deterPie.map((e,i)=><Cell key={i} fill={DETER_COLORS_MAP[e.rawName]||T.orange}/>)}
              </Pie>
              <Tooltip formatter={(v)=>`${v.toLocaleString("pt-BR")} km²`}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </PieChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Alertas DETER — Tendência Anual (km²)" fonte="INPE/DETER" periodo="Histórico">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={deterAnual} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="ano" tick={{fontSize:9}}/>
              <YAxis tickFormatter={v=>`${(v/1000).toFixed(0)}k`} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Área (km²)" fill={T.red} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title={`Top 10 Municípios — Desmatamento PRODES ${prodes.ultimo_ano}`} fonte="INPE/PRODES" periodo={`${prodes.ultimo_ano}`}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={prodesRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit=" km²"/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Desmat. (km²)" radius={[0,4,4,0]}>{prodesRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Top 10 Municípios — Total Alertas DETER (km²)" fonte="INPE/DETER" periodo="Histórico">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={deterRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>`${(v/1000).toFixed(1)}k`} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Alertas (km²)" radius={[0,4,4,0]}>{deterRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
      </div>
    </div>;
  };

  // ─── AMBIENTAL — Uso do Solo & Queimadas ───
  const renderAmLULC=()=>{
    if(!ambData)return null;
    const{lulc,queimadas}=ambData;
    let totalNat=0,totalAnt=0;
    selT.forEach(t=>{
      totalNat+=lulc.por_tsbio[t]?.por_n0["Natural"]||0;
      totalAnt+=lulc.por_tsbio[t]?.por_n0["Antrópico"]||0;
    });
    const totalArea=totalNat+totalAnt;
    const pctNat=totalArea>0?+(totalNat/totalArea*100).toFixed(1):0;
    const pctAnt=totalArea>0?+(totalAnt/totalArea*100).toFixed(1):0;
    const qUltIdx=queimadas.anos.indexOf(queimadas.ultimo_ano);
    const qUltimo=selT.reduce((s,t)=>s+(queimadas.por_tsbio[t]?.series_ha[qUltIdx]||0),0);
    const n1Classes=lulc.classes_n1;
    const lulcByT=selT.filter(t=>lulc.por_tsbio[t]).map(t=>{
      const e={name:sh(t),full:t};
      n1Classes.forEach(c=>{e[c]=Math.round(lulc.por_tsbio[t].por_n1[c]||0);});
      return e;
    });
    const shAnos=lulc.serie_historica.anos;
    const lulcHist=shAnos.map((ano,i)=>{
      let nat=0,ant=0;
      selT.forEach(t=>{const ts=lulc.serie_historica.por_tsbio[t]||{};nat+=ts["Natural"]?.[i]||0;ant+=ts["Antrópico"]?.[i]||0;});
      return{ano,"Natural":Math.round(nat),"Antrópico":Math.round(ant)};
    });
    const qAnos=queimadas.anos.slice(-15);
    const qData=qAnos.map(ano=>{
      const idx=queimadas.anos.indexOf(ano);
      const e={ano};
      selT.forEach(t=>{e[sh(t)]=Math.round((queimadas.por_tsbio[t]?.series_ha[idx]||0)/1000);});
      return e;
    });
    const qRank=queimadas.ranking_municipios.filter(m=>selT.includes(m.tsbio)).slice(0,10)
      .map(m=>({name:sh(m.nome),full:m.nome,"Queimada (ha)":Math.round(m.area_ha),fill:TSBIO_COLORS[m.tsbio]}));
    const floAgro=selT.filter(t=>lulc.por_tsbio[t]).map(t=>({
      name:sh(t),full:t,
      "Floresta":Math.round(lulc.por_tsbio[t].por_n1["Floresta"]||0),
      "Agropecuária":Math.round(lulc.por_tsbio[t].por_n1["Agropecuária"]||0),
    }));
    const pieN0=[{name:"Natural",value:Math.round(totalNat)},{name:"Antrópico",value:Math.round(totalAnt)}];

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Cobertura Natural" v={pctNat} u="%" i="🌿" c={T.green}/>
        <KPI l="Área Antrópica" v={pctAnt} u="%" i="🌾" c={T.orange}/>
        <KPI l={`Queimada ${queimadas.ultimo_ano} (ha)`} v={Math.round(qUltimo/1000).toLocaleString("pt-BR")+"k"} i="🔥" c={T.red}/>
        <KPI l="Ref. MapBiomas" v={lulc.ultimo_ano} i="📡" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Composição de Uso do Solo — Natural vs Antrópico" fonte="MapBiomas Col 10" periodo={`${lulc.ultimo_ano}`}>
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie data={pieN0} cx="50%" cy="50%" innerRadius={60} outerRadius={95} dataKey="value" nameKey="name"
                label={({name,percent})=>`${name}: ${(percent*100).toFixed(1)}%`} labelLine={false}>
                <Cell fill={T.green}/><Cell fill={T.orange}/>
              </Pie>
              <Tooltip formatter={(v)=>`${v.toLocaleString("pt-BR")} km²`}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </PieChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Uso do Solo por TSBio — Classes Detalhadas (km²)" fonte="MapBiomas Col 10" periodo={`${lulc.ultimo_ano}`}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={lulcByT} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>`${(v/1000).toFixed(0)}k`} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              {n1Classes.map(c=><Bar key={c} dataKey={c} stackId="a" fill={LULC_N1_COLORS[c]||"#aaa"}/>)}
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Tendência Histórica — Natural vs Antrópico (km²)" fonte="MapBiomas Col 10" periodo="1985–2024">
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={lulcHist} margin={{left:8,right:8,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="ano" tick={{fontSize:9}}/>
              <YAxis tickFormatter={v=>`${(v/1000).toFixed(0)}k`} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Area type="monotone" dataKey="Natural" stroke={T.green} fill={T.green} fillOpacity={0.5}/>
              <Area type="monotone" dataKey="Antrópico" stroke={T.orange} fill={T.orange} fillOpacity={0.5}/>
            </AreaChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Área Queimada por TSBio — Últimos 15 Anos (mil ha)" fonte="MapBiomas Fogo Col4" periodo="2010–2024">
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={qData} margin={{left:8,right:8,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="ano" tick={{fontSize:9}}/>
              <YAxis tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              {selT.map(t=><Area key={t} type="monotone" dataKey={sh(t)} stackId="1" stroke={TSBIO_COLORS[t]} fill={TSBIO_COLORS[t]} fillOpacity={0.6}/>)}
            </AreaChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Floresta vs Agropecuária por TSBio (km²)" fonte="MapBiomas Col 10" periodo={`${lulc.ultimo_ano}`}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={floAgro} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis tickFormatter={v=>`${(v/1000).toFixed(0)}k`} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Floresta" fill={T.green} radius={[4,4,0,0]}/>
              <Bar dataKey="Agropecuária" fill={T.orange} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        {qRank.length>0&&<CC title="Top 10 Municípios — Área Queimada (último ano)" fonte="MapBiomas Fogo Col4" periodo={`${queimadas.ultimo_ano}`}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={qRank} layout="vertical" margin={{left:4,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={90} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Queimada (ha)" radius={[0,4,4,0]}>{qRank.map((e,i)=><Cell key={i} fill={e.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
      </div>
    </div>;
  };

  // ─── AMBIENTAL — Ordenamento Fundiário ───
  const renderAmFund=()=>{
    if(!ambData)return null;
    const fund=ambData.fundiario;
    const tiN=selT.reduce((s,t)=>s+(fund.resumo_tsbio[t]?.ti_n||0),0);
    const ucN=selT.reduce((s,t)=>s+(fund.resumo_tsbio[t]?.uc_n||0),0);
    const carN=selT.reduce((s,t)=>s+(fund.resumo_tsbio[t]?.car_n||0),0);
    const tiHa=selT.reduce((s,t)=>s+(fund.resumo_tsbio[t]?.ti_ha||0),0);
    const ucHa=selT.reduce((s,t)=>s+(fund.resumo_tsbio[t]?.uc_ha||0),0);
    const quiHa=selT.reduce((s,t)=>s+(fund.resumo_tsbio[t]?.qui_ha||0),0);
    const astHa=selT.reduce((s,t)=>s+(fund.resumo_tsbio[t]?.assent_ha||0),0);
    const protHa=tiHa+ucHa+quiHa;
    const pieData=[
      {name:"Terras Indígenas",value:Math.round(tiHa/1000)},
      {name:"Unid. Conservação",value:Math.round(ucHa/1000)},
      {name:"Assentamentos",value:Math.round(astHa/1000)},
    ].filter(d=>d.value>0);
    const fundByT=selT.filter(t=>fund.resumo_tsbio[t]).map(t=>({
      name:sh(t),full:t,
      "Terras Indígenas":+(fund.resumo_tsbio[t].ti_ha/1e6).toFixed(2),
      "Unid. Conservação":+(fund.resumo_tsbio[t].uc_ha/1e6).toFixed(2),
      "Assentamentos":+(fund.resumo_tsbio[t].assent_ha/1e6).toFixed(2),
    }));
    const nByT=selT.filter(t=>fund.resumo_tsbio[t]).map(t=>({
      name:sh(t),full:t,
      "TIs":fund.resumo_tsbio[t].ti_n,
      "UCs":fund.resumo_tsbio[t].uc_n,
      "Assent.":fund.resumo_tsbio[t].assent_n,
    }));
    const tiDet=(fund.terras_indigenas.detalhes||[]).filter(d=>selT.includes(d.tsbio)).sort((a,b)=>b.area_ha-a.area_ha).slice(0,20);
    const ucDet=(fund.unidades_conservacao.detalhes||[]).filter(d=>selT.includes(d.tsbio)).sort((a,b)=>b.area_ha-a.area_ha).slice(0,15);
    const rowSt={display:"grid",gridTemplateColumns:"2fr 1.2fr 1fr 0.9fr",gap:6,padding:"5px 10px",fontSize:11,alignItems:"center",borderBottom:"1px solid #f0f2f0"};
    const hSt={...rowSt,fontWeight:700,background:"#f5f7f5",fontSize:10,textTransform:"uppercase",letterSpacing:0.5,color:T.textLight};

    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Terras Indígenas" v={tiN} i="🏕️" c={T.orange}/>
        <KPI l="Unidades de Conservação" v={ucN} i="🌳" c={T.green}/>
        <KPI l="Imóveis CAR" v={carN.toLocaleString("pt-BR")} i="📋" c={T.blue}/>
        <KPI l="Área Protegida (TI+UC+Qui)" v={(protHa/1e6).toLocaleString("pt-BR",{maximumFractionDigits:1})+"M ha"} i="🛡️" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Composição Fundiária — TI, UC, Assentamentos (mil ha)" fonte="FUNAI/ICMBio/INCRA" periodo="2026">
          <ResponsiveContainer width="100%" height={250}>
            <PieChart>
              <Pie data={pieData} cx="50%" cy="50%" innerRadius={55} outerRadius={90} dataKey="value" nameKey="name"
                label={({name,percent})=>`${name.slice(0,8)}: ${(percent*100).toFixed(0)}%`} labelLine={false}>
                {pieData.map((e,i)=><Cell key={i} fill={[T.orange,T.green,T.blue][i%3]}/>)}
              </Pie>
              <Tooltip formatter={(v)=>`${v.toLocaleString("pt-BR")} mil ha`}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </PieChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Cobertura por TSBio — TI + UC + Assentamentos (M ha)" fonte="FUNAI/ICMBio/INCRA" periodo="2026">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={fundByT} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>`${v}M`} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Terras Indígenas" stackId="a" fill={T.orange}/>
              <Bar dataKey="Unid. Conservação" stackId="a" fill={T.green}/>
              <Bar dataKey="Assentamentos" stackId="a" fill={T.blue} radius={[0,4,4,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Nº de Unidades por TSBio" fonte="FUNAI/ICMBio/INCRA" periodo="2026">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={nByT} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/>
              <YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="TIs" fill={T.orange} radius={[4,4,0,0]}/>
              <Bar dataKey="UCs" fill={T.green} radius={[4,4,0,0]}/>
              <Bar dataKey="Assent." fill={T.blue} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        {tiDet.length>0&&<CC title="Terras Indígenas — Lista Detalhada" fonte="FUNAI 2026" periodo="2026" h={Math.min(tiDet.length*30+50,300)}>
          <div style={{overflowY:"auto",maxHeight:280}}>
            <div style={hSt}><span>Terra Indígena</span><span>Município</span><span>TSBio</span><span>Área (ha)</span></div>
            {tiDet.map((d,i)=><div key={i} style={{...rowSt,background:i%2===0?"#fff":"#fafafa"}}>
              <span style={{fontWeight:600,color:T.text,fontSize:10}}>{d.nome?.slice(0,35)}</span>
              <span style={{color:T.textLight,fontSize:10}}>{d.municipio}</span>
              <span><span style={{display:"inline-block",width:7,height:7,borderRadius:7,background:TSBIO_COLORS[d.tsbio],marginRight:4}}/>
                <span style={{fontSize:10}}>{d.tsbio?.slice(0,10)}</span></span>
              <span style={{fontSize:10}}>{Math.round(d.area_ha).toLocaleString("pt-BR")}</span>
            </div>)}
          </div>
        </CC>}
        {ucDet.length>0&&<CC title="Unidades de Conservação — Lista Detalhada" fonte="ICMBio 2026" periodo="2026" h={Math.min(ucDet.length*30+50,300)}>
          <div style={{overflowY:"auto",maxHeight:280}}>
            <div style={hSt}><span>Unidade de Conservação</span><span>Município</span><span>TSBio</span><span>Área (ha)</span></div>
            {ucDet.map((d,i)=><div key={i} style={{...rowSt,background:i%2===0?"#fff":"#fafafa"}}>
              <span style={{fontWeight:600,color:T.text,fontSize:10}}>{d.nome?.slice(0,35)}</span>
              <span style={{color:T.textLight,fontSize:10}}>{d.municipio}</span>
              <span><span style={{display:"inline-block",width:7,height:7,borderRadius:7,background:TSBIO_COLORS[d.tsbio],marginRight:4}}/>
                <span style={{fontSize:10}}>{d.tsbio?.slice(0,10)}</span></span>
              <span style={{fontSize:10}}>{Math.round(d.area_ha).toLocaleString("pt-BR")}</span>
            </div>)}
          </div>
        </CC>}
      </div>
    </div>;
  };

  // ─── PRODUTIVA — Perfil Agropecuário ───
  const renderProdAgro=()=>{
    if(!prodData)return null;
    const ca=prodData.censo_agro||{};
    const byTsbio=ca.por_tsbio||{};
    const filtered=Object.entries(byTsbio).filter(([t])=>selT.includes(t));
    // KPIs
    const avgAgFam=filtered.length?+(filtered.reduce((s,[,d])=>s+(d.agricultura_familiar_perc||0),0)/filtered.length).toFixed(1):0;
    const avgAssist=filtered.length?+(filtered.reduce((s,[,d])=>s+(d.assistencia_tecnica_perc||0),0)/filtered.length).toFixed(1):0;
    const totalEstab=filtered.reduce((s,[,d])=>s+(d.estabelecimentos||0),0);
    const avgArea=filtered.length?+(filtered.reduce((s,[,d])=>s+(d.area_media_ha||0),0)/filtered.length).toFixed(1):0;
    // Radar: 8 atividades
    const atividadeKeys=[
      {k:"atividade_lavoura_permanente_perc",l:"Lavoura Perm."},
      {k:"atividade_lavoura_temporaria_perc",l:"Lavoura Temp."},
      {k:"atividade_pecuaria_perc",l:"Pecuária"},
      {k:"atividade_pesca_perc",l:"Pesca"},
      {k:"atividade_aquicultura_perc",l:"Aquicultura"},
      {k:"atividade_producao_florestal_perc",l:"Prod. Florestal"},
      {k:"atividade_horticultura_perc",l:"Horticultura"},
      {k:"atividade_sementes_mudas_perc",l:"Sementes/Mudas"},
    ];
    const radarData=atividadeKeys.map(({k,l})=>{
      const entry={subject:l};
      filtered.forEach(([t,d])=>{entry[sh(t)]=+(d[k]||0).toFixed(1)});
      return entry;
    });
    // Bar charts
    const agFamBar=filtered.map(([t,d])=>({name:sh(t),full:t,"Agri Familiar":+(d.agricultura_familiar_perc||0).toFixed(1)})).sort((a,b)=>b["Agri Familiar"]-a["Agri Familiar"]);
    const terrasBar=filtered.map(([t,d])=>({name:sh(t),full:t,"Lavoura":+(d.uso_lavoura_perc||0).toFixed(1),"Pastagem":+(d.uso_pastagem_perc||0).toFixed(1)}));
    const bovinosBar=filtered.map(([t,d])=>({name:sh(t),full:t,"Corte":+(d.bovinos_corte_perc||0).toFixed(1),"Leite":+(d.bovinos_leite_perc||0).toFixed(1)}));
    const tratorBar=filtered.map(([t,d])=>({name:sh(t),full:t,"ha/trator":+(d.area_por_trator_ha||0).toFixed(0)})).sort((a,b)=>b["ha/trator"]-a["ha/trator"]);
    const agrotoxBar=filtered.map(([t,d])=>({name:sh(t),full:t,"% Agrotóxicos":+(d.agrotoxicos_perc||0).toFixed(1)})).sort((a,b)=>b["% Agrotóxicos"]-a["% Agrotóxicos"]);
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="% Agricultura Familiar (média)" v={avgAgFam} u="%" i="🌱" c={T.green}/>
        <KPI l="% com Assistência Técnica" v={avgAssist} u="%" i="📋" c={T.blue}/>
        <KPI l="Nº Estabelecimentos (total)" v={totalEstab.toLocaleString("pt-BR")} i="🏡" c={T.orange}/>
        <KPI l="Área média (ha)" v={avgArea.toLocaleString("pt-BR")} u="ha" i="📐" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        <CC title="Perfil de Atividades por TSBio (% estabelecimentos)" fonte="Censo Agro 2017" periodo="2017" h={300}>
          <ResponsiveContainer width="100%" height={290}>
            <RadarChart data={radarData} margin={{top:10,right:30,bottom:10,left:30}}>
              <PolarGrid/><PolarAngleAxis dataKey="subject" tick={{fontSize:9}}/>
              <PolarRadiusAxis angle={30} domain={[0,"auto"]} tick={{fontSize:8}}/>
              {filtered.map(([t])=><Radar key={t} name={sh(t)} dataKey={sh(t)} stroke={TSBIO_COLORS[t]} fill={TSBIO_COLORS[t]} fillOpacity={0.12}/>)}
              <Legend wrapperStyle={{fontSize:10}}/><Tooltip content={<CTip/>}/>
            </RadarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="% Agricultura Familiar por TSBio" fonte="Censo Agro 2017" periodo="2017">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={agFamBar} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Agri Familiar" radius={[0,4,4,0]}>{agFamBar.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Uso da Terra — Lavoura vs Pastagem (%)" fonte="Censo Agro 2017" periodo="2017">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={terrasBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Lavoura" stackId="a" fill={T.green} radius={[4,4,0,0]}/>
              <Bar dataKey="Pastagem" stackId="a" fill="#8B4513" radius={[0,0,4,4]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Bovinos — Corte vs Leite (% estab.)" fonte="Censo Agro 2017" periodo="2017">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={bovinosBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}} unit="%"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Corte" fill={T.red} radius={[4,4,0,0]}/>
              <Bar dataKey="Leite" fill={T.blue} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Mecanização — ha por Trator (quanto maior, menos mecanizado)" fonte="Censo Agro 2017" periodo="2017">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={tratorBar} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:10}} unit="ha"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="ha/trator" radius={[0,4,4,0]}>{tratorBar.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.orange}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Utilização de Agrotóxicos (% estabelecimentos)" fonte="Censo Agro 2017" periodo="2017">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={agrotoxBar} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="% Agrotóxicos" radius={[0,4,4,0]}>{agrotoxBar.map((e,i)=><Cell key={i} fill={T.red}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
      </div>
    </div>;
  };

  // ─── PRODUTIVA — Produção Agrícola (PAM) ───
  const renderProdPAM=()=>{
    if(!prodData)return null;
    const pam=prodData.pam||{};
    const perm=pam.permanente||{};
    const temp=pam.temporaria||{};
    const rankingComb=(pam.ranking_combinado||[]).filter(r=>selT.length===TSBIO_LIST.length||
      Object.keys((perm.por_tsbio||{})).some(t=>selT.includes(t)));
    // Aggregate por TSBio (selected only)
    const permByT=perm.por_tsbio||{};
    const tempByT=temp.por_tsbio||{};
    const filtPerm=Object.entries(permByT).filter(([t])=>selT.includes(t));
    const filtTemp=Object.entries(tempByT).filter(([t])=>selT.includes(t));
    // Total value (perm + temp, selected TSBios)
    let totalValor=0;
    filtPerm.forEach(([,d])=>Object.values(d.por_produto||{}).forEach(p=>totalValor+=p.valor||0));
    filtTemp.forEach(([,d])=>Object.values(d.por_produto||{}).forEach(p=>totalValor+=p.valor||0));
    // Combined ranking (selected TSBios)
    const combMap={};
    [...filtPerm,...filtTemp].forEach(([,d])=>Object.entries(d.por_produto||{}).forEach(([prod,pdata])=>{
      if(!combMap[prod])combMap[prod]={valor:0,quantidade:0};
      combMap[prod].valor+=pdata.valor||0;combMap[prod].quantidade+=pdata.quantidade||0;
    }));
    const top10=Object.entries(combMap).map(([p,v])=>({produto:p,valor:v.valor,quantidade:v.quantidade}))
      .sort((a,b)=>b.valor-a.valor).slice(0,10);
    const principalProd=top10[0]?.produto||"—";
    const totalArea=filtPerm.reduce((s,[,d])=>s+Object.values(d.por_produto||{}).reduce((s2,p)=>s2+(p.area_colhida||0),0),0)
      +filtTemp.reduce((s,[,d])=>s+Object.values(d.por_produto||{}).reduce((s2,p)=>s2+(p.area_colhida||0),0),0);
    // Top10 bar data
    const top10Bar=top10.map(r=>({name:r.produto.length>18?r.produto.slice(0,17)+"…":r.produto,full:r.produto,"Valor (R$ mil)":+(r.valor/1000).toFixed(0)}));
    // By TSBio stacked (top 5 products)
    const top5prods=top10.slice(0,5).map(r=>r.produto);
    const byTsbioBar=[...new Set([...filtPerm.map(([t])=>t),...filtTemp.map(([t])=>t)])].map(t=>{
      const entry={name:sh(t),full:t};
      top5prods.forEach(prod=>{
        const pv=(permByT[t]?.por_produto?.[prod]?.valor||0)+(tempByT[t]?.por_produto?.[prod]?.valor||0);
        entry[prod.length>12?prod.slice(0,11)+"…":prod]=+(pv/1000).toFixed(0);
      });
      return entry;
    });
    const top5Short=top5prods.map(p=>p.length>12?p.slice(0,11)+"…":p);
    // Rendimento culturas-chave Censo Agro (kg/ha), filtrar >0
    const rendKeys=[
      {k:"rendimento_cacau_kg_ha",l:"Cacau"},
      {k:"rendimento_mandioca_kg_ha",l:"Mandioca"},
      {k:"rendimento_milho_kg_ha",l:"Milho"},
      {k:"rendimento_arroz_kg_ha",l:"Arroz"},
      {k:"rendimento_cafe_kg_ha",l:"Café"},
      {k:"rendimento_laranja_kg_ha",l:"Laranja"},
    ];
    const caByT=prodData.censo_agro?.por_tsbio||{};
    const rendBar=rendKeys.map(({k,l})=>{
      const entry={cultura:l};
      Object.entries(caByT).filter(([t])=>selT.includes(t)).forEach(([t,d])=>{entry[sh(t)]=+(d[k]||0).toFixed(0)});
      return entry;
    }).filter(e=>Object.values(e).some((v,i)=>i>0&&v>0));
    // Série temporal (PAM permanente — açaí, cacau; PAM temporária — mandioca)
    const serieAnos=perm.serie?.anos||[];
    const serieProds=["Acai","Cacau","Mandioca"];
    const serieData=serieAnos.map((ano,ai)=>{
      const entry={ano};
      serieProds.forEach(prod=>{
        let total=0;
        const src=["Acai","Cacau"].includes(prod)?perm.serie:temp.serie;
        selT.forEach(t=>{total+=(src?.por_produto?.[prod]?.[t]?.[ai]||0)});
        entry[prod]=+(total/1000).toFixed(1);
      });
      return entry;
    });
    // Perm vs Temp por TSBio
    const permTempBar=[...new Set([...filtPerm.map(([t])=>t),...filtTemp.map(([t])=>t)])].map(t=>{
      const vp=Object.values(permByT[t]?.por_produto||{}).reduce((s,p)=>s+(p.valor||0),0);
      const vt=Object.values(tempByT[t]?.por_produto||{}).reduce((s,p)=>s+(p.valor||0),0);
      return{name:sh(t),full:t,"Permanente":+(vp/1000).toFixed(0),"Temporária":+(vt/1000).toFixed(0)};
    });
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Valor Total Produção (R$ mil)" v={(totalValor/1000).toLocaleString("pt-BR",{maximumFractionDigits:0})} i="💰" c={T.green}/>
        <KPI l="Principal Produto (por valor)" v={principalProd} i="🏆" c={T.orange}/>
        <KPI l="Área Total Colhida (ha)" v={Math.round(totalArea).toLocaleString("pt-BR")} i="🌿" c={T.blue}/>
        <KPI l="Ano de Referência PAM" v={pam.ultimo_ano||"—"} i="📅" c={T.purple}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        {top10Bar.length>0&&<CC title="Top 10 Produtos por Valor de Produção (R$ mil)" fonte="IBGE PAM 2024" periodo={String(pam.ultimo_ano||"")}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={top10Bar} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>`${v.toLocaleString("pt-BR")}`} tick={{fontSize:9}}/>
              <YAxis type="category" dataKey="name" width={120} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Valor (R$ mil)" radius={[0,4,4,0]}>{top10Bar.map((e,i)=><Cell key={i} fill={PROD_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {byTsbioBar.length>0&&top5Short.length>0&&<CC title="Valor Produção por TSBio — Top 5 Produtos (R$ mil)" fonte="IBGE PAM 2024" periodo={String(pam.ultimo_ano||"")}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={byTsbioBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}M`:v} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              {top5Short.map((p,i)=><Bar key={p} dataKey={p} stackId="a" fill={Object.values(PROD_COLORS)[i]||T.green}/>)}
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {rendBar.length>0&&<CC title="Rendimento de Culturas-chave (kg/ha)" fonte="IBGE Censo Agro 2017" periodo="2017">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={rendBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="cultura" tick={{fontSize:10}}/><YAxis tick={{fontSize:9}} unit="kg/ha"/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              {Object.entries(caByT).filter(([t])=>selT.includes(t)).map(([t])=><Bar key={t} dataKey={sh(t)} fill={TSBIO_COLORS[t]||T.green} radius={[4,4,0,0]}/>)}
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {serieData.length>0&&<CC title="Série Temporal — Açaí, Cacau & Mandioca (mil ton.)" fonte="IBGE PAM 2024" periodo="2010-2024">
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={serieData} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="ano" tick={{fontSize:10}}/><YAxis tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Area type="monotone" dataKey="Acai" stroke={T.purple} fill={T.purple} fillOpacity={0.15} strokeWidth={2}/>
              <Area type="monotone" dataKey="Cacau" stroke="#8B4513" fill="#8B4513" fillOpacity={0.15} strokeWidth={2}/>
              <Area type="monotone" dataKey="Mandioca" stroke={T.orange} fill={T.orange} fillOpacity={0.15} strokeWidth={2}/>
            </AreaChart>
          </ResponsiveContainer>
        </CC>}
        {permTempBar.length>0&&<CC title="Lavoura Permanente vs Temporária por TSBio (R$ mil)" fonte="IBGE PAM 2024" periodo={String(pam.ultimo_ano||"")}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={permTempBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}M`:v} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Bar dataKey="Permanente" fill={T.green} radius={[4,4,0,0]}/>
              <Bar dataKey="Temporária" fill={T.orange} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
      </div>
    </div>;
  };

  // ─── PRODUTIVA — Pecuária & Extrativismo ───
  const renderProdPecu=()=>{
    if(!prodData)return null;
    const ppm=prodData.ppm||{};
    const pevs=prodData.pevs||{};
    const rebanhosByT=ppm.rebanhos?.por_tsbio||{};
    const origByT=ppm.origem_animal?.por_tsbio||{};
    const extByT=pevs.extracao_vegetal?.por_tsbio||{};
    const filtRebanho=Object.entries(rebanhosByT).filter(([t])=>selT.includes(t));
    const filtExt=Object.entries(extByT).filter(([t])=>selT.includes(t));
    // KPIs
    const totalBovino=filtRebanho.reduce((s,[,d])=>s+(d.Bovino||0),0);
    const totalLeite=Object.entries(origByT).filter(([t])=>selT.includes(t))
      .reduce((s,[,d])=>s+(d.Leite?.quantidade||0),0);
    const totalValorExt=filtExt.reduce((s,[,d])=>s+Object.values(d.por_produto||{}).reduce((s2,p)=>s2+(p.valor||0),0),0);
    const nProdExt=new Set(filtExt.flatMap(([,d])=>Object.keys(d.por_produto||{}))).size;
    // Rebanhos bar
    const tiposReb=ppm.rebanhos?.tipos||[];
    const rebanhoBar=filtRebanho.map(([t,d])=>{
      const entry={name:sh(t),full:t};
      tiposReb.forEach(tp=>{entry[tp]=d[tp]||0});
      return entry;
    });
    // Origem animal bar
    const origBar=Object.entries(origByT).filter(([t])=>selT.includes(t)).map(([t,d])=>({
      name:sh(t),full:t,
      "Leite (mil l)":+(d.Leite?.quantidade||0).toFixed(0),
      "Ovos (mil dz)":+(d["Ovos de galinha"]?.quantidade||d.Ovos?.quantidade||0).toFixed(0),
      "Mel (kg)":+(d.Mel?.quantidade||0).toFixed(0),
    }));
    // Extração vegetal ranking
    const extMap={};
    filtExt.forEach(([,d])=>Object.entries(d.por_produto||{}).forEach(([p,v])=>{
      if(!extMap[p])extMap[p]={valor:0,quantidade:0};
      extMap[p].valor+=v.valor||0;extMap[p].quantidade+=v.quantidade||0;
    }));
    const extRanking=Object.entries(extMap).map(([p,v])=>({name:p.length>22?p.slice(0,21)+"…":p,full:p,"Valor (R$ mil)":+(v.valor/1000).toFixed(1),"Quantidade":v.quantidade}))
      .sort((a,b)=>b["Valor (R$ mil)"]-a["Valor (R$ mil)"]);
    // Série PEVS açaí e castanha
    const serieExt=pevs.extracao_vegetal?.serie||{};
    const serieAnos=serieExt.anos||[];
    const serieData=serieAnos.map((ano,ai)=>{
      const entry={ano};
      ["Acai (fruto)","Castanha-do-brasil"].forEach(prod=>{
        let total=0;
        selT.forEach(t=>{total+=(serieExt.por_produto?.[prod]?.[t]?.[ai]||0)});
        entry[prod==="Acai (fruto)"?"Açaí":"Castanha"]=+(total/1000).toFixed(1);
      });
      return entry;
    });
    // Aquicultura
    const aquiByT=ppm.aquicultura?.por_tsbio||{};
    const aquiBar=Object.entries(aquiByT).filter(([t])=>selT.includes(t)).map(([t,d])=>({
      name:sh(t),full:t,
      "Valor (R$ mil)":+(Object.values(d.por_produto||{}).reduce((s,p)=>s+(p.valor||0),0)/1000).toFixed(1),
    }));
    // Cards especiais açaí, castanha, borracha
    const highlights=["Acai (fruto)","Castanha-do-brasil","Borracha (latex)"].map(prod=>{
      let qty=0,val=0;
      filtExt.forEach(([,d])=>{qty+=(d.por_produto?.[prod]?.quantidade||0);val+=(d.por_produto?.[prod]?.valor||0)});
      return{prod,qty,val};
    }).filter(h=>h.val>0||h.qty>0);
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Efetivo Bovino Total" v={totalBovino.toLocaleString("pt-BR")} i="🐄" c={T.orange}/>
        <KPI l="Produção de Leite (mil litros)" v={totalLeite.toLocaleString("pt-BR",{maximumFractionDigits:0})} i="🥛" c={T.blue}/>
        <KPI l="Valor Extração Vegetal (R$ mil)" v={(totalValorExt/1000).toLocaleString("pt-BR",{maximumFractionDigits:0})} i="🌿" c={T.green}/>
        <KPI l="Nº Produtos PFNM com dados" v={nProdExt} i="🌰" c={T.purple}/>
      </div>
      {highlights.length>0&&<div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:10}}>
        {highlights.map(h=><div key={h.prod} style={{background:"#fff",borderRadius:12,padding:"14px 16px",boxShadow:"0 1px 6px rgba(0,0,0,0.06)",border:`1px solid ${T.border}`,textAlign:"center"}}>
          <div style={{fontSize:28,marginBottom:4}}>{h.prod.includes("Acai")?"🫐":h.prod.includes("Castanha")?"🌰":"🔵"}</div>
          <div style={{fontSize:13,fontWeight:700,color:T.text}}>{h.prod==="Acai (fruto)"?"Açaí":h.prod==="Castanha-do-brasil"?"Castanha":h.prod==="Borracha (latex)"?"Borracha":h.prod}</div>
          <div style={{fontSize:11,color:T.textLight,marginTop:4}}>{h.qty.toLocaleString("pt-BR")} ton • R$ {(h.val/1000).toLocaleString("pt-BR",{maximumFractionDigits:0})} mil</div>
        </div>)}
      </div>}
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        {rebanhoBar.length>0&&tiposReb.length>0&&<CC title="Efetivo de Rebanhos por TSBio" fonte="IBGE PPM 2024" periodo={String(ppm.rebanhos?.ultimo_ano||"")}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={rebanhoBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              {tiposReb.map(tp=><Bar key={tp} dataKey={tp} stackId="a" fill={REBANHO_COLORS[tp]||T.green}/>)}
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {origBar.length>0&&<CC title="Produção de Origem Animal por TSBio" fonte="IBGE PPM 2024" periodo={String(ppm.origem_animal?.ultimo_ano||"")}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={origBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:9}}/>
              <Bar dataKey="Leite (mil l)" fill={T.blue} radius={[4,4,0,0]}/>
              <Bar dataKey="Ovos (mil dz)" fill={T.orange} radius={[4,4,0,0]}/>
              <Bar dataKey="Mel (kg)" fill={T.gold} radius={[4,4,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {extRanking.length>0&&<CC title="Top Produtos Extração Vegetal (R$ mil)" fonte="IBGE PEVS 2024" periodo={String(pevs.extracao_vegetal?.ultimo_ano||"")}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={extRanking.slice(0,10)} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tick={{fontSize:9}}/><YAxis type="category" dataKey="name" width={130} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Valor (R$ mil)" radius={[0,4,4,0]}>{extRanking.slice(0,10).map((e,i)=><Cell key={i} fill={PROD_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {serieData.length>0&&<CC title="Série Histórica — Açaí e Castanha (mil ton.)" fonte="IBGE PEVS 2024" periodo="">
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={serieData} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="ano" tick={{fontSize:10}}/><YAxis tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/><Legend wrapperStyle={{fontSize:10}}/>
              <Area type="monotone" dataKey="Açaí" stroke={T.purple} fill={T.purple} fillOpacity={0.15} strokeWidth={2}/>
              <Area type="monotone" dataKey="Castanha" stroke="#8B4513" fill="#8B4513" fillOpacity={0.15} strokeWidth={2}/>
            </AreaChart>
          </ResponsiveContainer>
        </CC>}
        {aquiBar.length>0&&<CC title="Aquicultura por TSBio (valor R$ mil)" fonte="IBGE PPM 2024" periodo={String(ppm.aquicultura?.ultimo_ano||"")}>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={aquiBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Valor (R$ mil)" radius={[4,4,0,0]}>{aquiBar.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.blue}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
      </div>
    </div>;
  };

  // ─── PRODUTIVA — Cooperativas & Empreendimentos ───
  const renderProdCoop=()=>{
    if(!prodData)return null;
    const coop=prodData.cooperativas||{};
    const bcb=coop.bcb||{};
    const conexsus=coop.conexsus||{};
    const bcbByT=bcb.por_tsbio||{};
    const conexsusByT=conexsus.por_tsbio||{};
    const filtBcb=Object.entries(bcbByT).filter(([t])=>selT.includes(t));
    // KPIs
    const totalCoops=filtBcb.reduce((s,[,d])=>s+(d.total_de_cooperativas||0),0);
    const totalCoopers=filtBcb.reduce((s,[,d])=>s+(d.total_de_cooperados||0),0);
    const totalFem=filtBcb.reduce((s,[,d])=>s+(d.sexo_feminino||0),0);
    const pctFem=totalCoopers>0?+(totalFem/totalCoopers*100).toFixed(1):0;
    const totalOrgs=Object.entries(conexsusByT).filter(([t])=>selT.includes(t)).reduce((s,[,d])=>s+(d.n_organizacoes||0),0);
    // Cooperados por TSBio
    const coopBar=filtBcb.map(([t,d])=>({name:sh(t),full:t,"Cooperados":d.total_de_cooperados||0})).sort((a,b)=>b.Cooperados-a.Cooperados);
    // Gênero pie
    const genPie=[
      {name:"Feminino",value:filtBcb.reduce((s,[,d])=>s+(d.sexo_feminino||0),0),fill:"#e91e8c"},
      {name:"Masculino",value:filtBcb.reduce((s,[,d])=>s+(d.sexo_masculino||0),0),fill:T.blue},
    ].filter(p=>p.value>0);
    // Faixa etária
    const idadeLabels=[
      {k:"idade_menor_que_18_anos",l:"< 18"},
      {k:"idade_maior_que_18_e_menor_que_30_anos",l:"18-30"},
      {k:"idade_maior_que_30_e_menor_que_50_anos",l:"30-50"},
      {k:"idade_maior_que_50_e_menor_que_70_anos",l:"50-70"},
      {k:"idade_acima_de_70_anos",l:"> 70"},
    ];
    const idadeBar=idadeLabels.map(({k,l})=>({faixa:l,Cooperados:filtBcb.reduce((s,[,d])=>s+(d[k]||0),0)}));
    // Filiação
    const filiacaoLabels=[
      {k:"filiacao_ate_1_ano",l:"≤ 1 ano"},
      {k:"filiacao_de_1_a_5_anos",l:"1-5 anos"},
      {k:"filiacao_de_5_a_10_anos",l:"5-10 anos"},
      {k:"filiacao_acima_de_10_anos",l:"> 10 anos"},
    ];
    const filBar=filiacaoLabels.map(({k,l})=>({tempo:l,Cooperados:filtBcb.reduce((s,[,d])=>s+(d[k]||0),0)}));
    // Conexsus orgs table
    const conexsusOrgs=Object.entries(conexsusByT).filter(([t])=>selT.includes(t))
      .flatMap(([t,d])=>(d.organizacoes||[]).map(o=>({...o,tsbio:t})));
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:12}}>
        <KPI l="Total Cooperativas de Crédito" v={totalCoops.toLocaleString("pt-BR")} i="🏦" c={T.green}/>
        <KPI l="Total Cooperados" v={totalCoopers.toLocaleString("pt-BR")} i="👥" c={T.blue}/>
        <KPI l="% Mulheres Cooperadas" v={pctFem} u="%" i="♀️" c="#e91e8c"/>
        <KPI l="Nº Organizações Conexsus" v={totalOrgs} i="🤝" c={T.orange}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(360px,1fr))",gap:14}}>
        {coopBar.length>0&&<CC title="Cooperados por TSBio" fonte="Banco Central 2025" periodo="2025">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={coopBar} layout="vertical" margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis type="number" tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:10}}/>
              <YAxis type="category" dataKey="name" width={95} tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Cooperados" radius={[0,4,4,0]}>{coopBar.map((e,i)=><Cell key={i} fill={TSBIO_COLORS[e.full]||T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {genPie.length>0&&<CC title="Cooperados por Gênero" fonte="Banco Central 2025" periodo="2025" h={250}>
          <ResponsiveContainer width="100%" height={240}>
            <PieChart><Pie data={genPie} cx="50%" cy="50%" innerRadius={60} outerRadius={100} dataKey="value" label={({name,percent})=>`${name}: ${(percent*100).toFixed(0)}%`} labelLine={false}>
              {genPie.map((e,i)=><Cell key={i} fill={e.fill}/>)}
            </Pie><Tooltip content={<CTip/>}/></PieChart>
          </ResponsiveContainer>
        </CC>}
        {idadeBar.length>0&&<CC title="Cooperados por Faixa Etária" fonte="Banco Central 2025" periodo="2025">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={idadeBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="faixa" tick={{fontSize:10}}/><YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Cooperados" radius={[4,4,0,0]}>{idadeBar.map((e,i)=><Cell key={i} fill={T.blue}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
        {filBar.length>0&&<CC title="Cooperados por Tempo de Filiação" fonte="Banco Central 2025" periodo="2025">
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={filBar} margin={{left:8,right:16,top:8,bottom:8}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#eee"/>
              <XAxis dataKey="tempo" tick={{fontSize:10}}/><YAxis tickFormatter={v=>v>=1000?`${(v/1000).toFixed(0)}k`:v} tick={{fontSize:9}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Cooperados" radius={[4,4,0,0]}>{filBar.map((e,i)=><Cell key={i} fill={T.green}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>}
      </div>
      {conexsusOrgs.length>0&&<div style={{background:"#fff",borderRadius:12,padding:"16px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
          <h3 style={{margin:0,fontSize:13.5,fontWeight:700,color:T.text}}>Organizações Conexsus ({conexsusOrgs.length})</h3>
          <span style={{display:"inline-flex",alignItems:"center",gap:4,padding:"2px 8px",borderRadius:16,background:"rgba(27,122,61,0.08)",color:T.green,fontSize:10,fontWeight:600,border:"1px solid rgba(27,122,61,0.15)"}}>📊 Conexsus 2019</span>
        </div>
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><tr style={{background:"#f4f7f4",borderBottom:`2px solid ${T.border}`}}>
              {["Organização","Município","TSBio","Natureza","Nº Assoc.","Produtos"].map(h=><th key={h} style={{padding:"6px 10px",textAlign:"left",fontWeight:700,color:T.text,whiteSpace:"nowrap"}}>{h}</th>)}
            </tr></thead>
            <tbody>{conexsusOrgs.slice(0,30).map((o,i)=><tr key={i} style={{borderBottom:`1px solid ${T.border}`,background:i%2===0?"#fafbfa":"#fff"}}>
              <td style={{padding:"5px 10px",color:T.text,fontWeight:600,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{o.nome}</td>
              <td style={{padding:"5px 10px",color:T.textLight}}>{o.municipio}</td>
              <td style={{padding:"5px 10px"}}><span style={{display:"inline-block",background:`${TSBIO_COLORS[o.tsbio]}22`,color:TSBIO_COLORS[o.tsbio],padding:"1px 6px",borderRadius:8,fontWeight:700,fontSize:10}}>{sh(o.tsbio)}</span></td>
              <td style={{padding:"5px 10px",color:T.textLight}}>{o.natureza}</td>
              <td style={{padding:"5px 10px",color:T.text,textAlign:"center"}}>{o.associados}</td>
              <td style={{padding:"5px 10px",color:T.textLight,maxWidth:200,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{o.produtos}</td>
            </tr>)}</tbody>
          </table>
        </div>
        {conexsusOrgs.length>30&&<div style={{textAlign:"center",padding:"6px",color:T.textLight,fontSize:10}}>Exibindo 30 de {conexsusOrgs.length} organizações</div>}
      </div>}
    </div>;
  };

  const renderDimPH=(id)=>{
    const d=DIMS.find(x=>x.id===id);
    return <div style={{textAlign:"center",padding:"50px 20px",color:T.textLight}}>
      <div style={{fontSize:44,marginBottom:12}}>{d?.i}</div>
      <h3 style={{fontSize:18,fontWeight:700,color:"#4a5a4a",margin:"0 0 6px"}}>Dimensão {d?.l}</h3>
      <p style={{fontSize:13,maxWidth:380,margin:"0 auto",lineHeight:1.5}}>Em construção — próxima iteração.</p>
    </div>;
  };

  // ─── LOADING / ERROR ───
  if(loading) return (
    <div style={{minHeight:"100vh",background:T.bg,display:"flex",alignItems:"center",justifyContent:"center",fontFamily:"'DM Sans','Segoe UI',sans-serif"}}>
      <div style={{textAlign:"center",color:T.textLight}}>
        <div style={{fontSize:44,marginBottom:16}}>🌿</div>
        <h2 style={{fontSize:18,fontWeight:700,color:T.green,margin:"0 0 8px"}}>Carregando dados TSBio…</h2>
        <p style={{fontSize:13,margin:0}}>Processando 723 indicadores · 65 municípios · 6 territórios</p>
      </div>
    </div>
  );

  // ═══════════════════════════════════════════════════════
  // INFRAESTRUTURA RENDERS
  // ═══════════════════════════════════════════════════════

  const renderInfraSan=()=>{
    if(!infraData)return null;
    const san=infraData.saneamento||{};
    const byMun=san.por_municipio||{};
    const byTs=san.por_tsbio||{};

    // Filter by selT
    const filtTs=Object.entries(byTs).filter(([t])=>selT.includes(t));
    const avg=(key)=>{
      const vals=filtTs.map(([,d])=>d[key]).filter(v=>v!=null);
      return vals.length>0?+(vals.reduce((s,v)=>s+v,0)/vals.length).toFixed(1):null;
    };

    // KPIs
    const kpiAgua=avg("agua_total_pct");
    const kpiEsgoto=avg("esgoto_total_pct");
    const kpiLixo=avg("lixo_total_pct");
    const kpiPerdas=avg("perdas_agua_pct");

    // Água urbana vs rural por TSBio
    const aguaBar=filtTs.map(([t,d])=>({name:sh(t),full:t,"Urbana":d.agua_urbana_pct||0,"Rural":d.agua_rural_pct||0}));

    // Esgoto breakdown por TSBio
    const esgotoBar=filtTs.map(([t,d])=>({name:sh(t),full:t,
      "Coleta+Trat":d.esgoto_col_trat_pct||0,
      "Coleta s/Trat":d.esgoto_col_semtrat_pct||0,
      "Sol. Individual":d.esgoto_individual_pct||0,
    }));

    // Lixo urbana vs rural por TSBio
    const lixoBar=filtTs.map(([t,d])=>({name:sh(t),full:t,"Urbana":d.lixo_urbano_pct||0,"Rural":d.lixo_rural_pct||0}));

    // Radar por TSBio
    const radarData=filtTs.map(([t,d])=>({subject:sh(t),
      "Água":d.agua_total_pct||0,
      "Esgoto":d.esgoto_total_pct||0,
      "Lixo":d.lixo_total_pct||0,
      "Sem Perdas":d.perdas_agua_pct!=null?Math.max(0,100-d.perdas_agua_pct):null,
    }));

    // Top 10 pior água
    const rankingAgua=(san.ranking_pior_agua||[]).filter(r=>selT.includes(r.tsbio)).slice(0,10);

    const KPI=({label,val,unit="%",color=T.blue})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:160}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:26,fontWeight:800,color}}>{val!=null?val+""+unit:"—"}</div>
      </div>
    );

    return (
      <div style={{display:"flex",flexDirection:"column",gap:18}}>
        <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
          <KPI label="Abastecimento de Água" val={kpiAgua} color={INFRA_COLORS.agua}/>
          <KPI label="Esgotamento Sanitário" val={kpiEsgoto} color={INFRA_COLORS.esgoto}/>
          <KPI label="Coleta de Resíduos" val={kpiLixo} color={INFRA_COLORS.lixo}/>
          <KPI label="Perdas na Rede d'Água" val={kpiPerdas} color={T.red}/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Abastecimento de Água — Urbano vs Rural por TSBio" fonte="IAS 2026" periodo="2026">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={aguaBar} margin={{top:4,right:12,left:-10,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
                <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Bar dataKey="Urbana" fill={INFRA_COLORS.agua} radius={[3,3,0,0]}/>
                <Bar dataKey="Rural" fill="#64B5F6" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </CC>
          <CC title="Esgotamento Sanitário por TSBio" fonte="IAS 2026" periodo="2026">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={esgotoBar} margin={{top:4,right:12,left:-10,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
                <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Bar dataKey="Coleta+Trat" stackId="a" fill={INFRA_COLORS.esgoto} radius={[0,0,0,0]}/>
                <Bar dataKey="Coleta s/Trat" stackId="a" fill="#BCAAA4" radius={[0,0,0,0]}/>
                <Bar dataKey="Sol. Individual" stackId="a" fill="#D7CCC8" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </CC>
          <CC title="Coleta de Resíduos — Urbano vs Rural por TSBio" fonte="IAS 2026" periodo="2026">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={lixoBar} margin={{top:4,right:12,left:-10,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
                <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Bar dataKey="Urbana" fill={INFRA_COLORS.lixo} radius={[3,3,0,0]}/>
                <Bar dataKey="Rural" fill="#FFE082" radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </CC>
          <CC title="Perfil Saneamento por TSBio (Radar)" fonte="IAS 2026" periodo="2026">
            <ResponsiveContainer width="100%" height={240}>
              <RadarChart data={radarData} cx="50%" cy="50%" outerRadius={90}>
                <PolarGrid/>
                <PolarAngleAxis dataKey="subject" tick={{fontSize:10}}/>
                <PolarRadiusAxis domain={[0,100]} tick={{fontSize:8}}/>
                <Radar name="Água" dataKey="Água" stroke={INFRA_COLORS.agua} fill={INFRA_COLORS.agua} fillOpacity={0.25}/>
                <Radar name="Esgoto" dataKey="Esgoto" stroke={INFRA_COLORS.esgoto} fill={INFRA_COLORS.esgoto} fillOpacity={0.2}/>
                <Radar name="Lixo" dataKey="Lixo" stroke={INFRA_COLORS.lixo} fill={INFRA_COLORS.lixo} fillOpacity={0.2}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Tooltip formatter={(v)=>v!=null?v.toFixed(1)+"%":"—"}/>
              </RadarChart>
            </ResponsiveContainer>
          </CC>
        </div>
        <CC title="Top 10 municípios com menor abastecimento de água" fonte="IAS 2026" periodo="2026">
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead>
                <tr style={{background:"#f5f5f5"}}>
                  <th style={{padding:"6px 10px",textAlign:"left",fontWeight:700}}>Município</th>
                  <th style={{padding:"6px 10px",textAlign:"left",fontWeight:700}}>TSBio</th>
                  <th style={{padding:"6px 10px",textAlign:"right",fontWeight:700}}>Água (%)</th>
                  <th style={{padding:"6px 10px",textAlign:"right",fontWeight:700}}>Esgoto (%)</th>
                  <th style={{padding:"6px 10px",textAlign:"right",fontWeight:700}}>Lixo (%)</th>
                </tr>
              </thead>
              <tbody>
                {rankingAgua.map((r,i)=>{
                  const md=Object.values(byMun).find(m=>m.nome===r.nome)||{};
                  return (
                    <tr key={i} style={{background:i%2===0?"#fff":"#fafafa",borderLeft:`3px solid ${INFRA_COLORS.agua}`}}>
                      <td style={{padding:"5px 10px",fontWeight:600}}>{r.nome}</td>
                      <td style={{padding:"5px 10px",color:TSBIO_COLORS[r.tsbio]||T.textLight,fontWeight:600}}>{r.tsbio}</td>
                      <td style={{padding:"5px 10px",textAlign:"right",color:T.red,fontWeight:700}}>{r.pct!=null?r.pct.toFixed(1)+"%":"—"}</td>
                      <td style={{padding:"5px 10px",textAlign:"right"}}>{md.esgoto_total_pct!=null?md.esgoto_total_pct.toFixed(1)+"%":"—"}</td>
                      <td style={{padding:"5px 10px",textAlign:"right"}}>{md.lixo_total_pct!=null?md.lixo_total_pct.toFixed(1)+"%":"—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CC>
      </div>
    );
  };

  const renderInfraEntor=()=>{
    if(!infraData)return null;
    const entor=infraData.entorno||{};
    const inds=entor.indicadores||[];
    const byMun=entor.por_municipio||{};
    const byTs=entor.por_tsbio||{};

    const filtTs=Object.entries(byTs).filter(([t])=>selT.includes(t));
    const avg=(key)=>{
      const vals=filtTs.map(([,d])=>d[key]).filter(v=>v!=null);
      return vals.length>0?+(vals.reduce((s,v)=>s+v,0)/vals.length).toFixed(1):null;
    };

    // KPIs
    const kpiViaPav=avg("via_pav_pct");
    const kpiIlum=avg("iluminacao_pct");
    const kpiArb=avg("arborizacao_pct");
    const kpiCalc=avg("calcada_pct");

    // Radar por TSBio (8 indicadores)
    const radarData=inds.map(ind=>({
      subject:ind.l,
      ...Object.fromEntries(filtTs.map(([t,d])=>[sh(t),d[ind.id]||0]))
    }));

    // Bar 4 principais indicadores por TSBio
    const bar4=filtTs.map(([t,d])=>({
      name:sh(t),full:t,
      "Via Pav.":d.via_pav_pct||0,
      "Iluminação":d.iluminacao_pct||0,
      "Calçada":d.calcada_pct||0,
      "Ponto Ônibus":d.ponto_onibus_pct||0,
    }));

    // Ranking municípios via pavimentada (top 15, filtrados por selT)
    const rankVia=Object.entries(byMun)
      .filter(([,d])=>selT.includes(d.tsbio))
      .map(([cod,d])=>({cod,nome:d.nome,tsbio:d.tsbio,pct:d.via_pav_pct}))
      .filter(r=>r.pct!=null)
      .sort((a,b)=>b.pct-a.pct)
      .slice(0,15);

    const KPI=({label,val,color=T.blue})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:160}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:26,fontWeight:800,color}}>{val!=null?val+"%":"—"}</div>
      </div>
    );

    return (
      <div style={{display:"flex",flexDirection:"column",gap:18}}>
        <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
          <KPI label="Via Pavimentada" val={kpiViaPav} color={T.green}/>
          <KPI label="Iluminação Pública" val={kpiIlum} color={T.gold}/>
          <KPI label="Arborização" val={kpiArb} color={T.green}/>
          <KPI label="Calçada/Passeio" val={kpiCalc} color={T.blue}/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Perfil Entorno por TSBio (Radar — 8 indicadores)" fonte="Censo 2022" periodo="2022">
            <ResponsiveContainer width="100%" height={280}>
              <RadarChart data={radarData} cx="50%" cy="50%" outerRadius={100}>
                <PolarGrid/>
                <PolarAngleAxis dataKey="subject" tick={{fontSize:9}}/>
                <PolarRadiusAxis domain={[0,100]} tick={{fontSize:8}}/>
                {filtTs.map(([t])=><Radar key={t} name={sh(t)} dataKey={sh(t)} stroke={TSBIO_COLORS[t]} fill={TSBIO_COLORS[t]} fillOpacity={0.15}/>)}
                <Legend wrapperStyle={{fontSize:9}}/>
                <Tooltip formatter={(v)=>v!=null?v.toFixed(1)+"%":"—"}/>
              </RadarChart>
            </ResponsiveContainer>
          </CC>
          <CC title="Comparativo 4 Indicadores por TSBio" fonte="Censo 2022" periodo="2022">
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={bar4} margin={{top:4,right:12,left:-10,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
                <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Bar dataKey="Via Pav." fill={T.green} radius={[3,3,0,0]}/>
                <Bar dataKey="Iluminação" fill={T.gold} radius={[3,3,0,0]}/>
                <Bar dataKey="Calçada" fill={T.blue} radius={[3,3,0,0]}/>
                <Bar dataKey="Ponto Ônibus" fill={T.purple} radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </CC>
        </div>
        <CC title="Ranking municípios por % Via Pavimentada (top 15)" fonte="Censo 2022" periodo="2022" h={300}>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart layout="vertical" data={rankVia} margin={{top:4,right:30,left:100,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis type="number" domain={[0,100]} tick={{fontSize:10}} unit="%"/>
              <YAxis type="category" dataKey="nome" tick={{fontSize:9}} width={95}/>
              <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
              <Bar dataKey="pct" name="Via Pavimentada" radius={[0,3,3,0]}>
                {rankVia.map((r,i)=><Cell key={i} fill={TSBIO_COLORS[r.tsbio]||T.green}/>)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </CC>
        <CC title="Municípios × Indicadores de Entorno" fonte="Censo 2022" periodo="2022">
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
              <thead>
                <tr style={{background:"#f5f5f5"}}>
                  <th style={{padding:"5px 8px",textAlign:"left",fontWeight:700,whiteSpace:"nowrap"}}>Município</th>
                  <th style={{padding:"5px 8px",textAlign:"left",fontWeight:700}}>TSBio</th>
                  {inds.map(ind=><th key={ind.id} style={{padding:"5px 8px",textAlign:"right",fontWeight:700,whiteSpace:"nowrap"}}>{ind.l}</th>)}
                </tr>
              </thead>
              <tbody>
                {Object.entries(byMun).filter(([,d])=>selT.includes(d.tsbio)).sort((a,b)=>a[1].nome.localeCompare(b[1].nome)).map(([cod,d],i)=>(
                  <tr key={cod} style={{background:i%2===0?"#fff":"#fafafa"}}>
                    <td style={{padding:"4px 8px",fontWeight:600,whiteSpace:"nowrap"}}>{d.nome}</td>
                    <td style={{padding:"4px 8px",color:TSBIO_COLORS[d.tsbio]||T.textLight,fontWeight:600,whiteSpace:"nowrap"}}>{d.tsbio}</td>
                    {inds.map(ind=>{
                      const v=d[ind.id];
                      const bg=v==null?"#f5f5f5":v>=75?"rgba(27,122,61,0.12)":v>=50?"rgba(232,135,30,0.10)":"rgba(196,52,45,0.10)";
                      const c=v==null?T.textLight:v>=75?T.green:v>=50?T.orange:T.red;
                      return <td key={ind.id} style={{padding:"4px 8px",textAlign:"right",background:bg,color:c,fontWeight:600}}>{v!=null?v.toFixed(1)+"%":"—"}</td>;
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CC>
      </div>
    );
  };

  const renderInfraTrans=()=>{
    if(!infraData)return null;
    const trans=infraData.transporte||{};
    const info=infraData.informatica||{};
    const transByTs=trans.por_tsbio||{};
    const infoByTs=info.por_tsbio||{};
    const transByMun=trans.por_municipio||{};
    const infoByMun=info.por_municipio||{};
    const transCols=trans.colunas||{};
    const infoCols=info.colunas||{};

    const filtTransTs=Object.entries(transByTs).filter(([t])=>selT.includes(t));
    const filtInfoTs=Object.entries(infoByTs).filter(([t])=>selT.includes(t));

    const avgPct=(data,key)=>{
      const vals=data.map(([,d])=>d[key+"_pct"]).filter(v=>v!=null);
      return vals.length>0?+(vals.reduce((s,v)=>s+v,0)/vals.length).toFixed(1):null;
    };

    // KPIs
    const kpiOnibus=avgPct(filtTransTs,"onibus_intra");
    const kpiBarco=avgPct(filtTransTs,"barco");
    const kpiPlano=avgPct(filtTransTs,"plano_transporte");
    const kpiWeb=avgPct(filtInfoTs,"websites");

    // Transporte modal por TSBio
    const modalBar=filtTransTs.map(([t,d])=>({
      name:sh(t),full:t,
      "Ônibus Intra":d.onibus_intra_pct||0,
      "Ônibus Inter":d.onibus_inter_pct||0,
      "Barco":d.barco_pct||0,
      "Avião":d.aviao_pct||0,
    }));

    // Gov digital por TSBio
    const digitalBar=filtInfoTs.map(([t,d])=>({
      name:sh(t),full:t,
      "Website":d.websites_pct||0,
      "WhatsApp":d.whatsapp_pct||0,
      "Telefone":d.telefone_pct||0,
    }));

    // Políticas transporte por TSBio
    const polBar=filtTransTs.map(([t,d])=>({
      name:sh(t),full:t,
      "Plano":d.plano_transporte_pct||0,
      "Conselho":d.conselho_transporte_pct||0,
      "Frota Adapt.":d.frota_adaptada_pct||0,
    }));

    const KPI=({label,val,color=T.blue})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:160}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:26,fontWeight:800,color}}>{val!=null?val+"%":"—"}</div>
        <div style={{fontSize:9,color:T.textLight}}>municípios com Sim</div>
      </div>
    );

    // Tabela municípios × infraestruturas
    const transKeys=["onibus_intra","barco","aviao","ciclovia","plano_transporte","frota_adaptada"];
    const infoKeys=["websites","whatsapp"];
    const allMuns=Object.entries(transByMun).filter(([,d])=>selT.includes(d.tsbio)).sort((a,b)=>a[1].nome.localeCompare(b[1].nome));

    return (
      <div style={{display:"flex",flexDirection:"column",gap:18}}>
        <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
          <KPI label="c/ Ônibus Intramunicipal" val={kpiOnibus} color={INFRA_COLORS.transporte}/>
          <KPI label="c/ Transporte Fluvial" val={kpiBarco} color={T.blue}/>
          <KPI label="c/ Plano de Transporte" val={kpiPlano} color={T.green}/>
          <KPI label="c/ Website Oficial" val={kpiWeb} color={INFRA_COLORS.digital}/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Modais de Transporte por TSBio (% municípios)" fonte="MUNIC 2024" periodo="2024">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={modalBar} margin={{top:4,right:12,left:-10,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
                <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Bar dataKey="Ônibus Intra" fill={INFRA_COLORS.transporte} radius={[3,3,0,0]}/>
                <Bar dataKey="Ônibus Inter" fill="#90A4AE" radius={[3,3,0,0]}/>
                <Bar dataKey="Barco" fill={T.blue} radius={[3,3,0,0]}/>
                <Bar dataKey="Avião" fill={T.purple} radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </CC>
          <CC title="Governo Digital por TSBio (% municípios)" fonte="MUNIC 2024" periodo="2024">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={digitalBar} margin={{top:4,right:12,left:-10,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
                <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Bar dataKey="Website" fill={INFRA_COLORS.digital} radius={[3,3,0,0]}/>
                <Bar dataKey="WhatsApp" fill="#26A69A" radius={[3,3,0,0]}/>
                <Bar dataKey="Telefone" fill={T.gold} radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </CC>
          <CC title="Políticas de Transporte por TSBio (% municípios)" fonte="MUNIC 2024" periodo="2024">
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={polBar} margin={{top:4,right:12,left:-10,bottom:0}}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
                <XAxis dataKey="name" tick={{fontSize:10}}/>
                <YAxis domain={[0,100]} tick={{fontSize:10}} unit="%"/>
                <Tooltip formatter={(v)=>v.toFixed(1)+"%"}/>
                <Legend wrapperStyle={{fontSize:10}}/>
                <Bar dataKey="Plano" fill={T.green} radius={[3,3,0,0]}/>
                <Bar dataKey="Conselho" fill={T.orange} radius={[3,3,0,0]}/>
                <Bar dataKey="Frota Adapt." fill={T.red} radius={[3,3,0,0]}/>
              </BarChart>
            </ResponsiveContainer>
          </CC>
          <CC title="Tipo de Conexão com a Internet por TSBio" fonte="MUNIC 2024" periodo="2024">
            <div style={{overflowX:"auto",maxHeight:240}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                <thead>
                  <tr style={{background:"#f5f5f5"}}>
                    <th style={{padding:"5px 8px",textAlign:"left",fontWeight:700}}>TSBio</th>
                    <th style={{padding:"5px 8px",textAlign:"right",fontWeight:700}}>n</th>
                    <th style={{padding:"5px 8px",textAlign:"right",fontWeight:700}}>Ônibus Intra</th>
                    <th style={{padding:"5px 8px",textAlign:"right",fontWeight:700}}>Barco</th>
                    <th style={{padding:"5px 8px",textAlign:"right",fontWeight:700}}>Website</th>
                  </tr>
                </thead>
                <tbody>
                  {filtTransTs.map(([t,d],i)=>{
                    const id=infoByTs[t]||{};
                    return (
                      <tr key={t} style={{background:i%2===0?"#fff":"#fafafa"}}>
                        <td style={{padding:"5px 8px",fontWeight:700,color:TSBIO_COLORS[t]}}>{t}</td>
                        <td style={{padding:"5px 8px",textAlign:"right"}}>{d.n}</td>
                        <td style={{padding:"5px 8px",textAlign:"right"}}>{d.onibus_intra_pct!=null?d.onibus_intra_pct.toFixed(1)+"%":"—"}</td>
                        <td style={{padding:"5px 8px",textAlign:"right"}}>{d.barco_pct!=null?d.barco_pct.toFixed(1)+"%":"—"}</td>
                        <td style={{padding:"5px 8px",textAlign:"right"}}>{id.websites_pct!=null?id.websites_pct.toFixed(1)+"%":"—"}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </CC>
        </div>
        <CC title="Municípios × Infraestrutura de Transporte & Conectividade" fonte="MUNIC 2024" periodo="2024">
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
              <thead>
                <tr style={{background:"#f5f5f5"}}>
                  <th style={{padding:"5px 8px",textAlign:"left",fontWeight:700}}>Município</th>
                  <th style={{padding:"5px 8px",textAlign:"left",fontWeight:700}}>TSBio</th>
                  {transKeys.map(k=><th key={k} style={{padding:"5px 8px",textAlign:"center",fontWeight:700,whiteSpace:"nowrap"}}>{transCols[k]||k}</th>)}
                  {infoKeys.map(k=><th key={k} style={{padding:"5px 8px",textAlign:"center",fontWeight:700,whiteSpace:"nowrap"}}>{infoCols[k]||k}</th>)}
                </tr>
              </thead>
              <tbody>
                {allMuns.map(([cod,d],i)=>{
                  const infoM=infoByMun[cod]||{};
                  return (
                    <tr key={cod} style={{background:i%2===0?"#fff":"#fafafa"}}>
                      <td style={{padding:"4px 8px",fontWeight:600,whiteSpace:"nowrap"}}>{d.nome}</td>
                      <td style={{padding:"4px 8px",color:TSBIO_COLORS[d.tsbio]||T.textLight,fontWeight:600,whiteSpace:"nowrap"}}>{d.tsbio}</td>
                      {transKeys.map(k=><td key={k} style={{padding:"4px 8px",textAlign:"center"}}>{d[k]===1?"✅":d[k]===0?"❌":"—"}</td>)}
                      {infoKeys.map(k=><td key={k} style={{padding:"4px 8px",textAlign:"center"}}>{infoM[k]===1?"✅":infoM[k]===0?"❌":"—"}</td>)}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CC>
      </div>
    );
  };

  // ═══════════════════════════════════════════════════════
  // VULNERABILIDADES RENDERS
  // ═══════════════════════════════════════════════════════

  const vulColor=(v)=>v==null?"#ccc":v>0.7?VUL_COLORS.muitoalto:v>0.5?VUL_COLORS.alto:v>0.3?VUL_COLORS.medio:v>0.1?VUL_COLORS.baixo:VUL_COLORS.muitobaixo;
  const vulCls=(v)=>v==null?"—":v>0.7?"Muito Alto":v>0.5?"Alto":v>0.3?"Médio":v>0.1?"Baixo":"Muito Baixo";
  const MOD_LABELS={saude:"Saúde",alimentar:"Seg. Alimentar",desastres:"Desastres",hidrico:"Rec. Hídricos",biodiversidade:"Biodiversidade",energia:"Energia"};
  const MOD_KEYS=["saude","alimentar","desastres","hidrico","biodiversidade","energia"];
  const VBox=({title,children})=>(
    <div style={{background:"#fff",borderRadius:14,padding:20,boxShadow:"0 2px 8px rgba(0,0,0,0.06)",border:`1px solid ${T.border}`}}>
      {title&&<div style={{fontWeight:700,fontSize:13,color:T.text,marginBottom:14}}>{title}</div>}
      {children}
    </div>
  );

  const renderVulOverview=()=>{
    if(!vulData)return null;
    const ov=vulData.overview.por_tsbio||{};
    const vts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const KPI=({label,val,color})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:22,fontWeight:800,color:color||"#333"}}>{val!=null?val.toFixed(2):"—"}</div>
        <div style={{fontSize:10,color:T.textLight,marginTop:4}}>{vulCls(val)}</div>
      </div>
    );
    // KPIs: average each module across selected TSBios
    const kpiVals=MOD_KEYS.map(k=>({k,v:vts.length?+(vts.reduce((s,ts)=>s+(ov[ts]?.[k]||0),0)/vts.length).toFixed(4):null}));
    const MOD_COLORS_MAP={saude:VUL_COLORS.saude,alimentar:VUL_COLORS.alimentar,desastres:VUL_COLORS.desastres,hidrico:VUL_COLORS.hidrico,biodiversidade:VUL_COLORS.biodiversidade,energia:VUL_COLORS.energia};
    // Radar data: 6 modules as axes, one polygon per TSBio
    const radarData=MOD_KEYS.map(k=>({comp:MOD_LABELS[k],...Object.fromEntries(vts.map(ts=>[sh(ts),+(ov[ts]?.[k]||0).toFixed(2)]))}));
    // Grouped bar: TSBio × module
    const barData=vts.map(ts=>({name:sh(ts),full:ts,...Object.fromEntries(MOD_KEYS.map(k=>[MOD_LABELS[k],+(ov[ts]?.[k]||0).toFixed(3)]))}));
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(160px,1fr))",gap:12}}>
        {kpiVals.map(({k,v})=><KPI key={k} label={MOD_LABELS[k]} val={v} color={MOD_COLORS_MAP[k]}/>)}
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Perfil de Vulnerabilidade por TSBio (Radar — 6 Módulos)">
          <ResponsiveContainer width="100%" height={320}>
            <RadarChart data={radarData}>
              <PolarGrid/><PolarAngleAxis dataKey="comp" tick={{fontSize:10}}/>
              <PolarRadiusAxis domain={[0,1]} tick={{fontSize:8}}/>
              {vts.map(ts=><Radar key={ts} name={sh(ts)} dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} fill={TSBIO_COLORS[ts]} fillOpacity={0.12}/>)}
              <Legend/>
            </RadarChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Score por Módulo e TSBio">
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={barData} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/>
              {MOD_KEYS.map(k=><Bar key={k} dataKey={MOD_LABELS[k]} fill={MOD_COLORS_MAP[k]} radius={[3,3,0,0]}/>)}
              <Legend/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
      <VBox title="Heatmap: TSBio × Módulo (score 0-1 — verde=baixo, vermelho=alto)">
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead>
              <tr>
                <th style={{padding:"6px 10px",textAlign:"left",background:"#f8f8f8"}}>TSBio</th>
                {MOD_KEYS.map(k=><th key={k} style={{padding:"6px 10px",textAlign:"center",background:"#f8f8f8"}}>{MOD_LABELS[k]}</th>)}
              </tr>
            </thead>
            <tbody>
              {vts.map(ts=>(
                <tr key={ts}>
                  <td style={{padding:"6px 10px",fontWeight:600,color:TSBIO_COLORS[ts]}}>{ts}</td>
                  {MOD_KEYS.map(k=>{const v=ov[ts]?.[k];return(
                    <td key={k} style={{padding:"6px 10px",textAlign:"center",background:vulColor(v),color:"#fff",fontWeight:600,borderRadius:4,border:"2px solid #fff"}}>
                      {v!=null?v.toFixed(2):"—"}
                    </td>
                  );})}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </VBox>
    </div>;
  };

  const renderVulSaude=()=>{
    if(!vulData)return null;
    const sd=vulData.saude;
    const vts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const DISEASES=[
      {id:"arboviroses",l:"Arboviroses",c:VUL_COLORS.saude},
      {id:"lta",l:"L. Tegumentar",c:"#E8871E"},
      {id:"lv",l:"L. Visceral",c:"#D4A843"},
      {id:"malaria",l:"Malária",c:"#7B2D8E"},
    ];
    const KPI=({label,val,color})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:22,fontWeight:800,color}}>{val!=null?val.toFixed(2):"—"}</div>
        <div style={{fontSize:10,color:T.textLight,marginTop:4}}>{vulCls(val)}</div>
      </div>
    );
    const avg=(d,key)=>vts.length?+(vts.reduce((s,ts)=>s+(d[ts]?.[key]||0),0)/vts.length).toFixed(4):null;
    // Grouped bar: 4 diseases × TSBio
    const barData=vts.map(ts=>({name:sh(ts),...Object.fromEntries(DISEASES.map(d=>[d.l,+(sd[d.id]?.por_tsbio?.[ts]?.indice||0).toFixed(3)]))}));
    // Radar: components for arboviroses
    const arbTs=sd.arboviroses?.por_tsbio||{};
    const compLabels={ameaca:"Ameaça",sensib:"Sensibil.",cap_adapt:"Cap.Adapt.",exposicao:"Exposição"};
    const radData=Object.keys(compLabels).map(k=>({comp:compLabels[k],...Object.fromEntries(vts.map(ts=>[sh(ts),+(arbTs[ts]?.[k]||0).toFixed(2)]))}));
    // Projections LineChart: time × TSBio
    const PROJ_STEPS=[{k:"indice",l:"Baseline"},{k:"proj.2030_otim",l:"2030 Otim."},{k:"proj.2030_pess",l:"2030 Pess."},{k:"proj.2050_otim",l:"2050 Otim."},{k:"proj.2050_pess",l:"2050 Pess."}];
    const getProj=(ts,kpath)=>{const[a,b]=kpath.split(".");const d=arbTs[ts];return b?+(d?.proj?.[b]||0).toFixed(3):+(d?.[a]||0).toFixed(3);};
    const projData=PROJ_STEPS.map(({k,l})=>({name:l,...Object.fromEntries(vts.map(ts=>[sh(ts),getProj(ts,k)]))}));
    // Top worst municipalities by arboviroses
    const allMun=Object.entries(vulData.nomes).filter(([c,info])=>selT.includes(info.ts)).map(([cod,info])=>{const v=sd.arboviroses?.por_municipio?.[cod];return{cod,n:info.n,ts:info.ts,v:v?.indice,c:v?.c_indice};}).filter(m=>m.v!=null).sort((a,b)=>b.v-a.v).slice(0,12);
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(160px,1fr))",gap:12}}>
        {DISEASES.map(d=><KPI key={d.id} label={d.l} val={avg(sd[d.id]?.por_tsbio||{},"indice")} color={d.c}/>)}
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Score por Doença e TSBio">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={barData} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/>
              {DISEASES.map(d=><Bar key={d.id} dataKey={d.l} fill={d.c} radius={[3,3,0,0]}/>)}
              <Legend/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Arboviroses — Componentes por TSBio (Radar)">
          <ResponsiveContainer width="100%" height={280}>
            <RadarChart data={radData}>
              <PolarGrid/><PolarAngleAxis dataKey="comp" tick={{fontSize:10}}/>
              <PolarRadiusAxis domain={[0,1]} tick={{fontSize:8}}/>
              {vts.map(ts=><Radar key={ts} name={sh(ts)} dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} fill={TSBIO_COLORS[ts]} fillOpacity={0.12}/>)}
              <Legend/>
            </RadarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
      <VBox title="Arboviroses — Projeção Climática (Baseline→2050) por TSBio">
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={projData}>
            <CartesianGrid strokeDasharray="3 3" vertical={false}/>
            <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
            <Tooltip formatter={(v)=>v?.toFixed(3)}/>
            {vts.map(ts=><Line key={ts} type="monotone" dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} strokeWidth={2} dot={{r:3}}/>)}
            <Legend/>
          </LineChart>
        </ResponsiveContainer>
      </VBox>
      <VBox title="Top Municípios Mais Vulneráveis — Arboviroses">
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><tr style={{background:"#f8f8f8"}}>{["Município","TSBio","Score","Classe"].map(h=><th key={h} style={{padding:"6px 10px",textAlign:"left"}}>{h}</th>)}</tr></thead>
            <tbody>{allMun.map((m,i)=>(<tr key={m.cod} style={{background:i%2===0?"#fafafa":"#fff"}}>
              <td style={{padding:"5px 10px"}}>{m.n}</td>
              <td style={{padding:"5px 10px",color:TSBIO_COLORS[m.ts],fontWeight:600}}>{sh(m.ts)}</td>
              <td style={{padding:"5px 10px",fontWeight:700}}>{m.v?.toFixed(3)}</td>
              <td style={{padding:"5px 10px"}}><span style={{background:vulColor(m.v),color:"#fff",borderRadius:4,padding:"2px 7px",fontSize:10,fontWeight:700}}>{m.c||vulCls(m.v)}</span></td>
            </tr>))}</tbody>
          </table>
        </div>
      </VBox>
    </div>;
  };

  const renderVulAlimentar=()=>{
    if(!vulData)return null;
    const al=vulData.alimentar;
    const vts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const KPI=({label,val,color})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:22,fontWeight:800,color}}>{val!=null?val.toFixed(2):"—"}</div>
        <div style={{fontSize:10,color:T.textLight,marginTop:4}}>{vulCls(val)}</div>
      </div>
    );
    const avg=(sr,key)=>vts.length?+(vts.reduce((s,ts)=>s+(al[sr]?.por_tsbio?.[ts]?.[key]||0),0)/vts.length).toFixed(4):null;
    const barComp=vts.map(ts=>({name:sh(ts),"Acesso e Consumo":+(al.acesso_consumo?.por_tsbio?.[ts]?.indice||0).toFixed(3),"Disponibilidade":+(al.disponibilidade?.por_tsbio?.[ts]?.indice||0).toFixed(3)}));
    const compKeys={ameaca:"Ameaça",sensib:"Sensibil.",cap_adapt:"Cap.Adapt.",exposicao:"Exposição",desnutricao:"Desnutrição",bolsa_fam:"Bolsa Família"};
    const acPt=al.acesso_consumo?.por_tsbio||{};
    const radData=Object.keys(compKeys).slice(0,4).map(k=>({comp:compKeys[k],...Object.fromEntries(vts.map(ts=>[sh(ts),+(acPt[ts]?.[k]||0).toFixed(2)]))}));
    const PROJ=[{k:"indice",l:"Baseline"},{k:"proj.2030_otim",l:"2030 Otim."},{k:"proj.2030_pess",l:"2030 Pess."},{k:"proj.2050_otim",l:"2050 Otim."},{k:"proj.2050_pess",l:"2050 Pess."}];
    const getV=(ts,kp)=>{const[a,b]=kp.split(".");const d=acPt[ts];return+(b?d?.proj?.[b]||0:d?.[a]||0).toFixed(3);};
    const projData=PROJ.map(({k,l})=>({name:l,...Object.fromEntries(vts.map(ts=>[sh(ts),getV(ts,k)]))}));
    const specBar=vts.map(ts=>({name:sh(ts),"Desnutrição":+(acPt[ts]?.desnutricao||0).toFixed(3),"Bolsa Família":+(acPt[ts]?.bolsa_fam||0).toFixed(3)}));
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
        <KPI label="Acesso e Consumo de Alimentos" val={avg("acesso_consumo","indice")} color={VUL_COLORS.alimentar}/>
        <KPI label="Disponibilidade de Alimentos" val={avg("disponibilidade","indice")} color={VUL_COLORS.hidrico}/>
        <KPI label="Ameaça Climática (Acesso)" val={avg("acesso_consumo","ameaca")} color={VUL_COLORS.saude}/>
        <KPI label="Capacidade Adaptativa (Acesso)" val={avg("acesso_consumo","cap_adapt")} color={VUL_COLORS.biodiversidade}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Acesso vs Disponibilidade por TSBio">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={barComp} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
              <Bar dataKey="Acesso e Consumo" fill={VUL_COLORS.alimentar} radius={[3,3,0,0]}/>
              <Bar dataKey="Disponibilidade" fill={VUL_COLORS.hidrico} radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Acesso e Consumo — Componentes (Radar)">
          <ResponsiveContainer width="100%" height={280}>
            <RadarChart data={radData}>
              <PolarGrid/><PolarAngleAxis dataKey="comp" tick={{fontSize:10}}/>
              <PolarRadiusAxis domain={[0,1]} tick={{fontSize:8}}/>
              {vts.map(ts=><Radar key={ts} name={sh(ts)} dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} fill={TSBIO_COLORS[ts]} fillOpacity={0.12}/>)}
              <Legend/>
            </RadarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Projeção Acesso e Consumo (Baseline→2050)">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={projData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/>
              {vts.map(ts=><Line key={ts} type="monotone" dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} strokeWidth={2} dot={{r:3}}/>)}
              <Legend/>
            </LineChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Indicadores Específicos: Desnutrição e Bolsa Família">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={specBar} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
              <Bar dataKey="Desnutrição" fill={VUL_COLORS.saude} radius={[3,3,0,0]}/>
              <Bar dataKey="Bolsa Família" fill={VUL_COLORS.biodiversidade} radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
    </div>;
  };

  const renderVulDesastres=()=>{
    if(!vulData)return null;
    const ds=vulData.desastres;
    const vts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const KPI=({label,val,color})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:22,fontWeight:800,color}}>{val!=null?val.toFixed(2):"—"}</div>
        <div style={{fontSize:10,color:T.textLight,marginTop:4}}>{vulCls(val)}</div>
      </div>
    );
    const avg=(sr,key)=>vts.length?+(vts.reduce((s,ts)=>s+(ds[sr]?.por_tsbio?.[ts]?.[key]||0),0)/vts.length).toFixed(4):null;
    const barComp=vts.map(ts=>({name:sh(ts),"Deslizamento":+(ds.deslizamento?.por_tsbio?.[ts]?.indice||0).toFixed(3),"Inundações":+(ds.inundacoes?.por_tsbio?.[ts]?.indice||0).toFixed(3)}));
    const deslPt=ds.deslizamento?.por_tsbio||{};
    const compKeys={ameaca:"Ameaça",sensib:"Sensibil.",cap_adapt:"Cap.Adapt.",exposicao:"Exposição",dom_risco:"Dom. em Risco"};
    const radData=Object.keys(compKeys).map(k=>({comp:compKeys[k],...Object.fromEntries(vts.map(ts=>[sh(ts),+(deslPt[ts]?.[k]||0).toFixed(2)]))}));
    const PROJ=[{k:"indice",l:"Baseline"},{k:"proj.2030_otim",l:"2030 Otim."},{k:"proj.2030_pess",l:"2030 Pess."},{k:"proj.2050_otim",l:"2050 Otim."},{k:"proj.2050_pess",l:"2050 Pess."}];
    const getV=(ts,kp)=>{const[a,b]=kp.split(".");const d=deslPt[ts];return+(b?d?.proj?.[b]||0:d?.[a]||0).toFixed(3);};
    const projData=PROJ.map(({k,l})=>({name:l,...Object.fromEntries(vts.map(ts=>[sh(ts),getV(ts,k)]))}));
    const domRiscoBar=vts.map(ts=>({name:sh(ts),"Desliz.":+(deslPt[ts]?.dom_risco||0).toFixed(3),"Inund.":+(ds.inundacoes?.por_tsbio?.[ts]?.dom_risco||0).toFixed(3)}));
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
        <KPI label="Deslizamento de Terra" val={avg("deslizamento","indice")} color={VUL_COLORS.desastres}/>
        <KPI label="Inundações e Enxurradas" val={avg("inundacoes","indice")} color={VUL_COLORS.hidrico}/>
        <KPI label="Ameaça — Deslizamento" val={avg("deslizamento","ameaca")} color={VUL_COLORS.saude}/>
        <KPI label="Dom. em Áreas de Risco" val={avg("deslizamento","dom_risco")} color={VUL_COLORS.alimentar}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Deslizamento vs Inundações por TSBio">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={barComp} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
              <Bar dataKey="Deslizamento" fill={VUL_COLORS.desastres} radius={[3,3,0,0]}/>
              <Bar dataKey="Inundações" fill={VUL_COLORS.hidrico} radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Deslizamento — Componentes (Radar)">
          <ResponsiveContainer width="100%" height={280}>
            <RadarChart data={radData}>
              <PolarGrid/><PolarAngleAxis dataKey="comp" tick={{fontSize:10}}/>
              <PolarRadiusAxis domain={[0,1]} tick={{fontSize:8}}/>
              {vts.map(ts=><Radar key={ts} name={sh(ts)} dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} fill={TSBIO_COLORS[ts]} fillOpacity={0.12}/>)}
              <Legend/>
            </RadarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Projeção Deslizamento (Baseline→2050)">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={projData}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/>
              {vts.map(ts=><Line key={ts} type="monotone" dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} strokeWidth={2} dot={{r:3}}/>)}
              <Legend/>
            </LineChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Domicílios em Áreas de Risco por TSBio">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={domRiscoBar} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
              <Bar dataKey="Desliz." fill={VUL_COLORS.desastres} radius={[3,3,0,0]}/>
              <Bar dataKey="Inund." fill={VUL_COLORS.hidrico} radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
    </div>;
  };

  const renderVulHidrico=()=>{
    if(!vulData)return null;
    const hr=vulData.hidrico.estresse;
    const vts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const pt=hr?.por_tsbio||{};
    const KPI=({label,val,color})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:22,fontWeight:800,color}}>{val!=null?val.toFixed(2):"—"}</div>
        <div style={{fontSize:10,color:T.textLight,marginTop:4}}>{vulCls(val)}</div>
      </div>
    );
    const avg=(key)=>vts.length?+(vts.reduce((s,ts)=>s+(pt[ts]?.[key]||0),0)/vts.length).toFixed(4):null;
    const barData=vts.map(ts=>({name:sh(ts),full:ts,"Índice":+(pt[ts]?.indice||0).toFixed(3)}));
    barData.forEach(d=>{d.fill=vulColor(d["Índice"]);});
    const compKeys={ameaca:"Ameaça",sensib:"Sensibil.",cap_adapt:"Cap.Adapt.",exposicao:"Exposição",ineficiencia:"Ineficiência",areas_deg:"Áreas Degrad."};
    const radData=Object.keys(compKeys).map(k=>({comp:compKeys[k],...Object.fromEntries(vts.map(ts=>[sh(ts),+(pt[ts]?.[k]||0).toFixed(2)]))}));
    const PROJ=[{k:"indice",l:"Baseline"},{k:"proj.2030_otim",l:"2030 Otim."},{k:"proj.2030_pess",l:"2030 Pess."},{k:"proj.2050_otim",l:"2050 Otim."},{k:"proj.2050_pess",l:"2050 Pess."}];
    const getV=(ts,kp)=>{const[a,b]=kp.split(".");const d=pt[ts];return+(b?d?.proj?.[b]||0:d?.[a]||0).toFixed(3);};
    const projData=PROJ.map(({k,l})=>({name:l,...Object.fromEntries(vts.map(ts=>[sh(ts),getV(ts,k)]))}));
    const allMun=Object.entries(vulData.nomes).filter(([c,info])=>selT.includes(info.ts)).map(([cod,info])=>{const v=hr?.por_municipio?.[cod];return{n:info.n,ts:info.ts,v:v?.indice,c:v?.c_indice};}).filter(m=>m.v!=null).sort((a,b)=>b.v-a.v).slice(0,10);
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
        <KPI label="Risco de Estresse Hídrico" val={avg("indice")} color={VUL_COLORS.hidrico}/>
        <KPI label="Ameaça de Escassez" val={avg("ameaca")} color={VUL_COLORS.saude}/>
        <KPI label="Sensibilidade" val={avg("sensib")} color={VUL_COLORS.alimentar}/>
        <KPI label="Ineficiência Distribuição" val={avg("ineficiencia")} color={VUL_COLORS.desastres}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Índice de Estresse Hídrico por TSBio">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={barData} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/>
              <Bar dataKey="Índice" radius={[4,4,0,0]}>{barData.map((d,i)=><Cell key={i} fill={d.fill}/>)}</Bar>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Componentes — Radar">
          <ResponsiveContainer width="100%" height={280}>
            <RadarChart data={radData}>
              <PolarGrid/><PolarAngleAxis dataKey="comp" tick={{fontSize:10}}/>
              <PolarRadiusAxis domain={[0,1]} tick={{fontSize:8}}/>
              {vts.map(ts=><Radar key={ts} name={sh(ts)} dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} fill={TSBIO_COLORS[ts]} fillOpacity={0.12}/>)}
              <Legend/>
            </RadarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
      <VBox title="Projeção Estresse Hídrico (Baseline→2050)">
        <ResponsiveContainer width="100%" height={240}>
          <LineChart data={projData}>
            <CartesianGrid strokeDasharray="3 3" vertical={false}/>
            <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
            <Tooltip formatter={(v)=>v?.toFixed(3)}/>
            {vts.map(ts=><Line key={ts} type="monotone" dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} strokeWidth={2} dot={{r:3}}/>)}
            <Legend/>
          </LineChart>
        </ResponsiveContainer>
      </VBox>
      <VBox title="Top 10 Municípios — Estresse Hídrico">
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <thead><tr style={{background:"#f8f8f8"}}>{["Município","TSBio","Score","Classe"].map(h=><th key={h} style={{padding:"6px 10px",textAlign:"left"}}>{h}</th>)}</tr></thead>
          <tbody>{allMun.map((m,i)=>(<tr key={i} style={{background:i%2===0?"#fafafa":"#fff"}}>
            <td style={{padding:"5px 10px"}}>{m.n}</td>
            <td style={{padding:"5px 10px",color:TSBIO_COLORS[m.ts],fontWeight:600}}>{sh(m.ts)}</td>
            <td style={{padding:"5px 10px",fontWeight:700}}>{m.v?.toFixed(3)}</td>
            <td style={{padding:"5px 10px"}}><span style={{background:vulColor(m.v),color:"#fff",borderRadius:4,padding:"2px 7px",fontSize:10,fontWeight:700}}>{m.c||vulCls(m.v)}</span></td>
          </tr>))}</tbody>
        </table>
      </VBox>
    </div>;
  };

  const renderVulBiodiv=()=>{
    if(!vulData)return null;
    const bio=vulData.biodiversidade.bioma;
    const vts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const pt=bio?.por_tsbio||{};
    const KPI=({label,val,color})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:22,fontWeight:800,color}}>{val!=null?val.toFixed(2):"—"}</div>
        <div style={{fontSize:10,color:T.textLight,marginTop:4}}>{vulCls(val)}</div>
      </div>
    );
    const avg=(key)=>vts.length?+(vts.reduce((s,ts)=>s+(pt[ts]?.[key]||0),0)/vts.length).toFixed(4):null;
    const specKeys={fogo:"Fogo",agrotoxico:"Agrotóxico",mineracao:"Mineração",pastagem_deg:"Past. Degrad.",area_prot:"Área Protegida",ameaca:"Ameaça Clim.",cap_adapt:"Cap. Adapt."};
    const radData=Object.keys(specKeys).map(k=>({comp:specKeys[k],...Object.fromEntries(vts.map(ts=>[sh(ts),+(pt[ts]?.[k]||0).toFixed(2)]))}));
    const barBase=vts.map(ts=>({name:sh(ts),"Base. 2017":+(pt[ts]?.indice||0).toFixed(3),"2040 +1.5°C":+(pt[ts]?.proj?.["2040_swl1"]||0).toFixed(3),"2040 +2°C":+(pt[ts]?.proj?.["2040_swl2"]||0).toFixed(3)}));
    const allMun=Object.entries(vulData.nomes).filter(([c,info])=>selT.includes(info.ts)).map(([cod,info])=>{const v=bio?.por_municipio?.[cod];return{n:info.n,ts:info.ts,v:v?.indice,c:v?.c_indice};}).filter(m=>m.v!=null).sort((a,b)=>b.v-a.v).slice(0,10);
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
        <KPI label="Integridade do Bioma" val={avg("indice")} color={VUL_COLORS.biodiversidade}/>
        <KPI label="Ameaça Climática" val={avg("ameaca")} color={VUL_COLORS.saude}/>
        <KPI label="Fogo" val={avg("fogo")} color={VUL_COLORS.alimentar}/>
        <KPI label="Pastagem Degradada" val={avg("pastagem_deg")} color={VUL_COLORS.energia}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Componentes de Pressão sobre o Bioma (Radar)">
          <ResponsiveContainer width="100%" height={280}>
            <RadarChart data={radData}>
              <PolarGrid/><PolarAngleAxis dataKey="comp" tick={{fontSize:9}}/>
              <PolarRadiusAxis domain={[0,1]} tick={{fontSize:8}}/>
              {vts.map(ts=><Radar key={ts} name={sh(ts)} dataKey={sh(ts)} stroke={TSBIO_COLORS[ts]} fill={TSBIO_COLORS[ts]} fillOpacity={0.12}/>)}
              <Legend/>
            </RadarChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Integridade do Bioma — Baseline vs Projeções Climáticas">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={barBase} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
              <Bar dataKey="Base. 2017" fill={VUL_COLORS.biodiversidade} radius={[3,3,0,0]}/>
              <Bar dataKey="2040 +1.5°C" fill={VUL_COLORS.alimentar} radius={[3,3,0,0]}/>
              <Bar dataKey="2040 +2°C" fill={VUL_COLORS.saude} radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
      <VBox title="Top 10 Municípios — Vulnerabilidade do Bioma">
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
          <thead><tr style={{background:"#f8f8f8"}}>{["Município","TSBio","Score","Classe"].map(h=><th key={h} style={{padding:"6px 10px",textAlign:"left"}}>{h}</th>)}</tr></thead>
          <tbody>{allMun.map((m,i)=>(<tr key={i} style={{background:i%2===0?"#fafafa":"#fff"}}>
            <td style={{padding:"5px 10px"}}>{m.n}</td>
            <td style={{padding:"5px 10px",color:TSBIO_COLORS[m.ts],fontWeight:600}}>{sh(m.ts)}</td>
            <td style={{padding:"5px 10px",fontWeight:700}}>{m.v?.toFixed(3)}</td>
            <td style={{padding:"5px 10px"}}><span style={{background:vulColor(m.v),color:"#fff",borderRadius:4,padding:"2px 7px",fontSize:10,fontWeight:700}}>{m.c||vulCls(m.v)}</span></td>
          </tr>))}</tbody>
        </table>
      </VBox>
    </div>;
  };

  const renderVulEnergia=()=>{
    if(!vulData)return null;
    const en=vulData.energia;
    const vts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const ptA=en?.acesso_energia?.por_tsbio||{};
    const ptD=en?.disponib_energia?.por_tsbio||{};
    const KPI=({label,val,color})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:22,fontWeight:800,color}}>{val!=null?val.toFixed(2):"—"}</div>
        <div style={{fontSize:10,color:T.textLight,marginTop:4}}>{vulCls(val)}</div>
      </div>
    );
    const avg=(pt,key)=>vts.length?+(vts.reduce((s,ts)=>s+(pt[ts]?.[key]||0),0)/vts.length).toFixed(4):null;
    const barComp=vts.map(ts=>({name:sh(ts),"Acesso Energia":+(ptA[ts]?.indice||0).toFixed(3),"Disponibilidade":+(ptD[ts]?.indice||0).toFixed(3)}));
    const potBar=vts.map(ts=>({name:sh(ts),"Solar":+(ptA[ts]?.pot_solar||0).toFixed(3),"Eólico":+(ptA[ts]?.pot_eolico||0).toFixed(3),"Hidrelétrico":+(ptA[ts]?.pot_hidro||0).toFixed(3)}));
    const projBar=vts.map(ts=>({name:sh(ts),"Acesso Base.":+(ptA[ts]?.indice||0).toFixed(3),"Acesso 2055 swl2":+(ptA[ts]?.proj?.["2055_swl2"]||0).toFixed(3),"Dispon. Base.":+(ptD[ts]?.indice||0).toFixed(3),"Dispon. 2055 swl2":+(ptD[ts]?.proj?.["2055_swl2"]||0).toFixed(3)}));
    const pobBar=vts.map(ts=>({name:sh(ts),full:ts,"Pobreza Energ.":+(ptA[ts]?.pobreza_en||0).toFixed(3)}));
    return <div style={{display:"flex",flexDirection:"column",gap:16}}>
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
        <KPI label="Acesso à Energia" val={avg(ptA,"indice")} color={VUL_COLORS.energia}/>
        <KPI label="Disponibilidade de Energia" val={avg(ptD,"indice")} color={VUL_COLORS.desastres}/>
        <KPI label="Pobreza Energética" val={avg(ptA,"pobreza_en")} color={VUL_COLORS.saude}/>
        <KPI label="Ameaça Climática" val={avg(ptA,"ameaca")} color={VUL_COLORS.alimentar}/>
      </div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
        <VBox title="Acesso vs Disponibilidade de Energia por TSBio">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={barComp} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
              <Bar dataKey="Acesso Energia" fill={VUL_COLORS.energia} radius={[3,3,0,0]}/>
              <Bar dataKey="Disponibilidade" fill={VUL_COLORS.desastres} radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
        <VBox title="Potencial de Energias Renováveis por TSBio">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={potBar} margin={{left:-10}}>
              <CartesianGrid strokeDasharray="3 3" vertical={false}/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
              <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
              <Bar dataKey="Solar" fill="#D4A843" radius={[3,3,0,0]}/>
              <Bar dataKey="Eólico" fill="#2E86AB" radius={[3,3,0,0]}/>
              <Bar dataKey="Hidrelétrico" fill="#1565C0" radius={[3,3,0,0]}/>
            </BarChart>
          </ResponsiveContainer>
        </VBox>
      </div>
      <VBox title="Projeção 2055 (swl2 +2°C): Acesso e Disponibilidade de Energia">
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={projBar} margin={{left:-10}}>
            <CartesianGrid strokeDasharray="3 3" vertical={false}/>
            <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis domain={[0,1]} tick={{fontSize:9}}/>
            <Tooltip formatter={(v)=>v?.toFixed(3)}/><Legend/>
            <Bar dataKey="Acesso Base." fill={VUL_COLORS.energia} radius={[3,3,0,0]} opacity={0.6}/>
            <Bar dataKey="Acesso 2055 swl2" fill={VUL_COLORS.saude} radius={[3,3,0,0]}/>
            <Bar dataKey="Dispon. Base." fill={VUL_COLORS.desastres} radius={[3,3,0,0]} opacity={0.6}/>
            <Bar dataKey="Dispon. 2055 swl2" fill={VUL_COLORS.alimentar} radius={[3,3,0,0]}/>
          </BarChart>
        </ResponsiveContainer>
      </VBox>
    </div>;
  };

  // ═══════════════════════════════════════════════════════
  // POLÍTICAS PÚBLICAS RENDERS
  // ═══════════════════════════════════════════════════════

  const renderPolProg=()=>{
    if(!polData)return null;
    const prog=polData.programas;
    const pt=prog.por_tsbio||{};
    const sel_ts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const KPI=({label,val,unit="",color=POL_COLORS.prog})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:24,fontWeight:800,color}}>{val!=null?val+(unit?" "+unit:""):"—"}</div>
      </div>
    );
    const totBF=sel_ts.reduce((s,ts)=>s+(pt[ts]?.bf_familias||0),0);
    const totCad=sel_ts.reduce((s,ts)=>s+(pt[ts]?.cadunico_familias||0),0);
    const totPNAE=sel_ts.reduce((s,ts)=>s+(pt[ts]?.pnae_valor_total||0),0);
    const avgIVCAD=sel_ts.length?+(sel_ts.reduce((s,ts)=>s+(pt[ts]?.cadunico_ivcad||0),0)/sel_ts.length).toFixed(3):null;
    const bfBar=sel_ts.map(ts=>({name:sh(ts),familias:Math.round((pt[ts]?.bf_familias||0)/1000)}));
    const cadBar=sel_ts.map(ts=>({name:sh(ts),"Obs.CadÚnico":Math.round((pt[ts]?.cadunico_familias||0)/1000),"SAGI":Math.round((pt[ts]?.sagi_familias||0)/1000)}));
    const ivcadBar=sel_ts.map(ts=>({name:sh(ts),ivcad:+(pt[ts]?.cadunico_ivcad||0).toFixed(3)}));
    const paaBar=sel_ts.map(ts=>({name:sh(ts),agricultores:pt[ts]?.paa_agricultores||null}));
    const sfBF=prog.series_bf||{};
    const bfAllAnos=sfBF.anos||[];
    const bfAnos=bfAllAnos.filter(a=>a>=2019);
    const bfSeries=sfBF.por_tsbio||{};
    const bfLineData=bfAnos.map(a=>({ano:a,...Object.fromEntries(sel_ts.map(ts=>[sh(ts),(bfSeries[ts]||[])[bfAllAnos.indexOf(a)]]))}));
    const sfPN=prog.series_pnae||{};
    const pnAllAnos=sfPN.anos||[];
    const pnAnos=pnAllAnos.filter(a=>a>=2015);
    const pnSeries=sfPN.por_tsbio||{};
    const pnLineData=pnAnos.map(a=>({ano:a,...Object.fromEntries(sel_ts.map(ts=>[sh(ts),+((((pnSeries[ts]||[])[pnAllAnos.indexOf(a)])||0)/1e6).toFixed(1)]))}));
    return(
      <div style={{display:"flex",flexDirection:"column",gap:18}}>
        <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
          <KPI label="Famílias Bolsa Família" val={totBF>0?(totBF/1000).toFixed(0)+"k":null} color={POL_COLORS.prog}/>
          <KPI label="Famílias no CadÚnico" val={totCad>0?(totCad/1000).toFixed(0)+"k":null} color={T.orange}/>
          <KPI label="PNAE Acumulado" val={totPNAE>0?"R$"+(totPNAE/1e9).toFixed(1)+"Bi":null} color={T.green}/>
          <KPI label="IVCad Médio" val={avgIVCAD} color={T.purple}/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Famílias Bolsa Família por Território (mil)" fonte="GOV Dados Abertos" periodo="Último ano disponível">
            <ResponsiveContainer width="100%" height={220}><BarChart data={bfBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="familias" name="Famílias (mil)" fill={POL_COLORS.prog} radius={[3,3,0,0]}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Famílias no Cadastro Único por Território (mil)" fonte="SAGI / Observatório CadÚnico" periodo="2025/2026">
            <ResponsiveContainer width="100%" height={220}><BarChart data={cadBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Obs.CadÚnico" fill={T.orange} radius={[3,3,0,0]}/>
              <Bar dataKey="SAGI" fill={T.blue} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Evolução Bolsa Família — Famílias (2019–)" fonte="GOV Dados Abertos">
            <ResponsiveContainer width="100%" height={220}><LineChart data={bfLineData} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="ano" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              {sel_ts.map(ts=><Line key={ts} type="monotone" dataKey={sh(ts)} name={ts} stroke={TSBIO_COLORS[ts]||T.green} dot={false} strokeWidth={2}/>)}
              <Legend wrapperStyle={{fontSize:10}}/>
            </LineChart></ResponsiveContainer>
          </CC>
          <CC title="PNAE — Recursos Repassados por Território (R$ Mi)" fonte="FNDE" periodo="2015–2022">
            <ResponsiveContainer width="100%" height={220}><LineChart data={pnLineData} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="ano" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              {sel_ts.map(ts=><Line key={ts} type="monotone" dataKey={sh(ts)} name={ts} stroke={TSBIO_COLORS[ts]||T.green} dot={false} strokeWidth={2}/>)}
              <Legend wrapperStyle={{fontSize:10}}/>
            </LineChart></ResponsiveContainer>
          </CC>
          <CC title="PAA — Agricultores Beneficiados por Território" fonte="SESAN 2026">
            <ResponsiveContainer width="100%" height={200}><BarChart data={paaBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="agricultores" name="Agricultores" fill={T.green} radius={[3,3,0,0]}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="IVCad — Índice de Vulnerabilidade Social por Território" fonte="Observatório CadÚnico 2025">
            <ResponsiveContainer width="100%" height={200}><BarChart data={ivcadBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}} domain={[0,0.6]}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="ivcad" name="IVCad (0–1)" fill={T.red} radius={[3,3,0,0]}/>
            </BarChart></ResponsiveContainer>
          </CC>
        </div>
      </div>
    );
  };

  const renderPolSocio=()=>{
    if(!polData)return null;
    const sb=polData.sociobio;
    const st=sb.por_tsbio||{};
    const sel_ts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const KPI=({label,val,unit="",color=POL_COLORS.sociobio})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:24,fontWeight:800,color}}>{val!=null?val+(unit?" "+unit:""):"—"}</div>
      </div>
    );
    const totKg=sel_ts.reduce((s,ts)=>s+(st[ts]?.sociobio_kg||0),0);
    const totContr=sel_ts.reduce((s,ts)=>s+(st[ts]?.sicor_contratos||0),0);
    const totProd=sel_ts.reduce((s,ts)=>s+(st[ts]?.mapa_prod_ativos||0)+(st[ts]?.sigorg_produtores||0),0);
    const totCadsol=sel_ts.reduce((s,ts)=>s+(st[ts]?.cadsol_empreend||0),0);
    const kgBar=sel_ts.map(ts=>({name:sh(ts),"Sociobio (ton)":+((st[ts]?.sociobio_kg||0)/1000).toFixed(1),"Subvencao (ton)":+((st[ts]?.subvencao_kg||0)/1000).toFixed(1)}));
    const sicorBar=sel_ts.map(ts=>({name:sh(ts),contratos:st[ts]?.sicor_contratos||null,"Area mil ha":+((st[ts]?.sicor_area_ha||0)/1000).toFixed(1)}));
    const prodBar=sel_ts.map(ts=>({name:sh(ts),"MAPA":st[ts]?.mapa_prod_ativos||null,"SIGORG":st[ts]?.sigorg_produtores||null,"CADSOL":st[ts]?.cadsol_empreend||null}));
    const topProd=(sb.top_produtos||[]).slice(0,10);
    const rankKg=(sb.ranking_kg||[]).filter(m=>selT.length===6||selT.includes(m.tsbio)).slice(0,10);
    return(
      <div style={{display:"flex",flexDirection:"column",gap:18}}>
        <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
          <KPI label="Sociobio CONAB (ton)" val={totKg>0?(totKg/1000).toFixed(1):null} color={POL_COLORS.sociobio}/>
          <KPI label="Contratos SICOR" val={totContr||null} color={T.blue}/>
          <KPI label="Produtores Organicos" val={totProd||null} color={T.green}/>
          <KPI label="Empreend. Solidarios" val={totCadsol||null} color={T.orange}/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Sociobiodiversidade CONAB — Volume (ton)" fonte="CONAB 2025" periodo="Acumulado">
            <ResponsiveContainer width="100%" height={220}><BarChart data={kgBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Sociobio (ton)" fill={POL_COLORS.sociobio} radius={[3,3,0,0]}/>
              <Bar dataKey="Subvencao (ton)" fill={T.gold} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Credito Rural SICOR — Contratos e Area" fonte="SICOR 2025" periodo="Acumulado">
            <ResponsiveContainer width="100%" height={220}><BarChart data={sicorBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="contratos" name="Contratos" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="Area mil ha" name="Area (mil ha)" fill={T.green} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Producao Organica e Economia Solidaria" fonte="MAPA / SIGORG / CADSOL 2026">
            <ResponsiveContainer width="100%" height={220}><BarChart data={prodBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="MAPA" fill={T.green} radius={[3,3,0,0]}/>
              <Bar dataKey="SIGORG" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="CADSOL" fill={T.orange} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Top 10 Produtos da Sociobiodiversidade (volume)" fonte="CONAB 2025">
            <div style={{height:220,overflowY:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                <thead><tr style={{background:"#f5f7f5",position:"sticky",top:0}}>
                  <th style={{padding:"4px 6px",textAlign:"left",fontWeight:700}}>Produto</th>
                  <th style={{padding:"4px 6px",textAlign:"right",fontWeight:700}}>Volume (kg)</th>
                  <th style={{padding:"4px 6px",textAlign:"right",fontWeight:700}}>Municipios</th>
                </tr></thead>
                <tbody>{topProd.map((p,i)=>(
                  <tr key={i} style={{borderBottom:"1px solid #f0f2f0",background:i%2?"#fafafa":"#fff"}}>
                    <td style={{padding:"4px 6px"}}>{p.produto}</td>
                    <td style={{padding:"4px 6px",textAlign:"right"}}>{(p.kg||0).toLocaleString("pt-BR")}</td>
                    <td style={{padding:"4px 6px",textAlign:"right"}}>{p.n_municipios}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </CC>
        </div>
        <CC title="Ranking Municipios — Sociobiodiversidade CONAB (top 10 por volume)" fonte="CONAB 2025">
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><tr style={{background:"#f5f7f5"}}>
                <th style={{padding:"5px 8px",textAlign:"left",fontWeight:700}}>Municipio</th>
                <th style={{padding:"5px 8px",textAlign:"left",fontWeight:700}}>TSBio</th>
                <th style={{padding:"5px 8px",textAlign:"right",fontWeight:700}}>Volume (kg)</th>
                <th style={{padding:"5px 8px",textAlign:"right",fontWeight:700}}>Valor (R$)</th>
              </tr></thead>
              <tbody>{rankKg.map((m,i)=>(
                <tr key={i} style={{borderBottom:"1px solid #f0f2f0",background:i%2?"#fafafa":"#fff"}}>
                  <td style={{padding:"5px 8px",fontWeight:500}}>{m.nome}</td>
                  <td style={{padding:"5px 8px",color:T.textLight}}>{m.tsbio}</td>
                  <td style={{padding:"5px 8px",textAlign:"right"}}>{(m.kg||0).toLocaleString("pt-BR")}</td>
                  <td style={{padding:"5px 8px",textAlign:"right"}}>R$ {(m.valor||0).toLocaleString("pt-BR",{maximumFractionDigits:0})}</td>
                </tr>
              ))}</tbody>
            </table>
          </div>
        </CC>
      </div>
    );
  };

  const renderPolGov=()=>{
    if(!polData)return null;
    const gv=polData.governanca;
    const gt=gv.por_tsbio||{};
    const sel_ts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const KPI=({label,val,unit="",color=POL_COLORS.gov})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:24,fontWeight:800,color}}>{val!=null?val+(unit?" "+unit:""):"—"}</div>
      </div>
    );
    const n_total=sel_ts.reduce((s,ts)=>s+(gt[ts]?.n||0),0);
    const totHab=sel_ts.reduce((s,ts)=>s+(gt[ts]?.hab_plano||0),0);
    const totLai=sel_ts.reduce((s,ts)=>s+(gt[ts]?.gov_lai||0),0);
    const totIR=sel_ts.reduce((s,ts)=>s+(gt[ts]?.ir_conselho||0),0);
    const totRH=sel_ts.reduce((s,ts)=>s+(gt[ts]?.rh_total||0),0);
    const habBar=sel_ts.map(ts=>({name:sh(ts),Plano:gt[ts]?.hab_plano||0,Conselho:gt[ts]?.hab_conselho||0,Fundo:gt[ts]?.hab_fundo||0}));
    const govBar=sel_ts.map(ts=>({name:sh(ts),"Lei AI":gt[ts]?.gov_lai||0,"Cons.Transp":gt[ts]?.gov_transp||0,"Portal":gt[ts]?.gov_portal||0,"Diario":gt[ts]?.gov_diario||0}));
    const irBar=sel_ts.map(ts=>({name:sh(ts),Conselho:gt[ts]?.ir_conselho||0,Plano:gt[ts]?.ir_plano||0,"Prog.Ind":gt[ts]?.ir_prog_indigena||0,"Prog.Qui":gt[ts]?.ir_prog_quilombola||0}));
    const rhBar=sel_ts.map(ts=>({name:sh(ts),Estatutarios:Math.round((gt[ts]?.rh_estat||0)/1000),Celetistas:Math.round((gt[ts]?.rh_celet||0)/1000),Comissionados:Math.round((gt[ts]?.rh_comiss||0)/1000)}));
    return(
      <div style={{display:"flex",flexDirection:"column",gap:18}}>
        <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
          <KPI label="Mun. c/ Plano Habitacao" val={`${totHab} de ${n_total}`} color={POL_COLORS.gov}/>
          <KPI label="Mun. c/ Lei Acesso Info." val={`${totLai} de ${n_total}`} color={T.blue}/>
          <KPI label="Mun. c/ Conselho IR" val={`${totIR} de ${n_total}`} color={T.purple}/>
          <KPI label="Total RH Municipal" val={totRH>0?(totRH/1000).toFixed(0)+"k":null} color={T.orange}/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Politica de Habitacao — Municipios por Territorio" fonte="IBGE MUNIC 2024">
            <ResponsiveContainer width="100%" height={220}><BarChart data={habBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Plano" fill={POL_COLORS.gov} radius={[3,3,0,0]}/>
              <Bar dataKey="Conselho" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="Fundo" fill={T.green} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Governanca e Transparencia — Municipios" fonte="IBGE MUNIC 2024">
            <ResponsiveContainer width="100%" height={220}><BarChart data={govBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Lei AI" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="Cons.Transp" fill={POL_COLORS.gov} radius={[3,3,0,0]}/>
              <Bar dataKey="Portal" fill={T.green} radius={[3,3,0,0]}/>
              <Bar dataKey="Diario" fill={T.gold} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Igualdade Racial — Orgaos e Politicas" fonte="IBGE MUNIC 2024">
            <ResponsiveContainer width="100%" height={220}><BarChart data={irBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Conselho" fill={T.purple} radius={[3,3,0,0]}/>
              <Bar dataKey="Plano" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="Prog.Ind" name="Prog. Indigenas" fill={T.orange} radius={[3,3,0,0]}/>
              <Bar dataKey="Prog.Qui" name="Prog. Quilombola" fill={T.red} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Recursos Humanos — Servidores por Territorio (mil)" fonte="IBGE MUNIC 2024">
            <ResponsiveContainer width="100%" height={220}><BarChart data={rhBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Estatutarios" fill={T.green} radius={[3,3,0,0]}/>
              <Bar dataKey="Celetistas" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="Comissionados" fill={T.orange} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
        </div>
      </div>
    );
  };

  const renderPolPovos=()=>{
    if(!polData)return null;
    const qui=polData.povos.quilombola;
    const ind=polData.povos.indigena;
    const qt=qui.por_tsbio||{};
    const it=ind.por_tsbio||{};
    const sel_ts=TSBIO_LIST.filter(ts=>selT.includes(ts));
    const KPI=({label,val,unit="",color=POL_COLORS.indigena})=>(
      <div style={{background:"#fff",borderRadius:12,padding:"14px 18px",boxShadow:"0 1px 6px rgba(0,0,0,0.05)",border:`1px solid ${T.border}`,flex:1,minWidth:150}}>
        <div style={{fontSize:11,color:T.textLight,fontWeight:600,marginBottom:6}}>{label}</div>
        <div style={{fontSize:24,fontWeight:800,color}}>{val!=null?val+(unit?" "+unit:""):"—"}</div>
      </div>
    );
    const totInd=sel_ts.reduce((s,ts)=>s+(it[ts]?.pop_indigena||0),0);
    const totQui=sel_ts.reduce((s,ts)=>s+(qt[ts]?.pop_quilombola||0),0);
    const nMunInd=(ind.municipios_com_pop||[]).filter(m=>selT.includes(m.tsbio)).length;
    const nMunQui=(qui.municipios_com_pop||[]).filter(m=>selT.includes(m.tsbio)).length;
    const popBar=sel_ts.map(ts=>({name:sh(ts),"Ind.(mil)":+((it[ts]?.pop_indigena||0)/1000).toFixed(1),"Quilombolas":qt[ts]?.pop_quilombola||0}));
    const locBar=sel_ts.map(ts=>({name:sh(ts),"Localid.Ind":it[ts]?.n_localidades||null,"Etnias":it[ts]?.n_etnias||null,"Localid.Qui":qt[ts]?.n_localidades||null}));
    const indCharBar=sel_ts.map(ts=>({name:sh(ts),"Alfab":it[ts]?.alfab_pct||null,"Agua rede":it[ts]?.agua_rede_pct||null,"Coleta":it[ts]?.coleta_lixo_pct||null,"Sem ban":it[ts]?.sem_banheiro_pct||null}));
    const quiCharBar=sel_ts.map(ts=>({name:sh(ts),"Alfab":qt[ts]?.alfab_pct||null,"Agua rede":qt[ts]?.agua_rede_pct||null,"Sem ban":qt[ts]?.sem_banheiro_pct||null}));
    const topMunInd=(ind.municipios_com_pop||[]).filter(m=>selT.includes(m.tsbio)).slice(0,10);
    const topMunQui=(qui.municipios_com_pop||[]).filter(m=>selT.includes(m.tsbio));
    return(
      <div style={{display:"flex",flexDirection:"column",gap:18}}>
        <div style={{display:"flex",gap:12,flexWrap:"wrap"}}>
          <KPI label="Pop. Indigena" val={totInd>0?(totInd/1000).toFixed(1)+"k":null} color={POL_COLORS.indigena}/>
          <KPI label="Pop. Quilombola" val={totQui>0?totQui.toLocaleString("pt-BR"):null} color={POL_COLORS.quilombola}/>
          <KPI label="Mun. c/ Indigenas" val={nMunInd} unit="municipios" color={T.orange}/>
          <KPI label="Mun. c/ Quilombolas" val={nMunQui} unit="municipios" color={T.purple}/>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Populacoes Tradicionais por Territorio" fonte="Censo 2022">
            <ResponsiveContainer width="100%" height={220}><BarChart data={popBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Ind.(mil)" fill={POL_COLORS.indigena} radius={[3,3,0,0]}/>
              <Bar dataKey="Quilombolas" fill={POL_COLORS.quilombola} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Localidades Indigenas e Etnias por Territorio" fonte="Censo 2022">
            <ResponsiveContainer width="100%" height={220}><BarChart data={locBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Localid.Ind" name="Localid. Indigenas" fill={POL_COLORS.indigena} radius={[3,3,0,0]}/>
              <Bar dataKey="Etnias" fill={T.gold} radius={[3,3,0,0]}/>
              <Bar dataKey="Localid.Qui" name="Localid. Quilombola" fill={POL_COLORS.quilombola} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Condicoes dos Domicilios Indigenas (%)" fonte="Censo 2022">
            <ResponsiveContainer width="100%" height={220}><BarChart data={indCharBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}} domain={[0,100]}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Alfab" name="Alfabetizacao" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="Agua rede" fill={T.green} radius={[3,3,0,0]}/>
              <Bar dataKey="Coleta" name="Coleta lixo" fill={T.orange} radius={[3,3,0,0]}/>
              <Bar dataKey="Sem ban" name="Sem banheiro" fill={T.red} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
          <CC title="Condicoes dos Domicilios Quilombolas (%)" fonte="Censo 2022">
            <ResponsiveContainer width="100%" height={220}><BarChart data={quiCharBar} margin={{top:4,right:12,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0"/>
              <XAxis dataKey="name" tick={{fontSize:10}}/><YAxis tick={{fontSize:10}} domain={[0,100]}/>
              <Tooltip content={<CTip/>}/>
              <Bar dataKey="Alfab" name="Alfabetizacao" fill={T.blue} radius={[3,3,0,0]}/>
              <Bar dataKey="Agua rede" fill={T.green} radius={[3,3,0,0]}/>
              <Bar dataKey="Sem ban" name="Sem banheiro" fill={T.red} radius={[3,3,0,0]}/>
              <Legend wrapperStyle={{fontSize:10}}/>
            </BarChart></ResponsiveContainer>
          </CC>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:18}}>
          <CC title="Top 10 Municipios — Pop. Indigena" fonte="Censo 2022">
            <div style={{height:200,overflowY:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                <thead><tr style={{background:"#f5f7f5",position:"sticky",top:0}}>
                  <th style={{padding:"4px 6px",textAlign:"left",fontWeight:700}}>Municipio</th>
                  <th style={{padding:"4px 6px",textAlign:"right",fontWeight:700}}>Pop. Indigena</th>
                  <th style={{padding:"4px 6px",textAlign:"right",fontWeight:700}}>% no mun.</th>
                </tr></thead>
                <tbody>{topMunInd.map((m,i)=>(
                  <tr key={i} style={{borderBottom:"1px solid #f0f2f0",background:i%2?"#fafafa":"#fff"}}>
                    <td style={{padding:"4px 6px",fontWeight:500}}>{m.nome}</td>
                    <td style={{padding:"4px 6px",textAlign:"right"}}>{(m.pop||0).toLocaleString("pt-BR")}</td>
                    <td style={{padding:"4px 6px",textAlign:"right"}}>{m.pct!=null?m.pct.toFixed(1)+"%":"—"}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </CC>
          <CC title="Municipios com Comunidades Quilombolas" fonte="Censo 2022">
            <div style={{height:200,overflowY:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                <thead><tr style={{background:"#f5f7f5",position:"sticky",top:0}}>
                  <th style={{padding:"4px 6px",textAlign:"left",fontWeight:700}}>Municipio</th>
                  <th style={{padding:"4px 6px",textAlign:"right",fontWeight:700}}>Pop. Quilombola</th>
                  <th style={{padding:"4px 6px",textAlign:"right",fontWeight:700}}>% na pop.</th>
                </tr></thead>
                <tbody>
                  {topMunQui.length>0?topMunQui.map((m,i)=>(
                    <tr key={i} style={{borderBottom:"1px solid #f0f2f0",background:i%2?"#fafafa":"#fff"}}>
                      <td style={{padding:"4px 6px",fontWeight:500}}>{m.nome}</td>
                      <td style={{padding:"4px 6px",textAlign:"right"}}>{(m.pop||0).toLocaleString("pt-BR")}</td>
                      <td style={{padding:"4px 6px",textAlign:"right"}}>{m.pct!=null?m.pct.toFixed(2)+"%":"—"}</td>
                    </tr>
                  )):<tr><td colSpan={3} style={{padding:"20px",textAlign:"center",color:T.textLight}}>Nenhum municipio com quilombola neste filtro</td></tr>}
                </tbody>
              </table>
            </div>
          </CC>
        </div>
      </div>
    );
  };

  if(loadErr) return (
    <div style={{minHeight:"100vh",background:T.bg,display:"flex",alignItems:"center",justifyContent:"center",fontFamily:"'DM Sans','Segoe UI',sans-serif"}}>
      <div style={{textAlign:"center",color:T.textLight}}>
        <div style={{fontSize:44,marginBottom:16}}>⚠️</div>
        <h2 style={{fontSize:18,fontWeight:700,color:T.red,margin:"0 0 8px"}}>Erro ao carregar dados</h2>
        <p style={{fontSize:13,margin:0,fontFamily:"monospace"}}>{loadErr}</p>
      </div>
    </div>
  );

  return <div style={{minHeight:"100vh",background:`linear-gradient(175deg,#f4f7f4 0%,#eaf0ea 50%,#f0f4f0 100%)`,fontFamily:"'DM Sans','Segoe UI',sans-serif"}}>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&family=Playfair+Display:wght@700;800&display=swap" rel="stylesheet"/>

    {/* HEADER */}
    <header style={{background:`linear-gradient(135deg,${T.greenDark} 0%,${T.green} 40%,#14652f 100%)`,padding:"20px 24px 16px",color:"#fff"}}>
      <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",flexWrap:"wrap",gap:10}}>
        <div>
          <div style={{fontSize:9,fontWeight:700,textTransform:"uppercase",letterSpacing:2.5,opacity:.7,marginBottom:4}}>Projeto Sociobioeconomia na Amazônia</div>
          <h1 style={{margin:0,fontSize:24,fontWeight:800,fontFamily:"'Playfair Display',serif"}}>Dashboard TSBio</h1>
          <p style={{margin:"4px 0 0",fontSize:12,opacity:.7}}>Diagnóstico territorial · 6 TSBio · {D.length} municípios · Dados reais via ETL</p>
        </div>
        <div style={{display:"flex",gap:6,flexWrap:"wrap"}}>
          {Object.entries(TSBIO_COLORS).map(([n,c])=><span key={n} style={{display:"inline-flex",alignItems:"center",gap:4,padding:"2px 8px",borderRadius:16,background:"rgba(255,255,255,0.1)",fontSize:10,fontWeight:600}}><span style={{width:7,height:7,borderRadius:7,background:c}}/>{n}</span>)}
        </div>
      </div>
    </header>

    {/* FILTERS */}
    <div style={{background:"#fff",borderBottom:`1px solid ${T.border}`,padding:"10px 24px",display:"flex",gap:16,flexWrap:"wrap",alignItems:"flex-end"}}>
      <MultiSel label="Território TSBio" opts={TSBIO_LIST.map(t=>({value:t,label:t}))} sel={selT} onChange={setSelT} cmap={TSBIO_COLORS}/>
      <MultiSel label="Município" opts={mOpts} sel={selM} onChange={setSelM}/>
      <div style={{fontSize:11,color:T.textLight,padding:"0 0 5px"}}>{selT.length} territórios · {selM.length>0?`${selM.length} municípios selecionados`:`${fM.length} municípios`}</div>
    </div>

    {/* DIMENSION TABS */}
    <div style={{background:"#fff",borderBottom:`2px solid ${T.border}`,padding:"0 24px",display:"flex",gap:0,overflowX:"auto"}}>
      {DIMS.map(d=><button key={d.id} onClick={()=>{setDim(d.id);if(d.id==="socio")setSub("demo");if(d.id==="amb")setSub("desm");if(d.id==="prod")setSub("agro");if(d.id==="infra")setSub("san");if(d.id==="pol")setSub("prog");if(d.id==="vuln")setSub("over")}} style={{padding:"10px 14px",border:"none",borderBottom:dim===d.id?`3px solid ${T.green}`:"3px solid transparent",background:dim===d.id?"rgba(27,122,61,0.04)":"transparent",color:dim===d.id?T.green:d.on?"#4a5a4a":"#b0b8b0",fontSize:12,fontWeight:dim===d.id?700:500,cursor:"pointer",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:5,fontFamily:"inherit",transition:"all 0.15s"}}>
        <span style={{fontSize:14}}>{d.i}</span>{d.l}
        {!d.on&&<span style={{fontSize:8,background:"#e8ebe8",color:"#8a9a8a",padding:"1px 5px",borderRadius:6,fontWeight:600}}>EM BREVE</span>}
      </button>)}
    </div>

    {/* SUB-TABS (socio) */}
    {dim==="socio"&&<div style={{background:"#fafbfa",borderBottom:`1px solid ${T.border}`,padding:"0 24px",display:"flex",gap:0,overflowX:"auto"}}>
      {SUBTABS.map(s=><button key={s.id} onClick={()=>setSub(s.id)} style={{padding:"8px 14px",border:"none",borderBottom:sub===s.id?`2px solid ${T.blue}`:"2px solid transparent",background:sub===s.id?"rgba(46,134,171,0.04)":"transparent",color:sub===s.id?T.blue:"#6a7a6a",fontSize:11.5,fontWeight:sub===s.id?700:500,cursor:"pointer",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:4,fontFamily:"inherit",transition:"all 0.15s"}}>
        <span style={{fontSize:13}}>{s.i}</span>{s.l}
      </button>)}
    </div>}
    {/* SUB-TABS (amb) */}
    {dim==="amb"&&<div style={{background:"#fafbfa",borderBottom:`1px solid ${T.border}`,padding:"0 24px",display:"flex",gap:0,overflowX:"auto"}}>
      {SUBTABS_AMB.map(s=><button key={s.id} onClick={()=>setSub(s.id)} style={{padding:"8px 14px",border:"none",borderBottom:sub===s.id?`2px solid ${T.blue}`:"2px solid transparent",background:sub===s.id?"rgba(46,134,171,0.04)":"transparent",color:sub===s.id?T.blue:"#6a7a6a",fontSize:11.5,fontWeight:sub===s.id?700:500,cursor:"pointer",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:4,fontFamily:"inherit",transition:"all 0.15s"}}>
        <span style={{fontSize:13}}>{s.i}</span>{s.l}
      </button>)}
    </div>}
    {/* SUB-TABS (prod) */}
    {dim==="prod"&&<div style={{background:"#fafbfa",borderBottom:`1px solid ${T.border}`,padding:"0 24px",display:"flex",gap:0,overflowX:"auto"}}>
      {SUBTABS_PROD.map(s=><button key={s.id} onClick={()=>setSub(s.id)} style={{padding:"8px 14px",border:"none",borderBottom:sub===s.id?`2px solid ${T.blue}`:"2px solid transparent",background:sub===s.id?"rgba(46,134,171,0.04)":"transparent",color:sub===s.id?T.blue:"#6a7a6a",fontSize:11.5,fontWeight:sub===s.id?700:500,cursor:"pointer",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:4,fontFamily:"inherit",transition:"all 0.15s"}}>
        <span style={{fontSize:13}}>{s.i}</span>{s.l}
      </button>)}
    </div>}
    {/* SUB-TABS (infra) */}
    {dim==="infra"&&<div style={{background:"#fafbfa",borderBottom:`1px solid ${T.border}`,padding:"0 24px",display:"flex",gap:0,overflowX:"auto"}}>
      {SUBTABS_INFRA.map(s=><button key={s.id} onClick={()=>setSub(s.id)} style={{padding:"8px 14px",border:"none",borderBottom:sub===s.id?`2px solid ${T.blue}`:"2px solid transparent",background:sub===s.id?"rgba(46,134,171,0.04)":"transparent",color:sub===s.id?T.blue:"#6a7a6a",fontSize:11.5,fontWeight:sub===s.id?700:500,cursor:"pointer",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:4,fontFamily:"inherit",transition:"all 0.15s"}}>
        <span style={{fontSize:13}}>{s.i}</span>{s.l}
      </button>)}
    </div>}
    {/* SUB-TABS (pol) */}
    {dim==="pol"&&<div style={{background:"#fafbfa",borderBottom:`1px solid ${T.border}`,padding:"0 24px",display:"flex",gap:0,overflowX:"auto"}}>
      {SUBTABS_POL.map(s=><button key={s.id} onClick={()=>setSub(s.id)} style={{padding:"8px 14px",border:"none",borderBottom:sub===s.id?`2px solid ${T.blue}`:"2px solid transparent",background:sub===s.id?"rgba(46,134,171,0.04)":"transparent",color:sub===s.id?T.blue:"#6a7a6a",fontSize:11.5,fontWeight:sub===s.id?700:500,cursor:"pointer",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:4,fontFamily:"inherit",transition:"all 0.15s"}}>
        <span style={{fontSize:13}}>{s.i}</span>{s.l}
      </button>)}
    </div>}
    {/* SUB-TABS (vuln) */}
    {dim==="vuln"&&<div style={{background:"#fafbfa",borderBottom:`1px solid ${T.border}`,padding:"0 24px",display:"flex",gap:0,overflowX:"auto"}}>
      {SUBTABS_VUL.map(s=><button key={s.id} onClick={()=>setSub(s.id)} style={{padding:"8px 14px",border:"none",borderBottom:sub===s.id?`2px solid ${VUL_COLORS.saude}`:"2px solid transparent",background:sub===s.id?"rgba(196,52,45,0.04)":"transparent",color:sub===s.id?VUL_COLORS.saude:"#6a7a6a",fontSize:11.5,fontWeight:sub===s.id?700:500,cursor:"pointer",whiteSpace:"nowrap",display:"flex",alignItems:"center",gap:4,fontFamily:"inherit",transition:"all 0.15s"}}>
        <span style={{fontSize:13}}>{s.i}</span>{s.l}
      </button>)}
    </div>}

    {/* CONTENT */}
    <main style={{padding:"18px 24px 40px",maxWidth:1380,margin:"0 auto"}}>
      {dim==="socio"&&sub==="demo"&&renderDemo()}
      {dim==="socio"&&sub==="econ"&&renderEcon()}
      {dim==="socio"&&sub==="edu"&&renderEdu()}
      {dim==="socio"&&sub==="trab"&&renderTrab()}
      {dim==="socio"&&sub==="povos"&&renderPovos()}
      {dim==="socio"&&sub==="dom"&&renderDom()}
      {dim==="amb"&&sub==="desm"&&renderAmDesm()}
      {dim==="amb"&&sub==="lulc"&&renderAmLULC()}
      {dim==="amb"&&sub==="fund"&&renderAmFund()}
      {dim==="prod"&&sub==="agro"&&renderProdAgro()}
      {dim==="prod"&&sub==="pam"&&renderProdPAM()}
      {dim==="prod"&&sub==="pecu"&&renderProdPecu()}
      {dim==="prod"&&sub==="coop"&&renderProdCoop()}
      {dim==="infra"&&sub==="san"&&renderInfraSan()}
      {dim==="infra"&&sub==="entor"&&renderInfraEntor()}
      {dim==="infra"&&sub==="trans"&&renderInfraTrans()}
      {dim==="pol"&&sub==="prog"&&renderPolProg()}
      {dim==="pol"&&sub==="socio"&&renderPolSocio()}
      {dim==="pol"&&sub==="gov"&&renderPolGov()}
      {dim==="pol"&&sub==="povos"&&renderPolPovos()}
      {dim==="vuln"&&sub==="over"&&renderVulOverview()}
      {dim==="vuln"&&sub==="saude"&&renderVulSaude()}
      {dim==="vuln"&&sub==="alim"&&renderVulAlimentar()}
      {dim==="vuln"&&sub==="desast"&&renderVulDesastres()}
      {dim==="vuln"&&sub==="hidrico"&&renderVulHidrico()}
      {dim==="vuln"&&sub==="biodiv"&&renderVulBiodiv()}
      {dim==="vuln"&&sub==="energ"&&renderVulEnergia()}
      {dim!=="socio"&&dim!=="amb"&&dim!=="prod"&&dim!=="infra"&&dim!=="pol"&&dim!=="vuln"&&renderDimPH(dim)}
    </main>

    {/* FOOTER */}
    <footer style={{background:T.greenDark,color:"rgba(255,255,255,0.55)",padding:"14px 24px",fontSize:10,display:"flex",justifyContent:"space-between",flexWrap:"wrap",gap:6}}>
      <div>FAS — Fundação Amazônia Sustentável · MMA-DPEB · Oportunidade Nº 201/2025</div>
      <div>Dados reais via ETL · v0.3 · Fontes: Censo 2022, Atlas DH, IBGE PIB Municipal</div>
    </footer>
  </div>;
}
