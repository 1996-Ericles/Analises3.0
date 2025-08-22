# -*- coding: utf-8 -*-
"""
analise_completo.py
Unifica: Análise de Tickets por Analista + Análise de Demandas + Atuação
Regras principais (resumo):
- Um único arquivo/tela com filtros únicos (período, analista, status, mostrar gráficos e seleção de gráficos).
- A primeira seção mostra a tabela "Análise de Tickets por Analista" com as colunas:
  Responsável | Total de Tickets | Tickets Encerrados | Tickets em Aberto
  | Tempo médio para Encerramento (dias) | Média de Tickets Encerrados por Dia
- Interpretação de datas robusta (PT/EN) — entende "05/mai/25 4:30 PM" e equivalentes em inglês.
- Para um período selecionado:
  * Total de Tickets = quantidade de tickets CRIADOS no período.
  * Tickets Encerrados = quantidade de tickets RESOLVIDOS no período (status encerrados).
  * Tickets em Aberto = tickets CRIADOS no período que NÃO estavam resolvidos até o fim do período.
  * Tempo médio para Encerramento (dias) = média de (Resolvido - Criado) APENAS dos tickets resolvidos no período.
  * Média de Tickets Encerrados por Dia = (Tickets Encerrados no período) / (nº de dias ÚTEIS do período, segunda a sexta, incluindo zeros).
- Gráficos só aparecem quando:
  1) checkbox "Mostrar gráficos" estiver marcado; e
  2) filtro Analista = "Todos".
- Removidos: downloads de CSV e "Termos recorrentes".
"""
from __future__ import annotations
import re
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Tuple, List

import pandas as pd
import plotly.express as px
import streamlit as st

# =========================================
# Config
# =========================================
st.set_page_config(page_title="Análise Completa (Tickets + Demandas + Atuação)", layout="wide")
st.title("📈 Análise Completa: Tickets + Demandas + Atuação")

# =========================================
# Colunas possíveis (PT/EN) vindas do Jira/Sheets
# => Usamos nomes internos padronizados SEM acento
# =========================================
COLMAP = {
    "Responsavel": [
        "Responsável", "Responsavel", "Assignee", "Assignee display name",
        "Atribuído a", "Atribuido a", "Owner", "Agent", "Atendente"
    ],
    "Status": ["Status", "Status name", "State"],
    "Criado": [
        "Criado", "Created", "Data de criação", "Created date",
        "Data de criação do ticket", "Created Time", "Data de abertura"
    ],
    "Resolvido": [
        "Resolvido", "Resolved", "Resolution date", "Data de resolução",
        "Data de conclusão", "Resolved Time", "Data de fechamento", "Fechado em"
    ],
    "Projeto": ["Nome do projeto", "Project name", "Projeto", "Project"],
    "Resumo": ["Resumo", "Summary", "Assunto", "Title", "Título"],
    "Descricao": ["Descrição", "Description", "Detalhes", "Details"],
    "Aplicacao": [
        "Campo personalizado (Application/Software)",
        "Application/Software",
        "Aplicação",
        "Aplicacao",
        "Sistema",
        "App",
        "Application"
    ],
    "Tipo": ["Tipo", "Issue Type", "Tipo de solicitação", "Tipo de solicitacao", "Type"],
}

# Status considerados encerrado (inclui PT e EN)
STATUS_ENCERRADOS = {
    "Resolvido", "Fechada", "Concluído", "Cancelado",
    "Closed", "Done", "Resolved", "Canceled", "Cancelled", "Completed"
}

# Palavras para classificar Request/Incident quando não há uma coluna padronizada
PALAVRAS_REQUEST = {"request", "solicita", "requisição", "requisicao", "service request"}
PALAVRAS_INCIDENT = {"incident", "incidente"}

# =========================================
# Utilidades de Data/Hora — suporte PT/EN (meses)
# =========================================
_PT_TO_EN_MONTHS = {
    # abreviações
    r"\bjan\b": "Jan", r"\bfev\b": "Feb", r"\bmar\b": "Mar", r"\babr\b": "Apr",
    r"\bmai\b": "May", r"\bjun\b": "Jun", r"\bjul\b": "Jul", r"\bago\b": "Aug",
    r"\bset\b": "Sep", r"\bout\b": "Oct", r"\bnov\b": "Nov", r"\bdez\b": "Dec",
    # nomes completos
    r"\bjaneiro\b": "January", r"\bfevereiro\b": "February", r"\bmarço\b": "March",
    r"\bmarco\b": "March", r"\babril\b": "April", r"\bmaio\b": "May",
    r"\bjunho\b": "June", r"\bjulho\b": "July", r"\bagosto\b": "August",
    r"\bsetembro\b": "September", r"\boutubro\b": "October", r"\bnovembro\b": "November",
    r"\bdezembro\b": "December",
}

def _replace_pt_months_to_en(text: str) -> str:
    """Substitui meses PT por EN de forma case-insensitive."""
    if not isinstance(text, str):
        return text
    s = text
    for pattern, repl in _PT_TO_EN_MONTHS.items():
        s = re.sub(pattern, repl, s, flags=re.IGNORECASE)
    return s

def parse_mixed_datetime_series(s: pd.Series) -> pd.Series:
    """
    Converte série de datas que pode vir em PT ou EN.
    Aceita formatos como '05/mai/25 4:30 PM' ou '05/May/25 4:30 PM'.
    """
    if s is None or len(s) == 0:
        return pd.to_datetime(pd.Series([], dtype="object"))

    # 1) tenta direto
    out = pd.to_datetime(s, errors="coerce", dayfirst=True, infer_datetime_format=True)
    if out.notna().any() and out.isna().sum() <= (len(out) * 0.2):
        return out

    # 2) substitui meses PT->EN e tenta novamente
    s2 = s.astype(str).map(_replace_pt_months_to_en)
    out2 = pd.to_datetime(s2, errors="coerce", dayfirst=True, infer_datetime_format=True)
    if out2.notna().any():
        return out2

    # 3) tenta alguns formatos comuns manualmente
    tried = []
    for fmt in [
        "%d/%b/%y %I:%M %p", "%d/%b/%Y %I:%M %p",  # 05/May/25 4:30 PM
        "%d/%b/%y %H:%M", "%d/%b/%Y %H:%M",        # 05/May/25 16:30
        "%d/%m/%Y %H:%M", "%d/%m/%y %H:%M",        # 05/05/2025 16:30
        "%d/%m/%Y", "%d/%m/%y",                    # 05/05/2025
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",     # 2025-05-05 16:30:00
        "%Y-%m-%d",
    ]:
        try:
            parsed = pd.to_datetime(s2, format=fmt, errors="coerce")
            if parsed.notna().any():
                return parsed
        except Exception as e:
            tried.append((fmt, str(e)))

    # 4) fallback final — devolve tudo como NaT
    return pd.to_datetime(s2, errors="coerce", dayfirst=True)

# =========================================
# Utils – leitura robusta de CSV
# =========================================
def ler_csv_flex(arquivo) -> pd.DataFrame:
    """
    Leitura robusta:
    1) Detecta automaticamente separador (engine='python')
    2) Tenta ; , \t e |
    3) Varre encodings comuns
    4) Última tentativa: on_bad_lines='skip' para ignorar linhas ruins
    """
    tentativas = []
    # detecção automática
    tentativas.append(dict(sep=None, engine="python"))
    # separadores comuns
    for sep in [";", ",", "\t", "|"]:
        tentativas.append(dict(sep=sep, engine="python"))
    # ignorando linhas problemáticas
    for sep in [";", ",", "\t", "|", None]:
        tentativas.append(dict(sep=sep, engine="python", on_bad_lines="skip"))

    encodings = [None, "utf-8", "latin1", "cp1252"]

    pos = arquivo.tell()
    for enc in encodings:
        for kwargs in tentativas:
            try:
                arquivo.seek(pos)
                if enc:
                    df = pd.read_csv(arquivo, encoding=enc, **kwargs)
                else:
                    df = pd.read_csv(arquivo, **kwargs)
                if df.shape[1] > 0:
                    return df
            except Exception:
                continue
    arquivo.seek(pos)
    return pd.read_csv(arquivo, sep=",", engine="python", on_bad_lines="skip", encoding_errors="ignore")

# =========================================
# Normalização de colunas e validações
# =========================================
def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia para as chaves internas do COLMAP se encontrar equivalentes."""
    rename_map = {}
    for destino, candidatos in COLMAP.items():
        for col in candidatos:
            if col in df.columns:
                rename_map[col] = destino
                break
    return df.rename(columns=rename_map)

def validar_minimo(df: pd.DataFrame, necessarias: List[str]) -> Tuple[bool, List[str]]:
    faltando = [c for c in necessarias if c not in df.columns]
    return (len(faltando) == 0, faltando)

# =========================================
# Classificação de tipo (Request/Incident/Outro)
# =========================================
def normalizar_tipo_linha(row) -> str:
    """Determina se é Request/Incident/Outro olhando 'Tipo' e 'Projeto'."""
    texto = ""
    if "Tipo" in row and pd.notna(row["Tipo"]):
        texto += f" {str(row['Tipo'])}"
    if "Projeto" in row and pd.notna(row["Projeto"]):
        texto += f" {str(row['Projeto'])}"
    t = texto.lower()
    if any(p in t for p in PALAVRAS_INCIDENT):
        return "Incident"
    if any(p in t for p in PALAVRAS_REQUEST):
        return "Request"
    return "Outro"

# =========================================
# Filtros por período (Union de Criado OU Resolvido)
# =========================================
def period_bounds(modo: str, ano: int, mes: int, intervalo: Tuple[date, date]) -> Tuple[date, date]:
    """Retorna (início, fim) do período selecionado."""
    today = date.today()
    if modo == "Todo período":
        # Later bounds will be min/max by data; aqui retornamos um range amplo
        return date(1970, 1, 1), date(2100, 12, 31)
    if modo == "Ano":
        ini = date(ano, 1, 1)
        fim = date(ano, 12, 31)
        return ini, fim
    if modo == "Mês":
        ini = date(ano, mes, 1)
        if mes == 12:
            fim = date(ano, 12, 31)
        else:
            fim = date(ano, mes + 1, 1) - timedelta(days=1)
        return ini, fim
    # Intervalo
    return intervalo[0], intervalo[1]

def aplicar_periodo_union(df: pd.DataFrame, ini: date, fim: date) -> pd.DataFrame:
    """Aplica filtro de período considerando Criado OU Resolvido no intervalo."""
    criado = pd.to_datetime(df["Criado"], errors="coerce")
    if "Resolvido" in df.columns:
        resolvido = pd.to_datetime(df["Resolvido"], errors="coerce")
    else:
        resolvido = pd.Series([pd.NaT] * len(df))

    mask = (
        ((criado.dt.date >= ini) & (criado.dt.date <= fim)) |
        ((resolvido.dt.date >= ini) & (resolvido.dt.date <= fim))
    )
    return df[mask].copy()

# =========================================
# Métricas por analista considerando período
# =========================================
def resumo_por_analista_periodico(
    df_base: pd.DataFrame,
    ini: date,
    fim: date,
    status_encerrados: set[str] = STATUS_ENCERRADOS
) -> pd.DataFrame:
    """
    Calcula por responsável (no período [ini, fim], inclusivo):
      - Total de Tickets = criados no período
      - Tickets Encerrados = resolvidos no período (status em status_encerrados)
      - Tickets em Aberto = criados no período que NÃO estavam resolvidos até o fim do período
      - Tempo médio p/ Encerramento (dias) = média (Resolvido - Criado) somente de tickets resolvidos no período
      - Média de Tickets Encerrados por Dia = (Encerrados no período) / (nº de dias ÚTEIS do período)
    """
    if df_base.empty:
        return pd.DataFrame(columns=[
            "Responsável", "Total de Tickets", "Tickets Encerrados",
            "Tickets em Aberto", "Tempo médio para Encerramento (dias)",
            "Média de Tickets Encerrados por Dia"
        ])

    # Garantir datas
    df = df_base.copy()
    df["Criado"] = pd.to_datetime(df["Criado"], errors="coerce")
    if "Resolvido" in df.columns:
        df["Resolvido"] = pd.to_datetime(df["Resolvido"], errors="coerce")
    else:
        df["Resolvido"] = pd.NaT

    # Subconjuntos do período
    criados_mask = (df["Criado"].dt.date >= ini) & (df["Criado"].dt.date <= fim)
    enc_mask = df["Resolvido"].notna() & (df["Resolvido"].dt.date >= ini) & (df["Resolvido"].dt.date <= fim) & (df["Status"].isin(status_encerrados))

    df_criados = df[criados_mask]
    df_enc = df[enc_mask]

    # Total criados por responsável
    total = df_criados.groupby("Responsavel").size().rename("Total de Tickets")

    # Encerrados por responsável (no período)
    encerrados = df_enc.groupby("Responsavel").size().rename("Tickets Encerrados")

    # Abertos no período = criados no período que NÃO estavam resolvidos até o fim do período
    ainda_abertos_mask = criados_mask & (df["Resolvido"].isna() | (df["Resolvido"].dt.date > fim))
    df_abertos = df[ainda_abertos_mask]
    abertos = df_abertos.groupby("Responsavel").size().rename("Tickets em Aberto")

    # Tempo médio de encerramento (dias) — somente resolvidos no período
    if not df_enc.empty:
        aux = df_enc.dropna(subset=["Criado", "Resolvido"]).copy()
        aux["dur_dias"] = (aux["Resolvido"] - aux["Criado"]).dt.total_seconds() / (3600 * 24)
        tempo_medio = aux.groupby("Responsavel")["dur_dias"].mean().rename("Tempo médio para Encerramento (dias)")
    else:
        tempo_medio = pd.Series(dtype=float, name="Tempo médio para Encerramento (dias)")

    # Junta
    base = pd.concat([total, encerrados, abertos, tempo_medio], axis=1).fillna(0)

    # Dias ÚTEIS no período (segunda=0 .. sexta=4). Inclui dias sem encerramento (contados como 0)
    dias_uteis = pd.date_range(start=ini, end=fim, freq="B")
    n_dias_uteis = max(len(dias_uteis), 1)

    # Média de encerrados por dia útil
    base["Média de Tickets Encerrados por Dia"] = base.get("Tickets Encerrados", 0) / n_dias_uteis

    base = base.reset_index().rename(columns={"Responsavel": "Responsável"})
    base = base[[
        "Responsável", "Total de Tickets", "Tickets Encerrados", "Tickets em Aberto",
        "Tempo médio para Encerramento (dias)", "Média de Tickets Encerrados por Dia"
    ]].sort_values(["Total de Tickets", "Tickets Encerrados"], ascending=[False, False])

    return base

# =========================================
# KPIs simples
# =========================================
def tempo_medio_encerramento_dias(df: pd.DataFrame) -> float:
    if df.empty or "Resolvido" not in df.columns:
        return 0.0
    base = df.dropna(subset=["Criado", "Resolvido"]).copy()
    if base.empty:
        return 0.0
    base["dur_dias"] = (base["Resolvido"] - base["Criado"]).dt.total_seconds() / (3600 * 24)
    return float(base["dur_dias"].mean())

# =========================================
# Upload
# =========================================
uploaded = st.file_uploader("Carregue o CSV exportado do Jira/Sheets", type=["csv"])
if not uploaded:
    st.info("➡️ Anexe o CSV para iniciar a análise.")
    st.stop()

df_raw = ler_csv_flex(uploaded)
df = padronizar_colunas(df_raw)

# =========================================
# Validação mínima
# =========================================
ok, faltando = validar_minimo(df, ["Responsavel", "Status", "Criado"])
if not ok:
    st.error(f"Colunas obrigatórias ausentes: {', '.join(faltando)}. "
             f"Verifique os nomes ou ajuste o mapeamento em COLMAP.")
    st.stop()

# =========================================
# Tratamento de datas (PT/EN)
# =========================================
df["Criado"] = parse_mixed_datetime_series(df["Criado"])
if "Resolvido" in df.columns:
    df["Resolvido"] = parse_mixed_datetime_series(df["Resolvido"])

# =========================================
# Tipo normalizado (Request/Incident/Outro)
# =========================================
df["Tipo_Normalizado"] = df.apply(normalizar_tipo_linha, axis=1)

# =========================================
# Filtros (sidebar) – UM filtro único para tudo
# =========================================
with st.sidebar:
    st.header("⚙️ Filtros")

    modo_periodo = st.radio("Período", ["Todo período", "Ano", "Mês", "Intervalo"], horizontal=False)

    ano_sel = date.today().year
    mes_sel = date.today().month
    intervalo_sel = (date(date.today().year, 1, 1), date.today())

    if modo_periodo == "Ano":
        ano_sel = st.number_input("Ano", min_value=2000, max_value=2100, value=date.today().year, step=1)
    elif modo_periodo == "Mês":
        c1, c2 = st.columns(2)
        with c1:
            ano_sel = st.number_input("Ano", min_value=2000, max_value=2100, value=date.today().year, step=1)
        with c2:
            mes_sel = st.number_input("Mês", min_value=1, max_value=12, value=date.today().month, step=1)
    elif modo_periodo == "Intervalo":
        intervalo_sel = st.date_input("Intervalo (início → fim)", value=intervalo_sel)

    # Analista
    analistas = ["Todos"] + sorted(df["Responsavel"].dropna().unique().tolist())
    analista_sel = st.selectbox("Analista", analistas)

    # Status
    todos_status = sorted(df["Status"].dropna().astype(str).unique().tolist())
    status_sel = st.multiselect("Status", options=todos_status, default=todos_status)

# =========================================
# Aplicar período
# =========================================
ini, fim = period_bounds(modo_periodo, ano_sel, mes_sel, intervalo_sel)

# Se "Todo período", restringe aos limites do dataset para o union
if modo_periodo == "Todo período":
    min_criado = df["Criado"].dropna().dt.date.min() or date(1970, 1, 1)
    max_criado = df["Criado"].dropna().dt.date.max() or date(2100, 12, 31)
    if "Resolvido" in df.columns:
        min_res = df["Resolvido"].dropna().dt.date.min() if df["Resolvido"].notna().any() else None
        max_res = df["Resolvido"].dropna().dt.date.max() if df["Resolvido"].notna().any() else None
    else:
        min_res = None
        max_res = None
    min_all = min([d for d in [min_criado, min_res] if d is not None])
    max_all = max([d for d in [max_criado, max_res] if d is not None])
    ini, fim = min_all, max_all

# Aplica UNION (Criado OU Resolvido no período)
dfp = aplicar_periodo_union(df, ini, fim)

# Filtro por analista
if analista_sel != "Todos":
    dfp = dfp[dfp["Responsavel"] == analista_sel]

# Filtro por status (se selecionado)
if status_sel:
    dfp = dfp[dfp["Status"].astype(str).isin(status_sel)]

# =========================================
# BLOCO 1 — Análise de Tickets por Analista (primeiro)
# =========================================
st.subheader("📊 Análise de Tickets por Analista")

# Tabela de resumo por analista com regras do período
resumo = resumo_por_analista_periodico(dfp, ini, fim, STATUS_ENCERRADOS)
st.dataframe(resumo, use_container_width=True)

# (Removidos os demais gráficos opcionais)

# =========================================
# BLOCO 2 — Restante do "Análise Demandas & Atuação"
# =========================================
st.subheader("🧩 Demandas & Atuação (com filtros unificados)")

# KPIs do período atual (base union)
c1, c2, c3, c4 = st.columns(4)
total_tickets_union = int(len(dfp))
qtd_inc = int((dfp["Tipo_Normalizado"] == "Incident").sum())
qtd_req = int((dfp["Tipo_Normalizado"] == "Request").sum())
perc_inc = (qtd_inc / total_tickets_union * 100) if total_tickets_union else 0
perc_req = (qtd_req / total_tickets_union * 100) if total_tickets_union else 0

# média de tempo de encerramento (somente tickets encerrados dentro do período)
dfp_enc = dfp[
    dfp["Resolvido"].notna()
    & (dfp["Resolvido"].dt.date >= ini)
    & (dfp["Resolvido"].dt.date <= fim)
    & (dfp["Status"].isin(STATUS_ENCERRADOS))
]

tmedio_dias = tempo_medio_encerramento_dias(dfp_enc)

with c1:
    st.metric("Total de Tickets (union no período)", f"{total_tickets_union:,}".replace(",", "."))
with c2:
    st.metric("Requests (%)", f"{perc_req:.1f}% ({qtd_req})")
with c3:
    st.metric("Incidents (%)", f"{perc_inc:.1f}% ({qtd_inc})")
with c4:
    st.metric("Tempo médio p/ encerrar (dias)", f"{tmedio_dias:.2f}")

# =========================================
# Gráfico FIXO — Top 10 Aplicações (sempre exibido, respeitando filtros aplicados)
# =========================================
st.subheader("🏆 Top 10 Aplicações")
if "Aplicacao" in dfp.columns and not dfp.empty:
    apps = (
        dfp.assign(Aplicacao=dfp["Aplicacao"].fillna("Não informado").astype(str))
           .groupby("Aplicacao").size().reset_index(name="Quantidade")
    )
    if apps.empty:
        st.info("Não há dados suficientes para gerar o Top 10 de Aplicações neste filtro.")
    else:
        top10 = apps.sort_values("Quantidade", ascending=False).head(10)
        # Ordena as barras no gráfico horizontal conforme a ordem do DataFrame
        fig_apps = px.bar(
            top10.sort_values("Quantidade"),  # menor → maior, para leitura top-down
            x="Quantidade",
            y="Aplicacao",
            orientation="h",
            text_auto=True,
            title="Top 10 Aplicações por Volume (Union)"
        )
        fig_apps.update_layout(
            yaxis={"categoryorder": "array",
                   "categoryarray": top10.sort_values("Quantidade")["Aplicacao"].tolist()}
        )
        st.plotly_chart(fig_apps, use_container_width=True)
else:
    st.info("Coluna de Aplicação não encontrada no arquivo. Verifique o mapeamento para 'Campo personalizado (Application/Software)'.")

# =========================================
# Tabela detalhada (com campos relevantes)
# =========================================
st.subheader("📄 Tabela detalhada (filtrada)")
cols_show = [c for c in [
    "Projeto", "Responsavel", "Status", "Tipo", "Tipo_Normalizado",
    "Aplicacao", "Criado", "Resolvido", "Resumo", "Descricao"
] if c in dfp.columns]

if cols_show:
    # Ordena por "Criado" desc quando existir, senão por "Resolvido"
    if "Criado" in cols_show:
        df_show = dfp[cols_show].sort_values("Criado", ascending=False)
    elif "Resolvido" in cols_show:
        df_show = dfp[cols_show].sort_values("Resolvido", ascending=False)
    else:
        df_show = dfp[cols_show]
    st.dataframe(df_show, use_container_width=True)
else:
    st.info("Não há colunas detalhadas disponíveis para exibir.")

# =========================================
# FIM
# =========================================
