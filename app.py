import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, render_template, request
from werkzeug.utils import secure_filename

DEFAULT_EXPORT_DIR = r"C:\\META REPRESENTANTES\\Exporta"
SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv"}
DATA_DIR = Path("data")
DATA_STORE_FILE = DATA_DIR / "metas.json"
DEFAULT_COLLECTION = "PRIMAVERA"
COLLECTION_OPTIONS = ["PRIMAVERA", "INVERNO", "VERAO"]

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024


def resolve_export_dir() -> Path:
    env_path = os.getenv("EXPORT_DIR")
    return Path(env_path) if env_path else Path(DEFAULT_EXPORT_DIR)


def find_latest_file(directory: Path) -> Path | None:
    if not directory.exists():
        return None
    files = [
        path
        for path in directory.iterdir()
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def purge_export_files(directory: Path) -> int:
    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)
        return 0
    removed = 0
    for path in directory.iterdir():
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS:
            path.unlink()
            removed += 1
    return removed


def default_store() -> dict:
    return {
        "version": 2,
        "periodos": {},
        "representantes": {},
        "metas_atuais": [],
        "historico_metas": [],
        "config": {
            "ano_atual": datetime.now().year,
            "colecao_atual": DEFAULT_COLLECTION,
            "ano_atual_dazul": None,
            "colecao_atual_dazul": None,
            "ano_atual_saron": None,
            "colecao_atual_saron": None,
        },
    }


def ensure_data_store() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not DATA_STORE_FILE.exists():
        DATA_STORE_FILE.write_text(
            json.dumps(default_store(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def load_data_store() -> dict:
    ensure_data_store()
    try:
        data = json.loads(DATA_STORE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        data = {}

    if "metas" in data and "periodos" not in data:
        period = data.get("periodo") or current_period()
        data = {
            "periodos": {
                period: {
                    "metas": data.get("metas", {}),
                    "updated_at": data.get("updated_at"),
                }
            }
        }

    defaults = default_store()
    for key, value in defaults.items():
        if key not in data:
            data[key] = value
    if not isinstance(data.get("periodos"), dict):
        data["periodos"] = {}
    if not isinstance(data.get("representantes"), dict):
        data["representantes"] = {}
    if not isinstance(data.get("metas_atuais"), list):
        data["metas_atuais"] = []
    if not isinstance(data.get("historico_metas"), list):
        data["historico_metas"] = []
    if not isinstance(data.get("config"), dict):
        data["config"] = defaults["config"]
    else:
        for key, value in defaults["config"].items():
            data["config"].setdefault(key, value)
    return data


def save_data_store(data: dict) -> None:
    ensure_data_store()
    DATA_STORE_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def current_period() -> str:
    return datetime.now().strftime("%Y-%m")


def normalize_period(value: str | None) -> str:
    if not value:
        return current_period()
    match = re.match(r"^\d{4}-\d{2}$", value.strip())
    if match:
        return value.strip()
    return current_period()


def normalize_identifier(code_value: str | None, name_value: str | None) -> str:
    code = str(code_value or "").strip()
    if code and code.lower() != "nan":
        return code
    name = str(name_value or "").strip().upper()
    return name


def parse_ptbr_number(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, (int, float)):
        return float(raw_value)
    value = str(raw_value).strip()
    if not value:
        return None
    value = value.replace("R$", "").replace(" ", "")
    value = value.replace(".", "").replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [column.strip().upper() for column in df.columns]
    original_columns = set(df.columns)

    def pick_column(options: list[str]) -> str | None:
        for option in options:
            if option in df.columns:
                return option
        return None

    rename_map = {}
    if "VENDEDOR" in df.columns and "CODIGO" in df.columns:
        order_signals = [
            "DOCUMENTO",
            "NUM_OS",
            "CODIGO_PEDIDO",
            "NUM_PE",
            "NUM_FA",
            "TOTAL_NOTA",
            "TOTAL_BRUTO",
            "CLIENTE",
        ]
        if any(signal in df.columns for signal in order_signals):
            rename_map["CODIGO"] = "CODIGO_PEDIDO"

    code_column = pick_column(
        [
            "VENDEDOR",
            "CODIGO_VENDEDOR",
            "CODIGO_REPRESENTANTE",
            "ID_REPRESENTANTE",
            "ID_REP",
            "CODIGO",
            "COD",
        ]
    )
    if code_column and code_column != "CODIGO":
        rename_map[code_column] = "CODIGO"

    if "NOME_VENDEDOR" in df.columns:
        name_column = "NOME_VENDEDOR"
    else:
        name_column = pick_column(
            ["VENDEDOR", "REPRESENTANTE", "NOME_REPRESENTANTE", "NOME"]
        )
    if name_column and name_column != "NOME_VENDEDOR":
        rename_map[name_column] = "NOME_VENDEDOR"

    column_aliases = {
        "QTDEITEM": ["ITENS", "ITENS_VENDIDOS", "QTDE_ITEM", "QTDE_ITENS", "NUM_ITENS"],
        "TOTAL_PEDIDOS": ["PEDIDOS", "QTDE_PEDIDOS", "TOTALPEDIDOS"],
        "TOTAL_CLIENTES": ["CLIENTES", "TOTALCLIENTES"],
        "CLIENTES_ATIVOS": ["ATIVOS", "CLIENTES_ATIVOS", "ATIVOS_CARTEIRA"],
        "CLIENTES_NOVOS": ["CLIENTES_NOVOS", "NOVOS_CLIENTES"],
        "VLR_LIQUIDO": [
            "VALOR_LIQUIDO",
            "VLR_LIQUIDO",
            "VENDAS_LIQUIDAS",
            "VALOR",
            "TOTAL_NOTA",
            "TOTAL_BRUTO",
            "TOTAL_PRODUTO",
        ],
        "PRECO_MEDIO": ["PRECO_MEDIO", "PREÇO_MEDIO", "VALOR_MEDIO"],
        "MEDIA_PEDIDOS": ["MEDIA_PEDIDOS", "MEDIA_PEDIDO"],
        "QTDE_MEDIA": ["QTDE_MEDIA", "MEDIA_ITENS"],
    }
    for target, options in column_aliases.items():
        column = pick_column([target] + options)
        if column and column != target:
            rename_map[column] = target

    if rename_map:
        df = df.rename(columns=rename_map)

    if "CODIGO" not in df.columns and {"VENDEDOR", "NOME_VENDEDOR"}.issubset(original_columns):
        df = df.rename(columns={"VENDEDOR": "CODIGO"})

    return df


def normalize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    numeric_columns = [
        "QTDEITEM",
        "TOTAL_PEDIDOS",
        "TOTAL_CLIENTES",
        "CLIENTES_ATIVOS",
        "QTDE_MEDIA",
        "VLR_LIQUIDO",
        "PRECO_MEDIO",
        "MEDIA_PEDIDOS",
        "CLIENTES_NOVOS",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = (
                df[column]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def aggregate_order_report(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "TOTAL_PEDIDOS" in df.columns or "TOTAL_CLIENTES" in df.columns:
        return df

    def pick_column(options: list[str]) -> str | None:
        for option in options:
            if option in df.columns:
                return option
        return None

    order_column = pick_column(["CODIGO_PEDIDO", "NUM_OS", "DOCUMENTO", "CODIGO"])
    client_column = pick_column(
        ["CLIENTE", "COD_CLIENTE", "CODIGO_CLIENTE", "ID_CLIENTE", "NOME_CLIENTE"]
    )
    active_column = pick_column(["CLIENTE_ATIVO", "STATUS_CLIENTE"])
    cadastro_column = pick_column(["CADASTRO_CLIENTE", "DATA_CADASTRO_CLIENTE"])
    qtde_column = pick_column(["QTDEITEM"])

    if "VLR_LIQUIDO" not in df.columns or "CODIGO" not in df.columns:
        return df

    work_df = df.copy()
    if "NOME_VENDEDOR" not in work_df.columns:
        work_df["NOME_VENDEDOR"] = "-"
    if order_column:
        work_df["_order_id"] = work_df[order_column]
    else:
        work_df["_order_id"] = work_df.index

    if client_column:
        work_df["_client_id"] = work_df[client_column]
    else:
        work_df["_client_id"] = None

    if active_column:
        work_df["_active_client"] = work_df[active_column].apply(
            lambda value: str(value).strip().upper()
            in {"SIM", "S", "ATIVO", "ATIVA", "1", "TRUE", "VERDADEIRO"}
        )
    else:
        work_df["_active_client"] = False

    if cadastro_column:
        cadastro_dates = pd.to_datetime(
            work_df[cadastro_column], errors="coerce", dayfirst=True
        )
        current_year = datetime.now().year
        work_df["_is_new_client"] = cadastro_dates.dt.year == current_year
    else:
        work_df["_is_new_client"] = False

    group_fields = ["CODIGO", "NOME_VENDEDOR"]
    aggregations = {"VLR_LIQUIDO": "sum"}
    if qtde_column:
        aggregations["QTDEITEM"] = "sum"

    grouped = work_df.groupby(group_fields, dropna=False)
    aggregated = grouped.agg(aggregations).reset_index()

    aggregated["TOTAL_PEDIDOS"] = grouped["_order_id"].nunique().values

    if client_column:
        aggregated["TOTAL_CLIENTES"] = grouped["_client_id"].nunique().values
        aggregated["CLIENTES_ATIVOS"] = (
            grouped.apply(
                lambda group: group.loc[group["_active_client"], "_client_id"]
                .dropna()
                .nunique()
            )
            .values
        )
        aggregated["CLIENTES_NOVOS"] = (
            grouped.apply(
                lambda group: group.loc[group["_is_new_client"], "_client_id"]
                .dropna()
                .nunique()
            )
            .values
        )
    else:
        aggregated["TOTAL_CLIENTES"] = 0
        aggregated["CLIENTES_ATIVOS"] = 0
        aggregated["CLIENTES_NOVOS"] = 0

    if "QTDEITEM" not in aggregated.columns:
        aggregated["QTDEITEM"] = 0
    else:
        aggregated["QTDEITEM"] = aggregated["QTDEITEM"].fillna(0)
    aggregated["TOTAL_PEDIDOS"] = aggregated["TOTAL_PEDIDOS"].fillna(0)

    aggregated["QTDE_MEDIA"] = aggregated.apply(
        lambda row: row["QTDEITEM"] / row["TOTAL_PEDIDOS"]
        if row["TOTAL_PEDIDOS"] > 0
        else 0,
        axis=1,
    )
    aggregated["PRECO_MEDIO"] = aggregated.apply(
        lambda row: row["VLR_LIQUIDO"] / row["QTDEITEM"] if row["QTDEITEM"] > 0 else 0,
        axis=1,
    )
    aggregated["MEDIA_PEDIDOS"] = aggregated.apply(
        lambda row: row["VLR_LIQUIDO"] / row["TOTAL_PEDIDOS"]
        if row["TOTAL_PEDIDOS"] > 0
        else 0,
        axis=1,
    )

    return normalize_numeric_columns(aggregated)


def get_current_config(store: dict) -> dict:
    config = store.get("config", {})
    ano_atual = config.get("ano_atual") or datetime.now().year
    colecao_atual = config.get("colecao_atual") or DEFAULT_COLLECTION
    return {
        "ano_atual": int(ano_atual),
        "colecao_atual": str(colecao_atual).upper(),
        "ano_atual_dazul": config.get("ano_atual_dazul"),
        "colecao_atual_dazul": config.get("colecao_atual_dazul"),
        "ano_atual_saron": config.get("ano_atual_saron"),
        "colecao_atual_saron": config.get("colecao_atual_saron"),
    }


def next_id(items: list[dict]) -> int:
    existing = [item.get("id", 0) for item in items if isinstance(item.get("id"), int)]
    return (max(existing) + 1) if existing else 1


def sync_representantes(store: dict, df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    representantes = store.setdefault("representantes", {})
    changed = False
    for _, row in df.iterrows():
        codigo = row.get("CODIGO")
        nome = row.get("NOME_VENDEDOR") or "-"
        rep_id = normalize_identifier(codigo, nome)
        if rep_id not in representantes:
            representantes[rep_id] = {
                "id": rep_id,
                "nome": nome,
                "status": "ATIVO",
                "marca": None,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            changed = True
    return changed


def build_metas_map(store: dict, ano: int, colecao: str) -> dict:
    metas_map: dict[str, dict] = {}
    for meta in store.get("metas_atuais", []):
        meta_colecao = str(meta.get("colecao") or "").upper()
        if meta.get("ano") == ano and meta_colecao == colecao:
            rep_id = meta.get("representante_id")
            if rep_id:
                metas_map[rep_id] = meta
    return metas_map


def load_report(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        csv_kwargs = {"sep": None, "engine": "python"}
        encodings_to_try = ("utf-8-sig", "utf-8", "utf-16", "cp1252", "latin1")
        df = None

        with path.open("rb") as file_handle:
            sample = file_handle.read(4096)
        for encoding in encodings_to_try:
            try:
                sample.decode(encoding)
                df = pd.read_csv(path, encoding=encoding, **csv_kwargs)
                break
            except UnicodeDecodeError:
                continue

        if df is None:
            df = pd.read_csv(
                path, encoding="latin1", encoding_errors="replace", **csv_kwargs
            )
    else:
        df = pd.read_excel(path)
    df = normalize_columns(df)
    df = normalize_numeric_columns(df)
    df = aggregate_order_report(df)
    return df


def build_summary(df: pd.DataFrame, metas_map: dict) -> dict:
    total_vendedores = df.shape[0]
    total_itens = int(df["QTDEITEM"].sum()) if "QTDEITEM" in df.columns else 0
    total_vendas = float(df["VLR_LIQUIDO"].sum()) if "VLR_LIQUIDO" in df.columns else 0.0
    total_pedidos = int(df["TOTAL_PEDIDOS"].sum()) if "TOTAL_PEDIDOS" in df.columns else 0
    total_clientes = int(df["TOTAL_CLIENTES"].sum()) if "TOTAL_CLIENTES" in df.columns else 0
    ativos = int(df["CLIENTES_ATIVOS"].sum()) if "CLIENTES_ATIVOS" in df.columns else 0
    novos = int(df["CLIENTES_NOVOS"].sum()) if "CLIENTES_NOVOS" in df.columns else 0
    total_meta = 0.0
    total_vendas_com_meta = 0.0
    gap_total = 0.0
    vendedores_com_meta = 0
    vendedores_acima_meta = 0
    vendedores_sem_meta = 0
    vendedores_abaixo_meta = 0
    for _, row in df.iterrows():
        rep_id = normalize_identifier(row.get("CODIGO"), row.get("NOME_VENDEDOR"))
        meta_data = metas_map.get(rep_id, {})
        meta_valor = meta_data.get("meta")
        vlr_liquido = float(row.get("VLR_LIQUIDO") or 0)
        if meta_valor and meta_valor > 0:
            vendedores_com_meta += 1
            total_meta += meta_valor
            total_vendas_com_meta += vlr_liquido
            gap_total += max(meta_valor - vlr_liquido, 0.0)
            if vlr_liquido / meta_valor >= 1:
                vendedores_acima_meta += 1
            else:
                vendedores_abaixo_meta += 1
        else:
            vendedores_sem_meta += 1

    percentual_meta = total_vendas_com_meta / total_meta if total_meta else 0.0
    meta_gap = total_meta - total_vendas_com_meta
    valor_faltante = max(meta_gap, 0.0)
    valor_excedente = max(-meta_gap, 0.0)

    if total_meta <= 0:
        meta_status_label = "Metas pendentes"
        meta_status_class = "status-neutral"
        meta_status_detail = "Sem metas cadastradas para o período."
    elif percentual_meta >= 1:
        meta_status_label = "Meta atingida"
        meta_status_class = "status-good"
        meta_status_detail = (
            f"Acima da meta em R$ {valor_excedente:,.2f}".replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
    elif percentual_meta >= 0.8:
        meta_status_label = "Reta final"
        meta_status_class = "status-warning"
        meta_status_detail = (
            f"Faltam R$ {valor_faltante:,.2f} para atingir a meta.".replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
    else:
        meta_status_label = "Atenção"
        meta_status_class = "status-alert"
        meta_status_detail = (
            f"Faltam R$ {valor_faltante:,.2f} para atingir a meta.".replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
    meta_progress = min(percentual_meta * 100, 100)

    ranking_vendas = (
        df.sort_values("VLR_LIQUIDO", ascending=False)
        .head(5)[["NOME_VENDEDOR", "VLR_LIQUIDO"]]
        .to_dict(orient="records")
        if "VLR_LIQUIDO" in df.columns
        else []
    )
    ranking_piores = (
        df.sort_values("VLR_LIQUIDO", ascending=True)
        .head(5)[["NOME_VENDEDOR", "VLR_LIQUIDO"]]
        .to_dict(orient="records")
        if "VLR_LIQUIDO" in df.columns
        else []
    )
    destaque = (
        df.sort_values("VLR_LIQUIDO", ascending=False)
        .head(1)[["NOME_VENDEDOR", "VLR_LIQUIDO"]]
        .to_dict(orient="records")
    )
    vendedor_destaque = destaque[0] if destaque else {"NOME_VENDEDOR": "-", "VLR_LIQUIDO": 0.0}

    vendedores_sem_clientes_ativos = 0
    if "CLIENTES_ATIVOS" in df.columns:
        vendedores_sem_clientes_ativos = int((df["CLIENTES_ATIVOS"].fillna(0) <= 0).sum())

    top_clientes_novos = {"nome": "-", "clientes_novos": 0}
    if "CLIENTES_NOVOS" in df.columns:
        clientes_novos = df["CLIENTES_NOVOS"].fillna(0)
        if not clientes_novos.empty:
            idx_top = clientes_novos.idxmax()
            nome_top = (
                df.at[idx_top, "NOME_VENDEDOR"] if "NOME_VENDEDOR" in df.columns else "-"
            )
            top_clientes_novos = {
                "nome": nome_top,
                "clientes_novos": int(clientes_novos.loc[idx_top]),
            }

    return {
        "total_vendedores": total_vendedores,
        "total_itens": total_itens,
        "total_vendas": total_vendas,
        "total_pedidos": total_pedidos,
        "total_clientes": total_clientes,
        "clientes_ativos": ativos,
        "clientes_novos": novos,
        "total_meta": total_meta,
        "percentual_meta": percentual_meta,
        "meta_progress": meta_progress,
        "meta_status_label": meta_status_label,
        "meta_status_class": meta_status_class,
        "meta_status_detail": meta_status_detail,
        "valor_faltante": valor_faltante,
        "gap_total": gap_total,
        "ranking_vendas": ranking_vendas,
        "ranking_piores": ranking_piores,
        "vendedor_destaque": vendedor_destaque,
        "vendedores_abaixo_meta": vendedores_abaixo_meta,
        "vendedores_sem_meta": vendedores_sem_meta,
        "vendedores_sem_clientes_ativos": vendedores_sem_clientes_ativos,
        "vendedores_com_meta": vendedores_com_meta,
        "vendedores_acima_meta": vendedores_acima_meta,
        "vendedores_abaixo_meta": vendedores_abaixo_meta,
        "top_clientes_novos": top_clientes_novos,
    }


def build_insights(df: pd.DataFrame) -> dict:
    insights = {
        "ticket_medio": 0.0,
        "media_itens_por_pedido": 0.0,
        "taxa_clientes_ativos": 0.0,
        "media_pedidos_por_cliente": 0.0,
        "clientes_por_vendedor": 0.0,
        "media_venda_por_vendedor": 0.0,
        "ticket_medio_cliente": 0.0,
    }
    if "VLR_LIQUIDO" in df.columns:
        insights["ticket_medio"] = float(df["VLR_LIQUIDO"].sum()) / max(
            df["TOTAL_PEDIDOS"].sum(), 1
        )
        insights["media_venda_por_vendedor"] = float(df["VLR_LIQUIDO"].sum()) / max(
            df.shape[0], 1
        )
    if "QTDEITEM" in df.columns:
        insights["media_itens_por_pedido"] = float(df["QTDEITEM"].sum()) / max(
            df["TOTAL_PEDIDOS"].sum(), 1
        )
    if "CLIENTES_ATIVOS" in df.columns:
        insights["taxa_clientes_ativos"] = (
            float(df["CLIENTES_ATIVOS"].sum()) / max(df["TOTAL_CLIENTES"].sum(), 1)
        )
    if "TOTAL_PEDIDOS" in df.columns and "TOTAL_CLIENTES" in df.columns:
        insights["media_pedidos_por_cliente"] = float(df["TOTAL_PEDIDOS"].sum()) / max(
            df["TOTAL_CLIENTES"].sum(), 1
        )
    if "TOTAL_CLIENTES" in df.columns:
        insights["clientes_por_vendedor"] = float(df["TOTAL_CLIENTES"].sum()) / max(
            df.shape[0], 1
        )
        insights["ticket_medio_cliente"] = float(df["VLR_LIQUIDO"].sum()) / max(
            df["TOTAL_CLIENTES"].sum(), 1
        )
    return insights


def build_vendedores(df: pd.DataFrame, metas_map: dict) -> list[dict]:
    df = df.copy()
    df["NOME_VENDEDOR"] = df.get("NOME_VENDEDOR", "-")
    df = df.sort_values("VLR_LIQUIDO", ascending=False)
    vendedores = []

    def safe_value(value, default=0):
        if pd.isna(value):
            return default
        return value

    for _, row in df.iterrows():
        rep_id = normalize_identifier(row.get("CODIGO"), row.get("NOME_VENDEDOR"))
        meta_data = metas_map.get(rep_id, {})
        valor_meta = safe_value(meta_data.get("meta"), 0.0)
        vlr_liquido = safe_value(row.get("VLR_LIQUIDO"), 0.0)
        percentual_meta = None
        gap_meta = None
        meta_status_class = "status-neutral"
        meta_status_label = "Sem meta cadastrada"

        if valor_meta and valor_meta > 0:
            percentual_meta = vlr_liquido / valor_meta
            gap_meta = valor_meta - vlr_liquido
            if percentual_meta >= 1:
                meta_status_class = "status-good"
                meta_status_label = "Meta atingida"
            elif percentual_meta >= 0.8:
                meta_status_class = "status-warning"
                meta_status_label = "Reta final"
            else:
                meta_status_class = "status-alert"
                meta_status_label = "Atenção"

        vendedores.append(
            {
                "codigo": row.get("CODIGO"),
                "nome": row.get("NOME_VENDEDOR"),
                "identificador": rep_id,
                "qtde_item": safe_value(row.get("QTDEITEM")),
                "total_pedidos": safe_value(row.get("TOTAL_PEDIDOS")),
                "total_clientes": safe_value(row.get("TOTAL_CLIENTES")),
                "clientes_ativos": safe_value(row.get("CLIENTES_ATIVOS")),
                "qtde_media": safe_value(row.get("QTDE_MEDIA")),
                "vlr_liquido": vlr_liquido,
                "preco_medio": safe_value(row.get("PRECO_MEDIO")),
                "media_pedidos": safe_value(row.get("MEDIA_PEDIDOS")),
                "clientes_novos": safe_value(row.get("CLIENTES_NOVOS")),
                "valor_meta": valor_meta,
                "percentual_meta": percentual_meta,
                "gap_meta": gap_meta,
                "meta_status_class": meta_status_class,
                "meta_status_label": meta_status_label,
            }
        )
    return vendedores


def build_rankings(vendedores: list[dict]) -> dict:
    ranking_data = {}

    def build_items(items: list[dict], value_key: str) -> list[dict]:
        return [
            {"nome": item["nome"], "valor": item[value_key]}
            for item in items
            if item.get(value_key) is not None
        ]

    ranking_data["valor"] = {
        "top": build_items(
            sorted(vendedores, key=lambda item: item.get("vlr_liquido", 0), reverse=True)[:5],
            "vlr_liquido",
        ),
        "bottom": build_items(
            sorted(vendedores, key=lambda item: item.get("vlr_liquido", 0))[:5],
            "vlr_liquido",
        ),
    }

    vendedores_com_meta = [item for item in vendedores if item.get("percentual_meta") is not None]
    ranking_data["atingimento"] = {
        "top": build_items(
            sorted(
                vendedores_com_meta,
                key=lambda item: item.get("percentual_meta", 0),
                reverse=True,
            )[:5],
            "percentual_meta",
        ),
        "bottom": build_items(
            sorted(
                vendedores_com_meta,
                key=lambda item: item.get("percentual_meta", 0),
            )[:5],
            "percentual_meta",
        ),
    }

    for item in vendedores:
        gap_valor = item.get("gap_meta")
        item["gap_valor"] = max(gap_valor, 0) if gap_valor is not None else None

    vendedores_com_gap = [item for item in vendedores if item.get("gap_valor") is not None]
    ranking_data["gap"] = {
        "top": build_items(
            sorted(vendedores_com_gap, key=lambda item: item.get("gap_valor", 0), reverse=True)[:5],
            "gap_valor",
        ),
        "bottom": build_items(
            sorted(vendedores_com_gap, key=lambda item: item.get("gap_valor", 0))[:5],
            "gap_valor",
        ),
    }

    return ranking_data


@app.route("/")
def dashboard():
    export_dir = resolve_export_dir()
    latest_file = find_latest_file(export_dir)
    if not latest_file:
        return render_template(
            "index.html",
            data_loaded=False,
            export_dir=str(export_dir),
            supported_extensions=", ".join(sorted(SUPPORTED_EXTENSIONS)),
        )

    df = load_report(latest_file)
    store = load_data_store()
    if sync_representantes(store, df):
        save_data_store(store)
    config = get_current_config(store)
    metas_map = build_metas_map(store, config["ano_atual"], config["colecao_atual"])
    summary = build_summary(df, metas_map)
    insights = build_insights(df)
    vendedores = build_vendedores(df, metas_map)
    ranking_data = build_rankings(vendedores)

    chart_data = json.dumps(
        {
            "labels": [item["nome"] for item in vendedores[:10]],
            "values": [item["vlr_liquido"] for item in vendedores[:10]],
        }
    )

    return render_template(
        "index.html",
        data_loaded=True,
        export_dir=str(export_dir),
        latest_file=latest_file.name,
        updated_at=datetime.fromtimestamp(latest_file.stat().st_mtime),
        config=config,
        summary=summary,
        insights=insights,
        vendedores=vendedores,
        chart_data=chart_data,
        ranking_data=json.dumps(ranking_data),
    )


@app.route("/admin", methods=["GET", "POST"])
def admin():
    export_dir = resolve_export_dir()
    latest_file = find_latest_file(export_dir)
    message = None
    message_type = "info"

    if request.method == "POST":
        uploaded = request.files.get("report_file")
        if not uploaded or not uploaded.filename:
            message = "Selecione um arquivo para atualizar o relatório."
            message_type = "error"
        else:
            filename = secure_filename(uploaded.filename)
            if not filename:
                message = "Nome de arquivo inválido."
                message_type = "error"
            else:
                extension = Path(filename).suffix.lower()
                if extension not in SUPPORTED_EXTENSIONS:
                    message = (
                        "Formato não suportado. Use: "
                        + ", ".join(sorted(SUPPORTED_EXTENSIONS))
                        + "."
                    )
                    message_type = "error"
                else:
                    purge_export_files(export_dir)
                    destination = export_dir / filename
                    uploaded.save(destination)
                    latest_file = destination
                    message = f"Arquivo {filename} atualizado com sucesso."
                    message_type = "success"

    return render_template(
        "admin.html",
        export_dir=str(export_dir),
        latest_file=latest_file.name if latest_file else None,
        updated_at=(
            datetime.fromtimestamp(latest_file.stat().st_mtime) if latest_file else None
        ),
        message=message,
        message_type=message_type,
        supported_extensions=", ".join(sorted(SUPPORTED_EXTENSIONS)),
    )


@app.route("/admin/metas", methods=["GET", "POST"])
def admin_metas():
    export_dir = resolve_export_dir()
    latest_file = find_latest_file(export_dir)
    store = load_data_store()
    df = load_report(latest_file) if latest_file else pd.DataFrame()
    if sync_representantes(store, df):
        save_data_store(store)
    config = get_current_config(store)
    message = None
    message_type = "info"
    edit_id = request.args.get("edit_id")

    if request.method == "POST":
        action = request.form.get("action", "save")
        if action == "delete":
            meta_id = request.form.get("meta_id")
            if meta_id:
                meta_id_int = int(meta_id)
                metas = store.get("metas_atuais", [])
                store["metas_atuais"] = [item for item in metas if item.get("id") != meta_id_int]
                save_data_store(store)
                message = "Meta excluída com sucesso."
                message_type = "success"
        else:
            meta_id = request.form.get("meta_id")
            rep_id = request.form.get("representante_id") or ""
            rep_choice = request.form.get("representante") or ""
            novo_nome = request.form.get("novo_representante") or ""
            status = (request.form.get("status_representante") or "ATIVO").upper()
            ano = int(request.form.get("ano") or config["ano_atual"])
            colecao = (request.form.get("colecao") or config["colecao_atual"]).upper()
            meta_valor = parse_ptbr_number(request.form.get("meta"))

            rep_lookup = {
                f"{rep_id_key} - {rep['nome']}": rep_id_key
                for rep_id_key, rep in store.get("representantes", {}).items()
            }
            rep_lookup.update({rep_id_key: rep_id_key for rep_id_key in store.get("representantes", {})})

            if not rep_id and rep_choice in rep_lookup:
                rep_id = rep_lookup[rep_choice]

            if not rep_id and novo_nome:
                rep_id = normalize_identifier(None, novo_nome)

            errors = []
            if not rep_id:
                errors.append("Informe o representante ou cadastre um novo.")
            if meta_valor is None or meta_valor < 0:
                errors.append("Meta deve ser um número válido maior ou igual a zero.")
            if colecao not in COLLECTION_OPTIONS:
                errors.append("Selecione uma coleção válida.")

            if not errors:
                representantes = store.setdefault("representantes", {})
                rep_nome_final = novo_nome or rep_choice
                rep_data = representantes.get(rep_id)
                if not rep_data:
                    rep_data = {
                        "id": rep_id,
                        "nome": rep_nome_final or rep_id,
                        "status": status if status in {"ATIVO", "NOVO"} else "NOVO",
                        "marca": None,
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                        "updated_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    representantes[rep_id] = rep_data
                else:
                    rep_data["nome"] = rep_data.get("nome") or rep_nome_final or rep_id
                    if status in {"ATIVO", "NOVO"}:
                        rep_data["status"] = status
                    rep_data["updated_at"] = datetime.now().isoformat(timespec="seconds")

                metas = store.setdefault("metas_atuais", [])
                meta_id_int = int(meta_id) if meta_id else None
                meta_entry = None
                if meta_id_int:
                    meta_entry = next(
                        (item for item in metas if item.get("id") == meta_id_int), None
                    )
                if not meta_entry:
                    meta_entry = next(
                        (
                            item
                            for item in metas
                            if item.get("representante_id") == rep_id
                            and item.get("ano") == ano
                            and item.get("colecao") == colecao
                        ),
                        None,
                    )

                if meta_entry:
                    meta_entry.update(
                        {
                            "representante_id": rep_id,
                            "marca": None,
                            "ano": ano,
                            "colecao": colecao,
                            "meta": meta_valor,
                            "updated_at": datetime.now().isoformat(timespec="seconds"),
                        }
                    )
                else:
                    metas.append(
                        {
                            "id": next_id(metas),
                            "representante_id": rep_id,
                            "marca": None,
                            "ano": ano,
                            "colecao": colecao,
                            "meta": meta_valor,
                            "created_at": datetime.now().isoformat(timespec="seconds"),
                            "updated_at": datetime.now().isoformat(timespec="seconds"),
                        }
                    )

                save_data_store(store)
                message = "Meta salva com sucesso."
                message_type = "success"
            else:
                message = " ".join(errors)
                message_type = "error"

    metas_atuais = [
        meta
        for meta in store.get("metas_atuais", [])
        if meta.get("ano") == config["ano_atual"]
        and meta.get("colecao") == config["colecao_atual"]
    ]
    representantes = store.get("representantes", {})
    metas_atuais = sorted(
        metas_atuais,
        key=lambda item: representantes.get(item.get("representante_id"), {}).get("nome", ""),
    )

    edit_meta = None
    if edit_id:
        try:
            edit_meta = next(
                (item for item in metas_atuais if item.get("id") == int(edit_id)), None
            )
        except ValueError:
            edit_meta = None

    rep_options = [
        {
            "value": f"{rep_id} - {rep.get('nome')}",
            "id": rep_id,
            "nome": rep.get("nome"),
            "status": rep.get("status"),
        }
        for rep_id, rep in representantes.items()
    ]
    rep_options = sorted(rep_options, key=lambda item: item["nome"] or "")

    return render_template(
        "admin_metas.html",
        export_dir=str(export_dir),
        latest_file=latest_file.name if latest_file else None,
        updated_at=(
            datetime.fromtimestamp(latest_file.stat().st_mtime) if latest_file else None
        ),
        config=config,
        metas_atuais=metas_atuais,
        rep_options=rep_options,
        edit_meta=edit_meta,
        message=message,
        message_type=message_type,
    )


@app.route("/admin/config", methods=["GET", "POST"])
def admin_config():
    export_dir = resolve_export_dir()
    latest_file = find_latest_file(export_dir)
    store = load_data_store()
    message = None
    message_type = "info"

    if request.method == "POST":
        ano_atual = request.form.get("ano_atual")
        colecao_atual = request.form.get("colecao_atual")
        ano_dazul = request.form.get("ano_atual_dazul") or None
        colecao_dazul = request.form.get("colecao_atual_dazul") or None
        ano_saron = request.form.get("ano_atual_saron") or None
        colecao_saron = request.form.get("colecao_atual_saron") or None

        errors = []
        if not ano_atual or not ano_atual.isdigit():
            errors.append("Informe um ano atual válido.")
        if colecao_atual not in COLLECTION_OPTIONS:
            errors.append("Selecione uma coleção válida.")

        if errors:
            message = " ".join(errors)
            message_type = "error"
        else:
            store["config"] = {
                "ano_atual": int(ano_atual),
                "colecao_atual": colecao_atual,
                "ano_atual_dazul": int(ano_dazul) if ano_dazul and ano_dazul.isdigit() else None,
                "colecao_atual_dazul": colecao_dazul or None,
                "ano_atual_saron": int(ano_saron) if ano_saron and ano_saron.isdigit() else None,
                "colecao_atual_saron": colecao_saron or None,
            }
            save_data_store(store)
            message = "Configurações atualizadas com sucesso."
            message_type = "success"

    config = get_current_config(store)
    return render_template(
        "admin_config.html",
        export_dir=str(export_dir),
        latest_file=latest_file.name if latest_file else None,
        updated_at=(
            datetime.fromtimestamp(latest_file.stat().st_mtime) if latest_file else None
        ),
        config=config,
        message=message,
        message_type=message_type,
        collection_options=COLLECTION_OPTIONS,
    )


@app.route("/admin/historico", methods=["GET", "POST"])
def admin_historico():
    export_dir = resolve_export_dir()
    latest_file = find_latest_file(export_dir)
    store = load_data_store()
    df = load_report(latest_file) if latest_file else pd.DataFrame()
    if sync_representantes(store, df):
        save_data_store(store)
    message = None
    message_type = "info"

    if request.method == "POST":
        action = request.form.get("action", "add")
        if action == "delete":
            entry_id = request.form.get("entry_id")
            if entry_id:
                entry_id_int = int(entry_id)
                store["historico_metas"] = [
                    item
                    for item in store.get("historico_metas", [])
                    if item.get("id") != entry_id_int
                ]
                save_data_store(store)
                message = "Histórico removido com sucesso."
                message_type = "success"
        else:
            entry_id = request.form.get("entry_id")
            rep_id = request.form.get("representante_id") or ""
            ano = request.form.get("ano")
            colecao = (request.form.get("colecao") or "").upper()
            meta_valor = parse_ptbr_number(request.form.get("meta"))
            realizado_valor = parse_ptbr_number(request.form.get("realizado"))

            errors = []
            if not rep_id:
                errors.append("Informe o representante.")
            if not ano or not ano.isdigit():
                errors.append("Informe um ano válido.")
            if colecao not in COLLECTION_OPTIONS:
                errors.append("Selecione uma coleção válida.")
            if meta_valor is None or meta_valor < 0:
                errors.append("Meta deve ser um número válido maior ou igual a zero.")
            if realizado_valor is not None and realizado_valor < 0:
                errors.append("Realizado deve ser maior ou igual a zero.")

            if not errors:
                historicos = store.setdefault("historico_metas", [])
                entry_id_int = int(entry_id) if entry_id else None
                entry = None
                if entry_id_int:
                    entry = next(
                        (item for item in historicos if item.get("id") == entry_id_int),
                        None,
                    )
                if not entry:
                    rep_entries = [item for item in historicos if item.get("representante_id") == rep_id]
                    if len(rep_entries) >= 3:
                        message = "Cada representante pode ter até 3 coleções no histórico."
                        message_type = "error"
                        return render_template(
                            "admin_historico.html",
                            export_dir=str(export_dir),
                            latest_file=latest_file.name if latest_file else None,
                            updated_at=(
                                datetime.fromtimestamp(latest_file.stat().st_mtime)
                                if latest_file
                                else None
                            ),
                            representantes=store.get("representantes", {}),
                            historico=sorted(
                                historicos,
                                key=lambda item: (
                                    store.get("representantes", {})
                                    .get(item.get("representante_id"), {})
                                    .get("nome", ""),
                                    -int(item.get("ano", 0)),
                                ),
                            ),
                            message=message,
                            message_type=message_type,
                            collection_options=COLLECTION_OPTIONS,
                        )
                    historicos.append(
                        {
                            "id": next_id(historicos),
                            "representante_id": rep_id,
                            "ano": int(ano),
                            "colecao": colecao,
                            "meta": meta_valor,
                            "realizado": realizado_valor,
                            "created_at": datetime.now().isoformat(timespec="seconds"),
                            "updated_at": datetime.now().isoformat(timespec="seconds"),
                        }
                    )
                    message = "Histórico adicionado com sucesso."
                    message_type = "success"
                else:
                    entry.update(
                        {
                            "representante_id": rep_id,
                            "ano": int(ano),
                            "colecao": colecao,
                            "meta": meta_valor,
                            "realizado": realizado_valor,
                            "updated_at": datetime.now().isoformat(timespec="seconds"),
                        }
                    )
                    message = "Histórico atualizado com sucesso."
                    message_type = "success"
                save_data_store(store)
            else:
                message = " ".join(errors)
                message_type = "error"

    historico = sorted(
        store.get("historico_metas", []),
        key=lambda item: (
            store.get("representantes", {})
            .get(item.get("representante_id"), {})
            .get("nome", ""),
            -int(item.get("ano", 0)),
        ),
    )

    return render_template(
        "admin_historico.html",
        export_dir=str(export_dir),
        latest_file=latest_file.name if latest_file else None,
        updated_at=(
            datetime.fromtimestamp(latest_file.stat().st_mtime) if latest_file else None
        ),
        representantes=store.get("representantes", {}),
        historico=historico,
        message=message,
        message_type=message_type,
        collection_options=COLLECTION_OPTIONS,
    )


@app.route("/representante/<rep_id>")
def representante_detail(rep_id: str):
    export_dir = resolve_export_dir()
    latest_file = find_latest_file(export_dir)
    if not latest_file:
        return render_template(
            "representante_detail.html",
            data_loaded=False,
            representante=None,
        )

    df = load_report(latest_file)
    store = load_data_store()
    if sync_representantes(store, df):
        save_data_store(store)
    config = get_current_config(store)
    metas_map = build_metas_map(store, config["ano_atual"], config["colecao_atual"])
    representante = store.get("representantes", {}).get(rep_id)

    vendas_data = None
    if not df.empty:
        for _, row in df.iterrows():
            rep_identifier = normalize_identifier(row.get("CODIGO"), row.get("NOME_VENDEDOR"))
            if rep_identifier == rep_id:
                vendas_data = row.to_dict()
                break

    meta_atual = metas_map.get(rep_id)
    historico = [
        item
        for item in store.get("historico_metas", [])
        if item.get("representante_id") == rep_id
    ]
    historico = sorted(historico, key=lambda item: (-int(item.get("ano", 0)), item.get("colecao", "")))

    return render_template(
        "representante_detail.html",
        data_loaded=True,
        representante=representante,
        vendas_data=vendas_data,
        config=config,
        meta_atual=meta_atual,
        historico=historico,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=False)
