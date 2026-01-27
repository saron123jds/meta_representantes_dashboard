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
METAS_FILE = DATA_DIR / "metas.json"

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


def ensure_metas_store() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not METAS_FILE.exists():
        METAS_FILE.write_text(
            json.dumps({"periodos": {}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def load_metas_store() -> dict:
    ensure_metas_store()
    try:
        data = json.loads(METAS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        data = {"periodos": {}}
    if "periodos" in data:
        return data
    if "metas" in data:
        period = data.get("periodo") or current_period()
        period_data = {
            period: {
                "metas": data.get("metas", {}),
                "updated_at": data.get("updated_at"),
            }
        }
        return {"periodos": period_data}
    return {"periodos": {}}


def save_metas_store(data: dict) -> None:
    ensure_metas_store()
    METAS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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
    code_column = pick_column(
        [
            "CODIGO",
            "COD",
            "CODIGO_VENDEDOR",
            "CODIGO_REPRESENTANTE",
            "ID_REPRESENTANTE",
            "ID_REP",
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
        "QTDEITEM": ["ITENS", "ITENS_VENDIDOS", "QTDE_ITEM"],
        "TOTAL_PEDIDOS": ["PEDIDOS", "QTDE_PEDIDOS", "TOTALPEDIDOS"],
        "TOTAL_CLIENTES": ["CLIENTES", "TOTALCLIENTES"],
        "CLIENTES_ATIVOS": ["ATIVOS", "CLIENTES_ATIVOS", "ATIVOS_CARTEIRA"],
        "CLIENTES_NOVOS": ["CLIENTES_NOVOS", "NOVOS_CLIENTES"],
        "VLR_LIQUIDO": ["VALOR_LIQUIDO", "VLR_LIQUIDO", "VENDAS_LIQUIDAS", "VALOR"],
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
    for _, row in df.iterrows():
        rep_id = normalize_identifier(row.get("CODIGO"), row.get("NOME_VENDEDOR"))
        meta_data = metas_map.get(rep_id, {})
        meta_valor = meta_data.get("meta_valor")
        vlr_liquido = float(row.get("VLR_LIQUIDO") or 0)
        if meta_valor and meta_valor > 0:
            vendedores_com_meta += 1
            total_meta += meta_valor
            total_vendas_com_meta += vlr_liquido
            gap_total += max(meta_valor - vlr_liquido, 0.0)
            if vlr_liquido / meta_valor >= 1:
                vendedores_acima_meta += 1
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

    vendedores_abaixo_meta = 0
    vendedores_sem_clientes_ativos = 0
    if metas_map and "VLR_LIQUIDO" in df.columns:
        for _, row in df.iterrows():
            rep_id = normalize_identifier(row.get("CODIGO"), row.get("NOME_VENDEDOR"))
            meta_data = metas_map.get(rep_id, {})
            meta_valor = meta_data.get("meta_valor")
            vendas = float(row.get("VLR_LIQUIDO") or 0)
            if meta_valor and meta_valor > 0:
                percentual = vendas / meta_valor
                if percentual < 0.5:
                    vendedores_abaixo_meta += 1
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
        valor_meta = safe_value(meta_data.get("meta_valor"), 0.0)
        meta_pedidos = safe_value(meta_data.get("meta_pedidos"), 0.0)
        vlr_liquido = safe_value(row.get("VLR_LIQUIDO"), 0.0)
        percentual_meta = None
        gap_meta = None
        meta_status_class = "status-neutral"
        meta_status_label = "Meta pendente"
        percentual_pedidos = None

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

        if meta_pedidos and meta_pedidos > 0:
            percentual_pedidos = safe_value(row.get("TOTAL_PEDIDOS"), 0) / meta_pedidos

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
                "meta_pedidos": meta_pedidos,
                "percentual_meta": percentual_meta,
                "percentual_pedidos": percentual_pedidos,
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
    period = normalize_period(request.args.get("periodo"))
    metas_store = load_metas_store()
    period_data = metas_store.get("periodos", {}).get(period, {})
    metas_map = period_data.get("metas", {})
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
        metas_periodo=period,
        metas_updated_at=period_data.get("updated_at"),
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
    period = normalize_period(request.values.get("periodo"))
    metas_store = load_metas_store()
    period_data = metas_store.setdefault("periodos", {}).setdefault(
        period, {"metas": {}, "updated_at": None}
    )
    metas_map = period_data.setdefault("metas", {})

    message = None
    message_type = "info"

    df = load_report(latest_file) if latest_file else pd.DataFrame()

    if request.method == "POST":
        action = request.form.get("action", "salvar")
        if action == "zerar":
            metas_map.clear()
            period_data["updated_at"] = datetime.now().isoformat(timespec="seconds")
            save_metas_store(metas_store)
            message = "Metas do período zeradas com sucesso."
            message_type = "success"
        else:
            rep_total = int(request.form.get("rep_total", 0))
            salvar_id = request.form.get("salvar_id")
            target_index = int(salvar_id) if salvar_id else None
            errors = []

            for index in range(rep_total):
                if target_index is not None and index != target_index:
                    continue
                rep_id = request.form.get(f"rep_id_{index}") or ""
                rep_nome = request.form.get(f"rep_nome_{index}") or ""
                meta_valor = parse_ptbr_number(request.form.get(f"meta_valor_{index}"))
                meta_pedidos = parse_ptbr_number(
                    request.form.get(f"meta_pedidos_{index}")
                )

                if meta_valor is not None and meta_valor < 0:
                    errors.append("Meta de valor deve ser maior ou igual a zero.")
                if meta_pedidos is not None and meta_pedidos < 0:
                    errors.append("Meta de pedidos deve ser maior ou igual a zero.")

                if not rep_id:
                    continue

                if meta_valor is None and meta_pedidos is None:
                    metas_map.pop(rep_id, None)
                else:
                    metas_map[rep_id] = {
                        "nome": rep_nome,
                        "meta_valor": meta_valor or 0,
                        "meta_pedidos": meta_pedidos or 0,
                    }

            if errors:
                message = " ".join(errors)
                message_type = "error"
            else:
                period_data["updated_at"] = datetime.now().isoformat(timespec="seconds")
                save_metas_store(metas_store)
                message = "Metas salvas com sucesso."
                message_type = "success"

    representantes = []
    if not df.empty:
        for _, row in df.iterrows():
            codigo = row.get("CODIGO")
            nome = row.get("NOME_VENDEDOR") or "-"
            rep_id = normalize_identifier(codigo, nome)
            meta_data = metas_map.get(rep_id, {})
            meta_valor = meta_data.get("meta_valor")
            meta_pedidos = meta_data.get("meta_pedidos")
            status = "Cadastrada" if (meta_valor or meta_pedidos) else "Pendente"
            representantes.append(
                {
                    "codigo": codigo or "-",
                    "nome": nome,
                    "identificador": rep_id,
                    "meta_valor": meta_valor,
                    "meta_pedidos": meta_pedidos,
                    "status": status,
                }
            )

    return render_template(
        "admin_metas.html",
        export_dir=str(export_dir),
        latest_file=latest_file.name if latest_file else None,
        updated_at=(
            datetime.fromtimestamp(latest_file.stat().st_mtime) if latest_file else None
        ),
        periodo=period,
        metas_updated_at=period_data.get("updated_at"),
        representantes=representantes,
        message=message,
        message_type=message_type,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=False)
