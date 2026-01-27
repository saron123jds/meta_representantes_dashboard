import json
import os
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, render_template, request
from werkzeug.utils import secure_filename

DEFAULT_EXPORT_DIR = r"C:\\META REPRESENTANTES\\Exporta"
SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv"}

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
        "VALOR_META_COLECAO",
        "QTDE_META_COLECAO",
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
    df.columns = [column.strip().upper() for column in df.columns]
    df = normalize_numeric_columns(df)
    return df


def build_summary(df: pd.DataFrame) -> dict:
    total_vendedores = df.shape[0]
    total_itens = int(df["QTDEITEM"].sum()) if "QTDEITEM" in df.columns else 0
    total_vendas = float(df["VLR_LIQUIDO"].sum()) if "VLR_LIQUIDO" in df.columns else 0.0
    total_pedidos = int(df["TOTAL_PEDIDOS"].sum()) if "TOTAL_PEDIDOS" in df.columns else 0
    total_clientes = int(df["TOTAL_CLIENTES"].sum()) if "TOTAL_CLIENTES" in df.columns else 0
    ativos = int(df["CLIENTES_ATIVOS"].sum()) if "CLIENTES_ATIVOS" in df.columns else 0
    novos = int(df["CLIENTES_NOVOS"].sum()) if "CLIENTES_NOVOS" in df.columns else 0
    total_meta = (
        float(df["VALOR_META_COLECAO"].sum()) if "VALOR_META_COLECAO" in df.columns else 0.0
    )
    percentual_meta = total_vendas / total_meta if total_meta else 0.0
    meta_gap = total_meta - total_vendas
    valor_faltante = max(meta_gap, 0.0)
    valor_excedente = max(-meta_gap, 0.0)

    if total_meta <= 0:
        meta_status_label = "Meta não definida"
        meta_status_class = "status-neutral"
        meta_status_detail = "Sem meta cadastrada para o período."
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
    vendedores_sem_meta = 0
    vendedores_sem_clientes_ativos = 0
    if "VALOR_META_COLECAO" in df.columns and "VLR_LIQUIDO" in df.columns:
        metas = df["VALOR_META_COLECAO"].fillna(0)
        vendas = df["VLR_LIQUIDO"].fillna(0)
        percentual_meta_vendedor = vendas.div(metas.where(metas > 0, pd.NA))
        vendedores_abaixo_meta = int((percentual_meta_vendedor < 0.5).fillna(False).sum())
        vendedores_sem_meta = int((metas <= 0).sum())
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
        "ranking_vendas": ranking_vendas,
        "ranking_piores": ranking_piores,
        "vendedor_destaque": vendedor_destaque,
        "vendedores_abaixo_meta": vendedores_abaixo_meta,
        "vendedores_sem_meta": vendedores_sem_meta,
        "vendedores_sem_clientes_ativos": vendedores_sem_clientes_ativos,
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


def build_vendedores(df: pd.DataFrame) -> list[dict]:
    df = df.copy()
    df["NOME_VENDEDOR"] = df.get("NOME_VENDEDOR", "-")
    df = df.sort_values("VLR_LIQUIDO", ascending=False)
    vendedores = []

    def safe_value(value, default=0):
        if pd.isna(value):
            return default
        return value

    for _, row in df.iterrows():
        valor_meta = safe_value(row.get("VALOR_META_COLECAO"), 0.0)
        vlr_liquido = safe_value(row.get("VLR_LIQUIDO"), 0.0)
        percentual_meta = None
        gap_meta = None
        meta_status_class = "status-neutral"
        meta_status_label = "Sem meta"

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
                "codigo": row.get("VENDEDOR"),
                "nome": row.get("NOME_VENDEDOR"),
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
                "qtde_meta": safe_value(row.get("QTDE_META_COLECAO"), 0),
                "percentual_meta": percentual_meta,
                "gap_meta": gap_meta,
                "meta_status_class": meta_status_class,
                "meta_status_label": meta_status_label,
            }
        )
    return vendedores


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
    summary = build_summary(df)
    insights = build_insights(df)
    vendedores = build_vendedores(df)

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
        summary=summary,
        insights=insights,
        vendedores=vendedores,
        chart_data=chart_data,
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=False)
