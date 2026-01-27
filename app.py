import json
import os
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, render_template

DEFAULT_EXPORT_DIR = r"C:\\META REPRESENTANTES\\Exporta"
SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv"}

app = Flask(__name__)


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
    total_clientes = int(df["TOTAL_CLIENTES"].sum()) if "TOTAL_CLIENTES" in df.columns else 0
    ativos = int(df["CLIENTES_ATIVOS"].sum()) if "CLIENTES_ATIVOS" in df.columns else 0
    novos = int(df["CLIENTES_NOVOS"].sum()) if "CLIENTES_NOVOS" in df.columns else 0

    ranking_vendas = (
        df.sort_values("VLR_LIQUIDO", ascending=False)
        .head(5)[["NOME_VENDEDOR", "VLR_LIQUIDO"]]
        .to_dict(orient="records")
        if "VLR_LIQUIDO" in df.columns
        else []
    )

    return {
        "total_vendedores": total_vendedores,
        "total_itens": total_itens,
        "total_vendas": total_vendas,
        "total_clientes": total_clientes,
        "clientes_ativos": ativos,
        "clientes_novos": novos,
        "ranking_vendas": ranking_vendas,
    }


def build_insights(df: pd.DataFrame) -> dict:
    insights = {}
    if "VLR_LIQUIDO" in df.columns:
        insights["ticket_medio"] = float(df["VLR_LIQUIDO"].sum()) / max(
            df["TOTAL_PEDIDOS"].sum(), 1
        )
    if "QTDEITEM" in df.columns:
        insights["media_itens_por_pedido"] = float(df["QTDEITEM"].sum()) / max(
            df["TOTAL_PEDIDOS"].sum(), 1
        )
    if "CLIENTES_ATIVOS" in df.columns:
        insights["taxa_clientes_ativos"] = (
            float(df["CLIENTES_ATIVOS"].sum()) / max(df["TOTAL_CLIENTES"].sum(), 1)
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
        vendedores.append(
            {
                "codigo": row.get("VENDEDOR"),
                "nome": row.get("NOME_VENDEDOR"),
                "qtde_item": safe_value(row.get("QTDEITEM")),
                "total_pedidos": safe_value(row.get("TOTAL_PEDIDOS")),
                "total_clientes": safe_value(row.get("TOTAL_CLIENTES")),
                "clientes_ativos": safe_value(row.get("CLIENTES_ATIVOS")),
                "qtde_media": safe_value(row.get("QTDE_MEDIA")),
                "vlr_liquido": safe_value(row.get("VLR_LIQUIDO")),
                "preco_medio": safe_value(row.get("PRECO_MEDIO")),
                "media_pedidos": safe_value(row.get("MEDIA_PEDIDOS")),
                "clientes_novos": safe_value(row.get("CLIENTES_NOVOS")),
                "valor_meta": safe_value(row.get("VALOR_META_COLECAO"), "-"),
                "qtde_meta": safe_value(row.get("QTDE_META_COLECAO"), "-"),
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=False)
