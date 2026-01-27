# Meta Representantes - Dashboard de Ranking de Vendas

Sistema local que lê automaticamente o arquivo mais recente na pasta `C:\META REPRESENTANTES\Exporta` e mostra o ranking completo de vendas no navegador.

## Como rodar

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Garanta que o relatório atualizado esteja salvo em `C:\META REPRESENTANTES\Exporta`.
3. Execute:
   ```bash
   python app.py
   ```
4. Abra `http://localhost:9000`.

## Configuração opcional

Para usar outra pasta de exportação, defina a variável de ambiente `EXPORT_DIR` antes de rodar.

```bash
set EXPORT_DIR=C:\\META REPRESENTANTES\\Exporta
python app.py
```

## Observações

- O sistema procura o arquivo mais recente com extensão `.xlsx`, `.xls` ou `.csv`.
- Não é necessário configurar API nem banco de dados.
