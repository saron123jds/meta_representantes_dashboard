# Meta Representantes - Dashboard de Ranking de Vendas

Sistema local que lê automaticamente o arquivo mais recente na pasta `C:\META REPRESENTANTES\Exporta` e mostra o ranking completo de vendas no navegador, incluindo metas individuais por representante.

## Como rodar

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Garanta que o relatório atualizado esteja salvo em `C:\META REPRESENTANTES\Exporta` (ou na pasta configurada).
3. Execute:
   ```bash
   python app.py
   ```
4. Abra `http://localhost:9000`.

## Onde colocar o CSV

- O sistema procura o arquivo mais recente com extensão `.xlsx`, `.xls` ou `.csv` dentro da pasta configurada.
- Por padrão a pasta é `C:\META REPRESENTANTES\Exporta`.
- Para usar outra pasta, defina a variável de ambiente `EXPORT_DIR` antes de rodar:
  ```bash
  set EXPORT_DIR=C:\\META REPRESENTANTES\\Exporta
  python app.py
  ```

## Como cadastrar metas por representante

1. Abra `http://localhost:9000/admin/metas`.
2. Selecione o período (YYYY-MM) e clique em **Carregar**.
3. Informe a **Meta (R$)** e, opcionalmente, **Meta pedidos** por representante.
4. Clique em **Salvar** por linha ou **Salvar tudo**.
5. As metas ficam armazenadas localmente em `data/metas.json` e persistem mesmo após atualizar o CSV.

## Observações

- Se um representante não tiver meta cadastrada, ele aparecerá como **Meta pendente** no dashboard.
- Nenhuma API externa ou banco de dados é usado; toda a persistência é feita em arquivo JSON local.
