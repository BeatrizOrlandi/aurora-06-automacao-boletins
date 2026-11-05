# Databricks notebook source

# MAGIC %md
# MAGIC # 06 — Automação de boletins verificáveis
# MAGIC
# MAGIC **Atualização em 28/10/2026:** MVP implementado e validado: três boletins por template; sem agenda ou envio externo. Resultados, limites e reprodução (no workspace Databricks).
# MAGIC
# MAGIC
# MAGIC **Estado do plano original:** somente planejado; nenhuma agenda será ativada nesta etapa. **Competência:** operar um fluxo recorrente de dados/IA com idempotência e rastreabilidade. **Dependências:** 01 e 02; redação avaliada pelo protocolo do 04. O 05 é integração opcional, não um bloqueio para o primeiro boletim.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivo e requisitos
# MAGIC
# MAGIC Transformar métricas aprovadas em um boletim semanal salvo no Databricks, com conferência numérica e histórico de execução.
# MAGIC
# MAGIC | ID | Requisito | Aceite futuro |
# MAGIC | --- | --- | --- |
# MAGIC | RF01 | Calcular período e métricas por código | Período, timezone e comparação inequívocos |
# MAGIC | RF02 | Consumir somente release Gold aprovada | ID da release e checks anexados |
# MAGIC | RF03 | Gerar rascunho por template ou LLM | Mesmo contrato de saída nas duas variantes |
# MAGIC | RF04 | Validar números e referências | Nenhuma métrica inventada na publicação |
# MAGIC | RF05 | Evitar duplicação por repetição/retry | Uma saída lógica por chave do boletim |
# MAGIC | RF06 | Recuperar falhas intermediárias | Retomar sem recalcular/publicar incorretamente |
# MAGIC | RF07 | Separar dry-run de publicação interna | Dry-run grava apenas evidência de teste identificada |
# MAGIC | RNF01 | Manter operação barata e compreensível | Execução manual antes de agenda; sem canal externo |
# MAGIC
# MAGIC Fora do MVP: e-mails, Slack, WhatsApp, postagem pública e acionamento autônomo de pessoas. A entrega é uma tabela/arquivo no próprio Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entradas, saídas e contratos
# MAGIC
# MAGIC Parâmetros: `library_id`, `period_start`, `period_end_exclusive`, `comparison_start`, `comparison_end_exclusive`, `data_reference`, `config_version`, `dry_run`.
# MAGIC
# MAGIC Período inicial proposto: semana anterior fechada, segunda a domingo na timezone `America/Sao_Paulo`, usando limites inclusivo/exclusivo convertidos para UTC. A comparação deve ter a mesma definição de duração/calendário. Feriados e número de dias podem afetar a interpretação, sem inferir causalidade.
# MAGIC
# MAGIC Tabelas propostas em `workspace.estudos_ai`: `aurora_boletim_execucoes`, `aurora_boletins`, `aurora_boletim_metricas`. Chave lógica: biblioteca + período + versão de configuração. Guardar tentativas com execution_id separado, sem duplicar o boletim.
# MAGIC
# MAGIC Resposta estruturada: `status`, `text`, `metric_references`, `warnings`, `period`, `gold_release_id`, `prompt_version`. Métricas recebem IDs; o texto usa esses IDs para rastrear cada observação.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquitetura e operação
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     P[Resolver parâmetros] --> Q[Confirmar qualidade e release]
# MAGIC     Q --> M[Consultar e calcular métricas]
# MAGIC     M --> C[Construir fatos canônicos]
# MAGIC     C --> R[Redigir template ou LLM]
# MAGIC     R --> V[Validar schema, números e referências]
# MAGIC     V --> G{Passou e não é dry-run?}
# MAGIC     G -->|sim| S[Salvar boletim aprovado]
# MAGIC     G -->|não| D[Salvar diagnóstico de teste ou bloqueio]
# MAGIC ```
# MAGIC
# MAGIC Implementar estados requested → validated_data → drafted → checked → published ou blocked/failed. Publicação interna usa upsert idempotente e guarda hash do conteúdo. Se a chave já existir com entradas e configuração iguais, retornar a saída existente; se a Gold mudar, gerar revisão explícita, preservando o original.
# MAGIC
# MAGIC Retries apenas para falhas transitórias e em tarefas idempotentes: até 2 novas tentativas. Falha de conferência é bloqueio, não motivo para repetir o LLM indefinidamente. Uma chamada externa de LLM pode ser repetida após falha de rede sem garantia de não ter consumido tokens: registrar tentativas e tratar consumo desconhecido como desconhecido.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tecnologias e bibliotecas
# MAGIC
# MAGIC | Componente/lib | Uso |
# MAGIC | --- | --- |
# MAGIC | Lakeflow Jobs | DAG, parâmetros, dependências e futura agenda |
# MAGIC | SQL/PySpark e Delta | Métricas, tabelas de estado e upsert |
# MAGIC | Python `datetime`, `zoneinfo`, `decimal` | Datas, timezone e arredondamento de valores |
# MAGIC | `pydantic` | Fatos canônicos e saída do redator |
# MAGIC | `jinja2` | Baseline determinístico de texto |
# MAGIC | `databricks-sdk` | Endpoint de redação, se disponível |
# MAGIC | MLflow | Experimentos de redação e traces de validação |
# MAGIC
# MAGIC O LLM não calcula números nem decide se a publicação deve ser liberada. O validador compara fatos estruturados e unidades, não apenas uma regex sobre texto livre. Preferir template com fatos marcados; texto livre adicional não pode introduzir novos números.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experimentos MLflow e métricas
# MAGIC
# MAGIC Experimento: `.../06_automacoes/experimentos/boletins`. Preparar 12 cenários de períodos, incluindo queda, aumento, divisão por zero, dados parciais, backfill e nenhuma atividade. Reservar 4 para teste após fechar a redação.
# MAGIC
# MAGIC | ID | Comparação | Métrica |
# MAGIC | --- | --- | --- |
# MAGIC | B01 | Template × LLM | correção de fatos, clareza e custo |
# MAGIC | B02 | Fatos canônicos completos × faltantes | bloqueio/abstenção corretos |
# MAGIC | B03 | Execução inicial × retry após falha | saídas duplicadas e retomada |
# MAGIC
# MAGIC Parâmetros: janela, template/prompt, endpoint, arredondamento, limites e config. Métricas: validade numérica, completude, publicações indevidas, duplicação, sucesso operacional, latência e chamadas. Artefatos: fatos de entrada, rascunho, relatório de verificação e decisão de publicação. MLflow não substitui o ledger operacional em Delta.
# MAGIC
# MAGIC Limite inicial: até 24 redações por rodada; simular falhas de infraestrutura com fixtures antes de consumir o endpoint. Meta de integridade: zero número sem fato correspondente e zero duplicação nos cenários testados. Avaliação de estilo usa rubrica de 1–5 com exemplos, não apenas preferência informal.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação, roteiro e demonstração
# MAGIC
# MAGIC Casos: mesma semana duas vezes; falha após redigir e antes de salvar; mudança da Gold; denominador zero; atraso de dados; timezone na virada da semana; revisão de configuração; rascunho com número trocado; timeout; dois disparos da mesma chave.
# MAGIC
# MAGIC Roteiro: contrato de fatos → template baseline → conferência → persistência/idempotência → Job manual → LLM opcional → ensaio de falha → agenda apenas quando solicitada e validada. Entregas planejadas: `01_periodos_metricas`, `02_redacao`, `03_validacao`, `04_publicacao_interna`, configuração de Job e runbook.
# MAGIC
# MAGIC **Demonstração:** uma execução completa, o boletim resultante, o vínculo de um número ao SQL de origem, repetição sem duplicação e bloqueio de um texto propositalmente incorreto. Mostrar também uma saída de dry-run claramente identificada.
# MAGIC
# MAGIC Riscos: períodos inconsistentes, revisão de dados sem versionamento e concorrência. A implementação deve validar a estratégia de serialização/upsert disponível; uma checagem “existe?” seguida de inserção não basta como garantia de exclusão mútua.
# MAGIC
# MAGIC Referências: [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/) e [MLflow Tracking](https://mlflow.org/docs/latest/ml/tracking/).
