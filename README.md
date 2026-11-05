# 06 — Automação de boletins

> **Automações** · Status: **MVP validado**  
> Parte do portfólio [Beatriz Helena — Engenharia de IA](https://beatrizorlandi.github.io/projetos/boletins-verificaveis.html)

Boletins por biblioteca produzidos a partir de métricas aprovadas, com conferência numérica e idempotência.

**Stack principal:** `Python` · `SQL` · `Delta Lake` · `Databricks Jobs` · `MLflow`

---

## O Problema

Redigir relatórios manualmente repete trabalho e pode introduzir números que não correspondem aos dados publicados.

### Escopo Entregue

Três boletins mensais de setembro de 2026 gerados por template Python a partir da release Gold aprovada. O fluxo faz parte do pipeline de sete tarefas e salva as saídas no Databricks.

---

## Arquitetura e Fluxo

**1. Gold aprovada** → **2. Métricas por biblioteca** → **3. Template Python** → **4. Conferência numérica** → **5. Chave idempotente** → **6. Salvar boletim**

### Decisões Técnicas

- Calcular datas e números por código, com referência à release utilizada.
- Evitar duplicação ao repetir biblioteca, período e configuração.
- Começar com template determinístico e execução manual para estabelecer uma referência verificável.

---

## Resultados Medidos

| Métrica / Indicador | Valor Observado |
| --- | --- |
| **3** | boletins gerados |
| **1** | por biblioteca fictícia |
| **0** | envios externos |

Geração concluída na execução de 28/10/2026. O ensaio de integração confirmou que repetir a geração mantém três boletins, sem duplicá-los.


---

## Limites Conhecidos e Próximos Passos

### Limites Conhecidos
- Template Python, sem redação por LLM.
- MVP mensal com execução manual; agenda, envio e fluxo completo de dry-run continuam planejados.

### Próximos Incrementos
- Formalizar dry-run e revisão antes de eventual agendamento.
- Comparar redação por LLM com o template mantendo os mesmos fatos e checks.

---

## Estrutura do Repositório

- `informacoes_tecnicas/PLANEJAMENTO.md`: Planejamento técnico detalhado (requisitos, contratos, arquitetura, MLflow e validação).
- `00_PLANEJAMENTO.py`: Versão em notebook Databricks do planejamento técnico.
- Notebooks e módulos em Python/PySpark (quando aplicável ao estágio de implementação do estudo).
- `evidencias/` (quando aplicável): Resumo estático das execuções validadas no Databricks.

## Links

- **Portfólio Completo:** [https://beatrizorlandi.github.io](https://beatrizorlandi.github.io)
- **Estudo de Caso no Site:** [https://beatrizorlandi.github.io/projetos/boletins-verificaveis.html](https://beatrizorlandi.github.io/projetos/boletins-verificaveis.html)
- **Planejamento Técnico no Site:** [https://beatrizorlandi.github.io/documentacao/boletins-verificaveis.html](https://beatrizorlandi.github.io/documentacao/boletins-verificaveis.html)
