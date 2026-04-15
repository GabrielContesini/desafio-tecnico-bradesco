# Sample Data

Os arquivos deste diretório representam exemplos lógicos de nomes de arquivo e payloads.

- `parts/example_parts.jsonl`: exemplos de PART, incluindo duplicidade.
- `manifests/example_manifests.jsonl`: exemplos de MANIFEST, incluindo conflito.

Para gerar arquivos físicos de demo (em ambiente Linux/Databricks), use:

```bash
python -m ingestion_orchestrator.jobs.simulate_landing --output-path sample_data/demo_landing
```
