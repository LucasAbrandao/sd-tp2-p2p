# Evidências da execução

Execução local em 18/10/2026, Windows 11, Python 3.13.0.

- 28 testes automatizados aprovados: `tests.txt`.
- 24 cenários de transferência aprovados: `results.csv` e `results.json`.
- 72 verificações de tamanho e SHA-256, incluindo os arquivos fontes.
- Compartilhamento parcial confirmado nos 12 cenários com quatro peers.
- Amostras reais dos logs: `selected-events.jsonl`.
- Resumo do ambiente e das verificações: `validation.json`.

Cada cenário usou um arquivo aleatório novo, processos separados e TCP em loopback. Foi feita uma execução por combinação. Os tempos incluem inicialização e montagem, e não constituem médias estatísticas. A geração do arquivo e do manifesto não entra no tempo de parede.

As topologias diferem: com dois peers há uma ligação A/B; com quatro, D só conhece B/C e não acessa a fonte A. Nos arquivos de até 20 KiB com quatro peers, houve atraso artificial de 3 ms por envio de bloco. Esses parâmetros estão explícitos nos resultados.

Para repetir: `python scripts/benchmark.py --output evidence --timeout 600`, a partir da raiz do projeto. Por padrão, os arquivos aleatórios, configurações e logs completos da nova execução ficam em `.runtime/benchmark`, fora do Git. Os dados da execução entregue foram gerados em uma pasta de trabalho separada.
