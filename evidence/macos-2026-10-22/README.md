# Execução no macOS — dados para o relatório

**Data:** 22/10/2026 (America/Sao_Paulo).

## Resultado e alteração

- 28/28 testes automatizados aprovados, incluindo quatro testes de integração com processos reais.
- 24/24 cenários da matriz completa aprovados.
- 48 arquivos baixados por leechers, todos com tamanho e SHA-256 corretos.
- O benchmark registra 72 verificações: 24 arquivos-fonte e 48 downloads. Não são 72 downloads.
- Auditoria independente posterior releu 96 arquivos: as mesmas 24 fontes e 48 saídas baixadas, mais 24 saídas montadas pelos seeders.
- Compartilhamento antes da conclusão em 12/12 cenários de quatro peers: 45346 eventos de envio de B/C para D com inventário parcial.
- 184472 eventos de recebimento. Para cada leecher, todos os índices esperados apareceram exatamente uma vez nos logs.
- Todos os 72 processos tiveram stderr vazio.

A única alteração no código é em `tests/test_protocol.py`: as expectativas de `directory` e `seed` agora usam `.resolve()`, assim como a configuração. Isso trata a equivalência entre `/var` e `/private/var` no macOS. Não houve mudança no protocolo nem na implementação de transferência.

## Ambiente

- CPU: Apple M5; memória: 16 GiB; arquitetura: arm64.
- macOS 27.0, build 26A428; Python 3.12.14 do runtime disponível no Codex.
- O Python 3.9 do sistema está abaixo do mínimo 3.10 declarado pelo projeto; não foi usado nesta execução.
- Revisão-base local: `71147a530ba2d4f30a78b4bd93e15571929c426a`, com a correção de teste não commitada.
- Transporte: TCP em `127.0.0.1`; um processo independente por peer; uma execução por cenário.

## Tempos por cenário

Tempo em segundos desde o início do primeiro processo até a conclusão de todos os leechers. Inclui inicialização, transferência, verificação e montagem; exclui preparação de arquivo, manifesto e configurações. Não são médias.

| Arquivo | 2 peers / 1 KiB | 2 peers / 4 KiB | 4 peers / 1 KiB | 4 peers / 4 KiB |
|---|---:|---:|---:|---:|
| 10 KiB | 0.075 | 0.088 | 0.195 | 0.189 |
| 20 KiB | 0.087 | 0.087 | 0.215 | 0.171 |
| 1 MiB | 0.676 | 0.228 | 0.835 | 0.335 |
| 5 MiB | 3.114 | 0.847 | 3.104 | 0.979 |
| 10 MiB | 6.409 | 1.640 | 7.742 | 1.983 |
| 20 MiB | 13.681 | 3.228 | 16.012 | 3.889 |

## Fragmentação e compartilhamento parcial

| Arquivo | Blocos / 1 KiB | Blocos / 4 KiB | Envios parciais a D / 1 KiB | Envios parciais a D / 4 KiB |
|---|---:|---:|---:|---:|
| 10 KiB | 10 | 3 | 6 | 1 |
| 20 KiB | 20 | 5 | 17 | 3 |
| 1 MiB | 1024 | 256 | 1014 | 247 |
| 5 MiB | 5120 | 1280 | 5035 | 1237 |
| 10 MiB | 10240 | 2560 | 10216 | 2552 |
| 20 MiB | 20480 | 5120 | 19989 | 5029 |

Os números de envios parciais são contagens de eventos de envio, não uma medida de bytes únicos ou de ganho de desempenho. A evidência usa `have < total` após a escrita da mensagem.

## Interpretação para o relatório

Nos arquivos de 1 a 20 MiB, a execução com blocos de 4 KiB levou menos tempo em todas as combinações de peers. A fragmentação em 4 KiB reduz aproximadamente a um quarto o número de blocos e de solicitações em comparação a 1 KiB. Essa observação é compatível com menor custo de mensagens, hashes, logs e operações de arquivo; o experimento não separa causalmente esses custos.

| Arquivo | Razão tempo 1 KiB / 4 KiB — 2 peers | Razão tempo 1 KiB / 4 KiB — 4 peers |
|---|---:|---:|
| 1 MiB | 2.97 | 2.49 |
| 5 MiB | 3.68 | 3.17 |
| 10 MiB | 3.91 | 3.91 |
| 20 MiB | 4.24 | 4.12 |

Com dois peers, A é a fonte e B o receptor. Com quatro, as conexões formam A–B–D e A–C–D; D não conhece A e recebeu exclusivamente de B/C. B e C podem obter blocos de A e D. A topologia demonstra propagação entre participantes, inclusive enquanto B/C ainda estão incompletos.

Os testes automatizados complementam a matriz com último bloco parcial, arquivo menor que um bloco, arquivo vazio, corrupção, retomada, seed tardio, mensagens TCP fragmentadas/truncadas, entradas inválidas e timeouts. A matriz usa tamanhos maiores que zero; o caso vazio está na suíte de armazenamento.

## Limites de interpretação

- Uma execução por combinação: não há desvio-padrão, intervalo de confiança ou média estatística.
- Todos os peers compartilham CPU, disco e sistema operacional; não foi medida uma rede entre máquinas físicas.
- Os cenários de quatro peers envolvem três downloads, contra um nos cenários de dois peers, e usam topologia diferente. Não isolam o efeito do número de peers.
- Nos cenários de quatro peers com 10/20 KiB foi aplicado atraso de 3 ms por bloco. Os demais usam zero. Não comparar esses tempos como se todos tivessem a mesma condição.
- KiB = 1024 bytes e MiB = 1048576 bytes, convenção explicitada para os KB/MB do enunciado.
- Eventos de conexão/timeout/retry podem ocorrer por inicialização e encerramento ou conexões ociosas. Os arquivos finais foram validados separadamente; tais eventos não são contagem de arquivos corrompidos.
- Os dados antigos de Windows permanecem separados. Diferenças entre execuções não isolam o efeito de sistema operacional ou hardware.

## Eventos de rede observados

Foram registrados 684 eventos `neighbor_error`, 684 `retry` e dois `connection_error`. Os dois últimos informam `[Errno 54] Connection reset by peer`. Todos os cenários concluíram com integridade correta, todos os processos tiveram stderr vazio e todas as linhas de log foram interpretadas como JSON. Os eventos não foram ocultados; os motivos completos estão em `validation.json` e os logs integrais permanecem disponíveis. A ordem de inicialização inicia receptores antes da fonte e pode gerar tentativas de conexão recusadas; não há aqui um experimento isolando a causa de cada reinicialização de conexão.

## Arquivos e reprodução

- [tests.txt](tests.txt): saída completa dos 28 testes.
- [benchmark.txt](benchmark.txt): saída dos 24 cenários.
- [environment.json](environment.json): ambiente, revisão-base e horários UTC.
- [validation.json](validation.json): totais e verificações independentes.
- [results.csv](results.csv) e [results.json](results.json): resultados originais do benchmark, hashes e eventos selecionados.
- [case-summary.csv](case-summary.csv): resumo ampliado por cenário, incluindo quantidade de blocos.
- [per-peer.csv](per-peer.csv): 72 linhas com progresso, origens, tempos, contagem de eventos e caminho do log de cada processo.
- [file-verifications.csv](file-verifications.csv): 96 verificações independentes, separadas por fonte, saída do seeder e download.
- [selected-events.jsonl](selected-events.jsonl): amostras originais de eventos.
- Arquivos originais, blocos, configurações e logs integrais: `.runtime/benchmark-macos-2026-10-22/` na raiz do repositório. Essa pasta é ignorada pelo Git; não apagar antes de terminar o relatório se quiser preservar os dados brutos.

Com Python 3.10 ou superior, a partir da raiz:

```console
python -m unittest discover -s tests -v
python scripts/benchmark.py --output evidence/nova-execucao --work-dir .runtime/nova-execucao --timeout 600
```

Os CSVs adicionais e a auditoria de 96 arquivos foram produzidos nesta revisão a partir das configurações, logs integrais e conteúdo em disco. O benchmark padrão gera os arquivos `results.*` e `selected-events.jsonl`; não gera automaticamente estes CSVs adicionais.

O relatório PDF existente não foi substituído. Não houve commit, push, publicação ou alteração da visibilidade do repositório.
