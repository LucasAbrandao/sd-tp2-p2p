# Transferência de arquivos Peer-to-Peer

Implementação do Trabalho Prático 2 de Sistemas Distribuídos, CEFET-MG, 2026/2. Cada peer é um processo Python que recebe conexões TCP e, ao mesmo tempo, solicita blocos aos seus vizinhos. Um bloco baixado e verificado já pode ser compartilhado, mesmo que o arquivo inteiro ainda não esteja pronto.

O projeto usa apenas a biblioteca padrão do Python. O código está organizado em módulos pequenos, com comentários voltados às decisões de concorrência, validação e armazenamento.

## O que foi implementado

- Divisão de arquivos em blocos de tamanho fixo, com tratamento do último bloco menor.
- Metadados em JSON, com tamanho do arquivo, tamanho dos blocos, SHA-256 de cada bloco e SHA-256 do arquivo inteiro.
- Cliente e servidor simultâneos com `asyncio` e sockets TCP não bloqueantes.
- Lista estática de vizinhos, conexão com todos eles e novas tentativas quando um vizinho está indisponível.
- Inventário local de blocos, reserva de pedidos em andamento e download de vizinhos em paralelo.
- Compartilhamento dos blocos parciais já recebidos.
- Persistência de blocos e retomada após reiniciar o peer, com nova verificação dos blocos existentes.
- Montagem do arquivo em ordem, conferência final de tamanho e SHA-256 e publicação do arquivo completo.
- Logs em JSON, testes automatizados e estudos de caso com processos reais.

O relatório de decisões e avaliação está em [relatorio.pdf](relatorio.pdf). Os dados dos estudos de caso ficam na pasta [evidence](evidence/).

## Como executar

Instale Python **3.10 ou mais recente** e abra o terminal na pasta deste repositório. Não é necessário instalar pacotes com `pip`. No Windows, se o comando `python` não estiver disponível, use `py` nos exemplos.

### Demonstração com dois peers

Prepare um arquivo aleatório de 1 MiB, seu metadado e as configurações:

```console
python scripts/prepare_demo.py --peers 2 --size 1048576 --block-size 1024
```

Execute cada comando abaixo em um terminal separado, sempre a partir da pasta do projeto:

```console
python -m p2p run --config .runtime/demo/A.json
```

```console
python -m p2p run --config .runtime/demo/B.json
```

O peer A começa com o arquivo completo. B baixa o arquivo e o salva em `.runtime/demo/peer-B/exemplo.bin`. A também mantém uma cópia verificada em sua própria pasta de saída. Procure o evento `download_complete` no terminal para confirmar a conclusão. Use **Ctrl+C** para encerrar cada peer: ele continua disponível para servir blocos depois de terminar o download.

Os peers podem ser iniciados em qualquer ordem. Se o vizinho ainda não estiver disponível, o programa tenta novamente até o prazo de download. O comando de preparação imprime os caminhos completos dos comandos e evita substituir uma demonstração já existente. Para criar outra, escolha uma nova pasta com `--output`.

### Demonstração com quatro peers

```console
python scripts/prepare_demo.py --output .runtime/demo4 --peers 4 --size 1048576 --block-size 4096 --base-port 9101
```

Depois execute os quatro comandos impressos, em terminais separados. As conexões configuradas formam esta topologia:

```text
      B
    /   \
   A     D
    \   /
      C
```

A é o seeder inicial. D conhece somente B e C, portanto precisa receber blocos compartilhados pelos peers intermediários. B e C podem enviar para D enquanto continuam baixando de A. A lista é estática, sem tracker.

### Compartilhar um arquivo próprio

Primeiro gere o arquivo de metadados:

```console
python -m p2p metadata caminho/arquivo.bin --block-size 1024 --output compartilhamento/metadata.json
```

Distribua o mesmo metadado aos peers e crie um JSON de configuração para cada um. Por exemplo, salve os arquivos abaixo em `compartilhamento/`, junto do metadado. Ajuste `seed` para apontar para o arquivo original:

`A.json`:

```json
{
  "id": "A",
  "host": "127.0.0.1",
  "port": 9001,
  "neighbors": [{"host": "127.0.0.1", "port": 9002}],
  "metadata": "metadata.json",
  "directory": "peer-A",
  "seed": "../caminho/arquivo.bin",
  "timeout": 120
}
```

`B.json`:

```json
{
  "id": "B",
  "host": "127.0.0.1",
  "port": 9002,
  "neighbors": [{"host": "127.0.0.1", "port": 9001}],
  "metadata": "metadata.json",
  "directory": "peer-B",
  "timeout": 120
}
```

Inicie os peers com `python -m p2p run --config compartilhamento/A.json` e o comando equivalente para B. Todos os caminhos dentro de uma configuração são relativos à pasta desse JSON. O campo `seed` existe apenas nos peers que já têm o arquivo completo.

Para usar computadores diferentes, configure o IP local de cada máquina, os IPs dos vizinhos e portas acessíveis entre elas. O metadado deve continuar idêntico. Os experimentos entregues foram executados em `127.0.0.1`.

### Campos da configuração

| Campo | Função | Padrão |
| --- | --- | --- |
| `id` | Nome do peer usado no handshake e nos logs, de 1 a 100 caracteres | Obrigatório |
| `host`, `port` | Endereço e porta em que o peer recebe conexões | Obrigatórios |
| `neighbors` | Lista de objetos com `host` e `port`, de 1 a 64 vizinhos | Obrigatória |
| `metadata` | Caminho do JSON compartilhado por todos | Obrigatório |
| `directory` | Pasta de blocos e arquivo final desse peer | Obrigatória |
| `seed` | Arquivo completo usado como fonte inicial | Opcional |
| `timeout` | Prazo para receber todos os blocos, antes da montagem final, em segundos | `120` |
| `request_timeout` | Prazo de conexão, leitura ou envio de uma mensagem, em segundos | `5` |
| `poll_interval` | Intervalo entre consultas de inventário e novas tentativas | `0.1` |
| `serve_delay` | Atraso artificial por bloco enviado, usado em demonstrações e testes | `0` |

Os prazos devem ser positivos; `serve_delay` pode ser zero. Portas válidas vão de 1 a 65535. Cada peer deve usar sua própria pasta de saída e uma porta disponível.

## Como funciona por dentro

### Metadados e armazenamento

`metadata.py` calcula o manifesto antes de iniciar os peers. O JSON possui exatamente cinco campos: `name`, `size`, `block_size`, `sha256` e `blocks`. `blocks` contém os hashes dos blocos na ordem original. O identificador `file_id` é o SHA-256 de uma representação canônica desse manifesto, impedindo que peers com metadados diferentes misturem transferências.

`storage.py` mantém os índices disponíveis em conjuntos. Cada bloco recebido passa pela verificação de índice, tamanho e SHA-256 antes de ser gravado em um arquivo temporário e publicado por substituição atômica. Só então ele entra no inventário anunciado. Os blocos ficam em `directory/.blocks/<file_id>/<indice>.part`.

Ao reiniciar, o peer revalida os blocos dessa pasta e aproveita somente os íntegros. O seed é verificado antes de ser anunciado. Leituras para envio também verificam o conteúdo novamente. Quando todos os blocos estão disponíveis, a montagem usa a ordem dos índices e confere o tamanho e o SHA-256 final. Um arquivo final diferente que já exista no destino causa erro, para evitar perda de conteúdo.

### Protocolo TCP

`protocol.py` usa um prefixo de **4 bytes sem sinal, em ordem de rede**, com o tamanho do corpo. O corpo é um objeto JSON em UTF-8. O receptor usa leituras de tamanho exato, porque TCP pode fragmentar uma mensagem ou entregar várias juntas. Quadros vazios ou maiores que **2 MiB** são rejeitados e as operações têm prazo.

| Mensagem | Campos principais | Significado |
| --- | --- | --- |
| `hello` | `version`, `peer`, `file_id` | Inicia a conexão e confirma protocolo e manifesto |
| `status` (pedido) | `type` | Solicita inventário do vizinho |
| `status` (resposta) | `blocks` | Informa os índices disponíveis |
| `get` | `index` | Solicita um bloco pelo índice, começando em zero |
| `block` | `index`, `data` | Retorna o conteúdo em Base64 |
| `missing` | `index` | Informa que o bloco solicitado não está disponível |

Exemplo do corpo de um pedido: `{"type":"get","index":3}`. O cabeçalho binário precede esse JSON. O hash esperado vem do manifesto, portanto não depende da resposta do vizinho.

### Concorrência e recuperação

`peer.py` inicia o listener e uma tarefa de download para cada vizinho. As tarefas mantêm conexões persistentes, consultam os inventários e reservam os índices em andamento para evitar solicitações duplicadas locais. Cada pequeno lote contém até 32 pedidos; depois disso, o inventário é atualizado para descobrir blocos recebidos recentemente por outros leechers. A posição inicial de busca varia por peer e por vizinho.

Uma falha de conexão libera o pedido reservado e permite nova tentativa. Mensagens inválidas encerram somente aquela conexão. O listener continua atendendo outros peers. Leituras, gravações e montagem usam threads de trabalho para que as operações de disco liberem o laço de eventos. Na montagem, a leitura dos blocos usa paralelismo limitado, mas a escrita do arquivo final segue a ordem original.

## Logs e verificação

Cada linha de log é um objeto JSON. Os eventos principais são `listening`, `connected`, `block_received`, `block_sent`, `neighbor_error`, `retry`, `connection_error` e `download_complete`.

Os eventos de blocos incluem o índice, a origem ou o destino e a quantidade disponível no momento. Um `block_sent` com `have` menor que `total` demonstra o envio durante um download incompleto. `download_complete` contém tamanho, SHA-256 e caminho do arquivo concluído.

Para guardar os logs de um peer, redirecione sua saída:

```console
python -m p2p run --config .runtime/demo/B.json > B.jsonl
```

Para conferir o SHA-256 manualmente no PowerShell:

```powershell
Get-FileHash .runtime/demo/exemplo.bin -Algorithm SHA256
Get-FileHash .runtime/demo/peer-B/exemplo.bin -Algorithm SHA256
```

## Testes e estudos de caso

Na validação local em Windows 11 com Python 3.13, **28 testes automatizados e os 24 cenários da matriz passaram**. Foram conferidas 72 cópias, incluindo as fontes, por tamanho e SHA-256. Todos os 12 cenários com quatro peers registraram compartilhamento antes da conclusão. Os resultados dessa execução estão preservados em `evidence/`.

Execute os testes automatizados a partir da raiz do projeto:

```console
python -m unittest discover -s tests -v
```

Os testes cobrem o manifesto, blocos corrompidos, retomada, montagem, arquivo vazio, último bloco parcial, configuração, enquadramento TCP, mensagens inválidas, timeouts e transferências com processos reais. A integração verifica também o compartilhamento antes da conclusão e a chegada tardia de um vizinho. O fluxo de GitHub Actions está configurado para executar a suíte em Windows e Linux.

Uma execução rápida dos estudos de caso usa arquivos de 10 KiB, blocos de 1024 bytes e topologias com 2 e 4 peers:

```console
python scripts/benchmark.py --quick --output evidence-quick
```

A matriz completa combina **2 e 4 peers**, **1024 e 4096 bytes por bloco** e arquivos de **10 KiB, 20 KiB, 1 MiB, 5 MiB, 10 MiB e 20 MiB**, totalizando **24 cenários**:

```console
python scripts/benchmark.py --output evidence --timeout 600
```

O script cria arquivos aleatórios, configurações e processos novos para cada cenário. Verifica o tamanho e o SHA-256 de todos os downloads contra o original. Nos cenários de quatro peers, verifica que D recebeu somente de B/C e que houve envio para D antes de o intermediário completar o próprio arquivo. Os processos são encerrados ao final de cada caso, inclusive em caso de falha.

As evidências geradas são:

- `results.csv`: resumo por cenário, com parâmetros, tempo, integridade e resultado.
- `results.json`: dados completos, hashes, origens dos blocos e contagens de eventos.
- `selected-events.jsonl`: amostra de eventos por cenário, incluindo envios parciais.

As configurações, arquivos e logs completos ficam em `.runtime/benchmark/`, fora do versionamento. Os resultados usam unidades binárias: 1 KiB = 1024 bytes e 1 MiB = 1024² bytes, como aproximação explícita dos KB/MB da tabela do enunciado.

Cada combinação tem uma execução local, sem média estatística. Os tempos incluem a inicialização dos processos e dependem do disco e do escalonador. Nos casos de quatro peers com arquivos de até 20 KiB, é aplicado um atraso de 3 ms por bloco para tornar o compartilhamento parcial observável; o atraso fica registrado nos resultados. A topologia também muda entre dois e quatro peers. Por isso, esses dados demonstram funcionamento e integridade, mas não isolam o efeito da quantidade de peers no desempenho de uma rede física.

## Estrutura do repositório

```text
p2p/
  __main__.py           Interface de terminal
  metadata.py           Manifesto, fragmentação e hashes
  storage.py            Blocos locais, retomada e montagem
  protocol.py           Enquadramento JSON sobre TCP
  peer.py               Configuração, servidor e download concorrente
scripts/
  prepare_demo.py       Prepara demonstrações com vizinhos estáticos
  benchmark.py          Executa a matriz e exporta evidências
  build_report.py       Reconstrói o relatório usando os resultados reais
tests/                  Testes unitários e integração
evidence/               Resultados preservados dos experimentos
relatorio.pdf           Decisões de projeto e avaliação dos casos
docs/relatorio.md       Texto editável do relatório
.github/workflows/      Verificação automática no GitHub
```

## Limites da solução

O objetivo é didático, para uma rede controlada com metadados previamente compartilhados. Não há autenticação, criptografia, tracker, descoberta automática, travessia de NAT ou garantia de defesa contra peers maliciosos. O checksum detecta conteúdo diferente do manifesto recebido; a confiança no próprio manifesto pertence à distribuição inicial.

Para limitar memória e mensagens, são aceitos arquivos de até 128 MiB, blocos de até 1 MiB e até 131072 blocos. O envio usa Base64, que aumenta o volume trafegado. A publicação atômica do arquivo final precisa de um sistema de arquivos que aceite hard links, como NTFS ou ext4. As gravações de blocos não forçam sincronização física individual; uma reinicialização revalida o conteúdo salvo, e a montagem final usa `fsync`. A implementação não foi projetada para uso em produção ou avaliação de alta taxa de transferência.

## Reconstruir o relatório

O PDF pronto já está incluído. Para gerá-lo novamente, instale a dependência opcional e use as evidências da matriz completa:

```console
python -m pip install -r requirements-report.txt
python scripts/build_report.py
```

Essa dependência é usada somente no relatório. O programa P2P e seus testes continuam usando apenas a biblioteca padrão.
