"""Gera o relatório de quatro páginas a partir das evidências reais da matriz.

Requer reportlab apenas para reconstruir o PDF; o sistema P2P não depende dele.
"""

import json
import html
from pathlib import Path
import re
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
)

ROOT = Path(__file__).resolve().parents[1]
URL = "https://github.com/LucasAbrandao/sd-tp2-p2p"
BLUE = colors.HexColor("#193E59")
TEAL = colors.HexColor("#167D8D")
LIGHT = colors.HexColor("#EEF4F7")


def main():
    document = json.loads((ROOT / "evidence/results.json").read_text(encoding="utf-8"))
    results = document["results"]
    if len(results) != 24 or any(case["status"] != "passed" for case in results):
        raise SystemExit("O relatório exige os 24 cenários aprovados e registrados.")
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("BodyTP", fontName="Helvetica", fontSize=10.1,
                              leading=14.3, spaceAfter=9, textColor=colors.HexColor("#243746")))
    styles.add(ParagraphStyle("TitleTP", fontName="Helvetica-Bold", fontSize=25,
                              leading=29, spaceAfter=16, textColor=BLUE))
    styles.add(ParagraphStyle("HeadTP", fontName="Helvetica-Bold", fontSize=14,
                              leading=18, spaceBefore=9, spaceAfter=10, textColor=BLUE))
    styles.add(ParagraphStyle("SubTP", fontName="Helvetica-Bold", fontSize=10.5,
                              leading=15, spaceAfter=5, textColor=TEAL))
    styles.add(ParagraphStyle("CellTP", fontName="Helvetica", fontSize=8.6,
                              leading=11, spaceAfter=0))
    styles.add(ParagraphStyle("CodeTP", fontName="Courier", fontSize=8,
                              leading=12, spaceAfter=9, backColor=LIGHT, borderPadding=8))
    styles.add(ParagraphStyle("NoteTP", fontName="Helvetica", fontSize=8.5,
                              leading=12, spaceAfter=9, textColor=colors.HexColor("#536775")))
    story = []
    markdown = []

    def paragraph(text, style="BodyTP"):
        story.append(Paragraph(text, styles[style]))
        editable = re.sub(r'<link href="([^"]+)" color="[^"]+">([^<]+)</link>',
                          r'[\2](\1)', text)
        editable = editable.replace("<b>", "**").replace("</b>", "**").replace("<br/>", "\n")
        editable = html.unescape(editable)
        if style == "CodeTP":
            editable = "```text\n" + editable + "\n```"
        else:
            editable = editable.replace(".blocks/<file_id>/<índice>.part",
                                        "`.blocks/<file_id>/<índice>.part`")
        markdown.append(("# " if style == "TitleTP" else "") + editable)

    def heading(text):
        paragraph(text, "HeadTP")
        markdown[-1] = "## " + markdown[-1]

    def table(rows, widths):
        cells = [[Paragraph(escape(str(cell)), styles["CellTP"]) for cell in row] for row in rows]
        item = Table(cells, colWidths=widths, repeatRows=1, hAlign="LEFT")
        item.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), LIGHT),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("LINEBELOW", (0, 0), (-1, 0), 1, TEAL),
            ("LINEBELOW", (0, 1), (-1, -1), 0.35, colors.HexColor("#D5E0E7")),
        ]))
        story.extend([item, Spacer(1, 10)])
        md_rows = [rows[0], ["---"] * len(rows[0]), *rows[1:]]
        markdown.append("\n".join("| " + " | ".join(str(c) for c in row) + " |" for row in md_rows))

    paragraph("SISTEMAS DISTRIBUÍDOS · CEFET-MG · 2026/2", "SubTP")
    paragraph("Transferência de arquivos<br/>Peer-to-Peer", "TitleTP")
    paragraph("Trabalho Prático 2 · Professora Michelle Hanne · 18/10/2026", "NoteTP")
    paragraph(f'Código-fonte: <link href="{URL}" color="#167D8D">{URL}</link>', "NoteTP")
    paragraph("Repositório privado. O acesso ao código para avaliação precisa ser concedido pelo proprietário.", "NoteTP")
    heading("1. Objetivo e decisões de projeto")
    paragraph("Foi implementado um sistema de transferência por blocos em que cada processo é, simultaneamente, "
              "cliente e servidor. Um seeder inicial possui o arquivo completo; os demais peers solicitam blocos "
              "a vizinhos predefinidos e compartilham os blocos já verificados, mesmo com o download incompleto.")
    paragraph("A linguagem escolhida foi Python 3.10 ou superior. O programa usa somente a biblioteca padrão: "
              "asyncio para concorrência e sockets TCP não bloqueantes, json para serialização, hashlib para "
              "SHA-256 e pathlib para armazenamento. Não há tracker nem serviço central; o manifesto e a lista "
              "estática de vizinhos são fornecidos antes de iniciar os processos.")
    heading("Arquitetura de um peer")
    table([
        ["Componente", "Responsabilidade"],
        ["Servidor TCP", "Aceitar conexões, anunciar o inventário e atender pedidos de blocos disponíveis."],
        ["Clientes concorrentes", "Manter uma conexão por vizinho, consultar seu inventário e solicitar blocos ausentes."],
        ["Armazenamento", "Validar, persistir, recuperar e remontar o conteúdo na ordem dos índices."],
        ["Registro JSONL", "Identificar peer, evento, índice, origem/destino e progresso de cada bloco."],
    ], [115, 390])
    paragraph("O listener começa antes dos clientes. Cada vizinho recebe uma tarefa própria; uma reserva local "
              "evita que dois clientes do mesmo processo solicitem o mesmo bloco em paralelo. A escolha percorre "
              "índices em ordem circular, com ponto inicial derivado do identificador do peer, e atualiza o "
              "inventário remoto a cada lote de até 32 pedidos. Essa regra é simples e não pretende otimizar raridade.")
    paragraph("Leitura, gravação e montagem são enviadas a threads de trabalho para liberar o laço de eventos "
              "durante operações de disco. Após concluir, o peer continua atendendo conexões até ser encerrado.")

    story.append(PageBreak())
    heading("2. Metadados, protocolo e integridade")
    paragraph("O manifesto é um arquivo JSON com exatamente cinco campos. Todos os peers usam a mesma cópia. "
              "O identificador file_id é o SHA-256 do JSON canônico desse manifesto, incluindo nome, tamanho, "
              "fragmentação e hashes. A conexão só prossegue quando versão e file_id coincidem.")
    table([
        ["Campo", "Conteúdo"], ["name", "Nome de arquivo seguro, sem diretórios."],
        ["size / block_size", "Tamanho total e tamanho fixo dos blocos, em bytes."],
        ["sha256", "Hash do arquivo inteiro."],
        ["blocks", "Lista ordenada de hashes SHA-256, um por bloco; o último pode ser menor."],
    ], [115, 390])
    paragraph("Cada mensagem é um quadro: um inteiro sem sinal de 4 bytes em ordem de rede (big-endian) "
              "com o comprimento do corpo, seguido do objeto JSON em UTF-8. readexactly trata cabeçalhos e corpos "
              "fragmentados pelo TCP. Quadros vazios ou acima de 2 MiB são rejeitados; leitura e escrita têm timeout.")
    table([
        ["Mensagem", "Papel"],
        ["hello", "Handshake com versão 1, identificador do peer e file_id."],
        ["status", "Solicitar ou responder a lista de índices atualmente disponíveis."],
        ["get", "Solicitar um bloco pelo seu índice, começando em zero."],
        ["block / missing", "Responder dados em Base64 ou informar que o bloco não está disponível."],
    ], [115, 390])
    heading("Persistência e recuperação")
    paragraph("Um bloco recebido precisa ter índice, tamanho e SHA-256 corretos. Ele é escrito em um arquivo "
              "temporário e renomeado antes de entrar no inventário. Blocos ficam em .blocks/&lt;file_id&gt;/&lt;índice&gt;.part. "
              "As gravações por bloco usam o cache do sistema operacional; não há fsync por KiB. Na reabertura, "
              "cada bloco é revalidado, e conteúdo incompleto ou corrompido não é anunciado.")
    paragraph("A montagem percorre todos os índices, confere novamente cada bloco, tamanho total e SHA-256 final. "
              "As leituras usam até oito threads, em lotes de até 64 blocos e no máximo 4 MiB; a escrita segue "
              "a ordem original. O arquivo pronto é sincronizado e publicado atomicamente com um hard link, sem substituir um destino "
              "de conteúdo diferente. Os testes locais usam NTFS; o destino precisa permitir hard links. Blocos são "
              "mantidos após a montagem para continuar compartilhando e permitir retomada.")
    paragraph("Se um vizinho falha, a reserva é liberada e a tarefa tenta reconectar. Blocos íntegros já salvos "
              "não são baixados de novo. Uma mensagem inválida encerra somente sua conexão. O prazo total do download "
              "evita espera ilimitada quando nenhum vizinho tem os blocos restantes.")

    story.append(PageBreak())
    heading("3. Estudos de caso e resultados medidos")
    paragraph("Foram executadas as 24 combinações de 2/4 peers, blocos de 1024/4096 bytes e arquivos de "
              "10/20 KiB, 1/5 MiB e 10/20 MiB. Adotaram-se unidades binárias: 1 KiB = 1024 bytes e "
              "1 MiB = 1048576 bytes. Cada arquivo foi gerado com os.urandom, e cada peer foi um processo separado, "
              "com comunicação TCP em 127.0.0.1, no Windows, Python 3.13. Uma execução por combinação.")
    paragraph("Com dois peers, A e B são vizinhos e A é a fonte. Com quatro, A conhece B/C; B e C conhecem A/D; "
              "D conhece apenas B/C. Os receptores iniciam antes de A. Essa topologia obriga D a obter conteúdo de "
              "intermediários e permite verificar o compartilhamento antes de eles terminarem de baixar.")
    paragraph("Nos casos de quatro peers e arquivos de até 20 KiB foi aplicado um atraso de 3 ms por bloco, "
              "registrado no resultado, para observar o compartilhamento parcial sem depender somente do escalonador. "
              "Os demais casos não usam atraso artificial. Portanto, as comparações não isolam apenas o número de peers.")
    rows = [["Arquivo", "2 peers / 1 KiB", "2 peers / 4 KiB", "4 peers / 1 KiB", "4 peers / 4 KiB"]]
    sizes = [(10 * 1024, "10 KiB"), (20 * 1024, "20 KiB"), (1024**2, "1 MiB"),
             (5 * 1024**2, "5 MiB"), (10 * 1024**2, "10 MiB"), (20 * 1024**2, "20 MiB")]
    lookup = {(r["peer_count"], r["block_size"], r["size_bytes"]): r for r in results}
    for size, label in sizes:
        rows.append([label] + [f'{lookup[(peers, block, size)]["wall_download_seconds"]:.3f} s'
                              for peers, block in ((2, 1024), (2, 4096), (4, 1024), (4, 4096))])
    table(rows, [65, 110, 110, 110, 110])
    paragraph("Tempo de parede desde o início do primeiro processo até a conclusão de todos os receptores, "
              "incluindo inicialização e montagem; exclui geração do arquivo e do manifesto. "
              "Os valores são observações locais, não médias estatísticas.", "NoteTP")
    verified = sum(len(r["verified_peers"]) for r in results)
    partial = sum(r["partial_sends_to_d"] for r in results)
    paragraph(f"<b>Resultado: 24/24 cenários aprovados.</b> Foram verificadas {verified} cópias (incluindo fontes) "
              "diretamente no sistema de arquivos: o tamanho e o SHA-256 coincidiram com o original. "
              f"Nos 12 cenários com quatro peers, D recebeu somente de B/C; foram registrados {partial} envios "
              "para D enquanto o intermediário ainda tinha apenas parte do arquivo.")
    sample = next(event for case in results for peer in case["peers"] for event in peer["selected_events"]
                  if event.get("event") == "block_sent" and event.get("target") == "D"
                  and 0 < event.get("have", 0) < event.get("total", 0))
    paragraph("Exemplo real do registro de compartilhamento parcial:", "NoteTP")
    compact = {key: sample[key] for key in ("peer", "event", "index", "target", "have", "total")}
    paragraph(escape(json.dumps(compact, separators=(",", ":"))), "CodeTP")
    paragraph("evidence/results.csv contém todos os tempos; results.json inclui hashes, fontes, contagens e "
              "parâmetros; selected-events.jsonl conserva amostras reais dos logs. Os logs completos e os arquivos "
              "de trabalho ficam em .runtime, sem entrar no Git.", "NoteTP")

    story.append(PageBreak())
    heading("4. Avaliação e reprodução")
    large = {key: lookup[(key[0], key[1], 20 * 1024**2)] for key in ((2, 1024), (2, 4096), (4, 1024), (4, 4096))}
    paragraph("A verificação de conteúdo foi bem-sucedida nos tamanhos pequenos, médios e grandes. Para 20 MiB, "
              f'os tempos foram {large[(2, 1024)]["wall_download_seconds"]:.3f} s e '
              f'{large[(2, 4096)]["wall_download_seconds"]:.3f} s com dois peers, e '
              f'{large[(4, 1024)]["wall_download_seconds"]:.3f} s e '
              f'{large[(4, 4096)]["wall_download_seconds"]:.3f} s com quatro peers '
              "(blocos de 1 e 4 KiB, respectivamente). Blocos de 4 KiB reduzem de 20480 para 5120 os blocos "
              "necessários para 20 MiB, diminuindo pedidos, hashes, registros e operações de arquivo.")
    paragraph("O teste com quatro processos também transfere mais cópias e inclui o caminho por intermediários. "
              "Disco, cache, escalonamento, Base64 e inicialização influenciam os tempos. Uma execução em loopback "
              "com duas topologias distintas não sustenta uma conclusão geral sobre escalabilidade. Seriam necessárias "
              "repetições e condições de rede controladas para separar esses efeitos.")
    heading("Testes complementares")
    paragraph("A suíte local de 28 testes passou. Ela cobre serialização e validação do manifesto; seed inválido; "
              "blocos com tamanho ou hash incorretos; duplicatas; retomada com corrupção; destino existente; arquivo vazio "
              "e último bloco parcial. Para a rede, cobre quadros fragmentados, concatenados e truncados, JSON/UTF-8 "
              "inválido, limites, timeout, bloco ausente, índice inválido e listener ativo após uma conexão malformada.")
    paragraph("Quatro testes iniciam processos reais e verificam o resultado: dois peers com último bloco parcial, "
              "arquivo menor que um bloco, quatro peers compartilhando antes da conclusão e chegada tardia do seed. "
              "O último aguarda um erro de conexão observado antes de iniciar a fonte, comprovando a nova tentativa. "
              "O fluxo de CI foi incluído para Python 3.10/3.12 em Windows/Linux; esta avaliação registra a execução local.")
    heading("Como executar")
    for command in ("python scripts/prepare_demo.py --peers 4", "python -m unittest discover -s tests -v",
                    "python scripts/benchmark.py --timeout 600"):
        paragraph(escape(command), "CodeTP")
    paragraph("O primeiro comando gera arquivo, manifesto e configurações e imprime um comando para cada terminal. "
              "O README descreve a execução com dois peers, os caminhos relativos, as portas e o uso em máquinas "
              "distintas. O benchmark encerra seus subprocessos automaticamente e escreve as evidências.")
    heading("Limites assumidos")
    paragraph("O sistema atende a um cenário didático com manifesto confiável: não oferece autenticação, TLS, "
              "descoberta dinâmica ou proteção completa contra participantes maliciosos. Os limites são 128 MiB por "
              "arquivo, 1 MiB por bloco e 131072 blocos. A rede não autentica quem fornece o manifesto. Como evolução, "
              "cabem inventários compactos, seleção por raridade, armazenamento em um único arquivo e experimentos "
              "repetidos em rede física com latência e perdas controladas.")

    def decorate(canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(TEAL)
        canvas.setLineWidth(1)
        canvas.line(45, 807, 550, 807)
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(BLUE)
        canvas.drawString(45, 819, "TP2 · Transferência de arquivos P2P")
        canvas.setFillColor(colors.HexColor("#536775"))
        canvas.drawString(45, 25, "Sistemas Distribuídos · 2026/2")
        canvas.drawRightString(550, 25, f"Página {doc.page}")
        canvas.restoreState()

    pdf = SimpleDocTemplate(str(ROOT / "relatorio.pdf"), pagesize=A4,
                            rightMargin=45, leftMargin=45, topMargin=48, bottomMargin=43,
                            title="TP2 - Transferência de arquivos Peer-to-Peer",
                            author="Projeto sd-tp2-p2p")
    pdf.build(story, onFirstPage=decorate, onLaterPages=decorate)
    docs = ROOT / "docs"
    docs.mkdir(exist_ok=True)
    (docs / "relatorio.md").write_text("\n\n".join(markdown) + "\n", encoding="utf-8")
    print("Relatório gerado: relatorio.pdf e docs/relatorio.md")


if __name__ == "__main__":
    main()
