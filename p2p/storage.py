"""Armazenamento local de blocos verificados e montagem segura do arquivo."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import tempfile

from .metadata import Metadata


class BlockStore:
    """Mantém apenas blocos íntegros no conjunto de disponibilidade anunciado."""

    def __init__(
        self, metadata: Metadata, directory: str | Path, seed: str | Path | None = None
    ) -> None:
        self.metadata = metadata
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.block_directory = self.directory / ".blocks" / metadata.file_id
        self.block_directory.mkdir(parents=True, exist_ok=True)
        self._cached: set[int] = set()
        self._seed_available: set[int] = set()
        self._seed: Path | None = None
        self._load_cached()
        if seed is not None:
            self._validate_seed(Path(seed))

    @property
    def available(self) -> set[int]:
        # A cópia impede que outro componente anuncie um bloco sem validá-lo.
        return self._cached | self._seed_available

    @property
    def have_count(self) -> int:
        # Os dois conjuntos são disjuntos; progresso e complete custam O(1).
        return len(self._cached) + len(self._seed_available)

    def has_block(self, index: int) -> bool:
        """Consulta pontual sem copiar todo o inventário a cada solicitação."""
        return index in self._cached or index in self._seed_available

    @property
    def complete(self) -> bool:
        return self.have_count == self.metadata.block_count

    def _validate_data(self, index: int, data: bytes) -> None:
        try:
            expected = self.metadata.expected_size(index)
        except IndexError as exc:
            raise ValueError("Índice de bloco inválido.") from exc
        if not isinstance(data, bytes) or len(data) != expected:
            raise ValueError("Tamanho de bloco diferente do esperado.")
        if hashlib.sha256(data).hexdigest() != self.metadata.blocks[index]:
            raise ValueError("Checksum SHA-256 do bloco não confere.")

    def _read_part(self, index: int) -> bytes:
        # Um arquivo adulterado não pode provocar uma leitura sem limite.
        with (self.block_directory / f"{index}.part").open("rb") as source:
            data = source.read(self.metadata.expected_size(index) + 1)
        self._validate_data(index, data)
        return data

    def _load_cached(self) -> None:
        """Na retomada, revalida os blocos deixados pela execução anterior."""
        for path in self.block_directory.iterdir():
            stem = path.name.removesuffix(".part")
            if not path.name.endswith(".part") or not stem.isascii() or not stem.isdigit() or len(stem) > 6:
                continue
            index = int(stem)
            if path.name != f"{index}.part" or not 0 <= index < self.metadata.block_count:
                continue
            try:
                self._read_part(index)
            except (OSError, ValueError):
                continue
            self._cached.add(index)

    def _validate_seed(self, path: Path) -> None:
        """Verifica o seed inteiro antes de disponibilizar qualquer bloco dele."""
        if path.stat().st_size != self.metadata.size:
            raise ValueError("O arquivo seed tem tamanho diferente dos metadados.")
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for index in range(self.metadata.block_count):
                data = source.read(self.metadata.expected_size(index))
                self._validate_data(index, data)
                digest.update(data)
            if source.read(1):
                raise ValueError("O arquivo seed cresceu durante a validação.")
        if digest.hexdigest() != self.metadata.sha256:
            raise ValueError("Checksum SHA-256 do arquivo seed não confere.")
        self._seed = path
        self._seed_available = set(range(self.metadata.block_count)) - self._cached

    def get_block(self, index: int) -> bytes:
        """Lê e revalida o bloco, inclusive se o disco mudou depois do início."""
        if type(index) is not int or not 0 <= index < self.metadata.block_count:
            raise KeyError(index)
        was_cached = index in self._cached
        if was_cached:
            try:
                return self._read_part(index)
            except (OSError, ValueError):
                self._cached.discard(index)
        if self._seed is not None and (index in self._seed_available or was_cached):
            try:
                with self._seed.open("rb") as source:
                    source.seek(index * self.metadata.block_size)
                    data = source.read(self.metadata.expected_size(index))
                self._validate_data(index, data)
                self._seed_available.add(index)
                return data
            except (OSError, ValueError):
                self._seed_available.discard(index)
        raise KeyError(index)

    def put_block(self, index: int, data: bytes) -> bool:
        """Persiste atomicamente um bloco íntegro; retorna False para duplicatas."""
        self._validate_data(index, data)
        if index in self._cached or index in self._seed_available:
            try:
                self.get_block(index)
                return False
            except KeyError:
                pass
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=self.block_directory, prefix=".incoming-", delete=False) as output:
                temporary = Path(output.name)
                output.write(data)
                # Fechar antes de renomear garante que outro pedido leia o bloco
                # inteiro. Não forçamos o disco para cada KiB; na retomada todos
                # os blocos são revalidados, e a montagem final usa fsync.
            os.replace(temporary, self.block_directory / f"{index}.part")
            self._cached.add(index)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return True

    def _matches_file(self, path: Path) -> bool:
        if not path.is_file() or path.stat().st_size != self.metadata.size:
            return False
        digest = hashlib.sha256()
        with path.open("rb") as source:
            while data := source.read(1024 * 1024):
                digest.update(data)
        return digest.hexdigest() == self.metadata.sha256

    def _assembly_blocks(self):
        """Lê em lotes limitados e entrega os blocos na ordem do arquivo."""
        def read(index):
            try:
                return self.get_block(index)
            except KeyError as exc:
                raise ValueError(f"O bloco {index} ficou indisponível durante a montagem.") from exc

        if self._seed is not None and not self._cached:
            # O seed já é um arquivo contínuo: não há milhares de arquivos novos
            # para abrir, e criar tarefas para cada bloco custaria mais que ler.
            for index in range(self.metadata.block_count):
                yield read(index)
            return
        if not self.metadata.block_count:
            return
        # No Windows, a primeira abertura de muitos arquivos recém-criados pode
        # aguardar verificações do sistema. O pool sobrepõe essas esperas sem
        # alterar a ordem de escrita. O lote limita dados pendentes a 4 MiB.
        batch_size = min(64, max(1, 4 * 1024 * 1024 // self.metadata.block_size))
        with ThreadPoolExecutor(max_workers=8) as executor:
            for first in range(0, self.metadata.block_count, batch_size):
                indices = range(first, min(first + batch_size, self.metadata.block_count))
                yield from executor.map(read, indices)

    def assemble(self) -> Path:
        """Monta e verifica o arquivo; a publicação final não substitui outro conteúdo."""
        if not self.complete:
            raise ValueError("Ainda faltam blocos para montar o arquivo.")
        target = self.directory / self.metadata.name
        if target.exists():
            if self._matches_file(target):
                return target
            raise FileExistsError(f"Já existe um arquivo diferente em {target}.")
        temporary: Path | None = None
        try:
            digest = hashlib.sha256()
            total = 0
            with tempfile.NamedTemporaryFile(dir=self.directory, prefix=".assembling-", delete=False) as output:
                temporary = Path(output.name)
                for data in self._assembly_blocks():
                    output.write(data)
                    total += len(data)
                    digest.update(data)
                output.flush()
                os.fsync(output.fileno())
            if total != self.metadata.size or digest.hexdigest() != self.metadata.sha256:
                raise ValueError("O tamanho ou SHA-256 do arquivo montado não confere.")
            try:
                # Criar um hard link publica o arquivo completo atomicamente e
                # falha se outro processo criou o destino durante a montagem.
                os.link(temporary, target)
            except FileExistsError:
                if not self._matches_file(target):
                    raise FileExistsError(f"Já existe um arquivo diferente em {target}.") from None
            return target
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
