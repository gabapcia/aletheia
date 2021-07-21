from collections.abc import Iterator
from pathlib import Path


class Chunkenizer:
    def __init__(self, filepath: Path) -> None:
        self._filepath = filepath

    def chunkenize(self, n_chunks: int = 1000) -> Iterator[tuple[int, int]]:
        with self._filepath.open(mode='r', encoding='iso-8859-1') as f:
            lines = sum(1 for _ in f)

        if lines > n_chunks:
            chunk_size = lines // n_chunks
            for i in range(n_chunks):
                start = i * chunk_size
                end = (i + 1) * chunk_size - 1

                if i == (n_chunks - 1):
                    end = lines

                yield start, end
        else:
            yield 0, lines
