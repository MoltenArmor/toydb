from collections.abc import Iterator
import os
import json
import uuid
import shutil
from typing import Final, cast
from pathlib import Path

FSDB_ROOT: Final[Path] = Path(os.getenv("FSDB_ROOT", "/var/lib/fsdb"))
FSDB_WORKDIR: Final[Path | None] = None
type JSONValue = dict[
    str, dict[str, JSONValue] | list[JSONValue] | str | int | float | bool | None
]


class FSDB:
    def __init__(self, root: Path, workdir: Path):
        self.root: Final = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.workdir: Final = workdir
        self.workdir.mkdir(parents=True, exist_ok=True)

    def _safe_name(self, name: str) -> None:
        if any(c in name for c in ("..", "/", "\\")):
            raise ValueError(f"Unsafe key: {name}")

    def _target_f(self, table: str, name: str | None = None) -> Path:
        self._safe_name(table)
        if name:
            self._safe_name(name)
            return self.root / table / name
        else:
            return self.root / table

    def _write(self, table: str, pk: str, data: dict[str, JSONValue]) -> None:
        # We do not create tables automatically
        if not self._target_f(table).exists():
            raise FileNotFoundError(f"Table {table} does not exist")

        target_f = self._target_f(table, pk)

        tmp = self.workdir / f"{table}-{pk}-{uuid.uuid1()}"
        try:
            with open(tmp, "w") as f:
                json.dump(data, f)
            shutil.move(tmp, target_f)  # pyright: ignore[reportUnusedCallResult]
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    def create(self, table: str) -> None:
        self._target_f(table).mkdir(exist_ok=True)

    def lsdb(self) -> list[str]:
        return [p.name for p in self.root.iterdir() if p.is_dir()]

    def lspk(self, table: str) -> list[str]:
        if not (d := (self.root / table)).exists():
            raise FileNotFoundError(f"Table {table} not found")

        return [p.name for p in d.iterdir() if p.is_file() and ":" not in p.name]

    def insert(
        self,
        table: str,
        pk: str,
        data: dict[str, JSONValue],
    ) -> None:
        if self._target_f(table, pk).exists():
            raise FileExistsError(f"Record {pk} already exists in {table}")

        self._write(table, pk, data)
        self._update_index(table, pk, None, data)

    def update(
        self,
        table: str,
        pk: str,
        data: dict[str, JSONValue],
    ) -> None:
        current = self.get(table, pk)

        if not current:
            raise FileNotFoundError(f"Record {pk} in {table} is missing")

        self._write(table, pk, data)
        self._update_index(table, pk, current, data)

    def upsert(
        self,
        table: str,
        pk: str,
        data: dict[str, JSONValue],
    ) -> None:
        current = self.get(table, pk)

        self._write(table, pk, data)
        self._update_index(table, pk, current, data)

    def get(self, table: str, pk: str) -> dict[str, JSONValue] | None:
        if (p := self._target_f(table, pk)).exists() and p.is_file():
            try:
                with open(p, "r") as f:
                    return cast(dict[str, JSONValue], json.load(f))
            # Not JSON???
            except:
                return None
        return None

    def delete(self, table: str, pk: str) -> bool:
        current = self.get(table, pk)
        if not current:
            return False

        if (p := self._target_f(table, pk)).exists():
            p.unlink()
            self._update_index(table, pk, current, None)
            return True

        return False

    def drop(self, table: str) -> bool:
        if (p := self.root / table).exists():
            shutil.rmtree(p)
            return True
        return False

    def _index(self, table: str, pk: str, field: str, value: str) -> None:
        index = self._target_f(table, f"@{field}") / value
        target = Path("..") / pk

        index.unlink(missing_ok=True)
        index.symlink_to(target)

    def has_index(self, table: str, field: str) -> bool:
        return (self.root / table / f"@{field}").is_dir()

    def create_index(self, table: str, field: str) -> None:
        d = list(self.scan(table))
        idxdir = self._target_f(table, f"@{field}")
        idxdir.mkdir(exist_ok=True)

        for pk, data in d:
            if field in data:
                self._index(table, pk, field, str(data[field]))

    def _update_index(
        self,
        table: str,
        pk: str,
        old: dict[str, JSONValue] | None,
        new: dict[str, JSONValue] | None,
    ) -> None:
        for idxdir in (self.root / table).glob("@*"):
            if not idxdir.is_dir():
                continue

            field = idxdir.name.lstrip("@")
            if (
                old
                and field in old
                and (link := (idxdir / str(old[field]))).is_symlink()
                and link.readlink() == Path(f"../{pk}")
            ):
                link.unlink()

            if new and field in new:
                self._index(table, pk, field, str(new[field]))

    def find(self, table: str, field: str, value: str) -> dict[str, JSONValue] | None:
        if (p := self._target_f(table, f"@{field}") / value).exists():
            try:
                with open(p, "r") as f:
                    return cast(dict[str, JSONValue], json.load(f))
            except:
                pass

    def link(
        self,
        src_table: str,
        src_pk: str,
        dest_table: str,
        dest_pk: str,
        data: dict[str, JSONValue] | None = None,
    ) -> None:
        if not self._target_f(src_table, src_pk).exists():
            raise FileNotFoundError(f"Primary key {src_pk} not found in {src_table}.")

        if not self._target_f(dest_table, dest_pk).exists():
            raise FileNotFoundError(f"Primary key {dest_pk} not found in {dest_table}.")

        self.upsert(src_table, f"{src_pk}:{dest_pk}", data or {})

    def query_links(
        self, table: str, left: str = "*", right: str = "*"
    ) -> list[tuple[str, str]]:
        return [
            (
                parts[0],
                parts[2],
            )
            for p in (self.root / table).glob(f"{left}:{right}")
            if ":" in (name := Path(p).name) and (parts := name.partition(":"))
        ]

    def scan(
        self, table: str, pattern: str = "*"
    ) -> Iterator[tuple[str, dict[str, JSONValue]]]:
        dir = self._target_f(table)

        for p in dir.glob(pattern):
            if not p.is_file():
                continue

            if ":" in p.name:
                continue

            try:
                with open(p, "r") as f:
                    yield p.name, cast(dict[str, JSONValue], json.load(f))

            except:
                continue
