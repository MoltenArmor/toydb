import json
import sys
import argparse
import tempfile
import logging
from pathlib import Path
from collections.abc import Callable, Mapping, Sequence
from fsdb.fsdb.fsdb import FSDB, FSDB_ROOT, FSDB_WORKDIR, JSONValue

type Response = Mapping[str, JSONValue | Sequence[str | JSONValue] | bool | None]


def _list(db: FSDB) -> Response:
    return {"data": db.lsdb()}


def _show(db: FSDB, table: str) -> Response:
    return {"data": db.lspk(table)}


def _create(db: FSDB, table: str) -> Response:
    db.create(table)
    return {}


def _drop(db: FSDB, table: str) -> Response:
    return {"status": db.drop(table)}


def _get(db: FSDB, table: str, pk: str) -> Response:
    return {"data": db.get(table, pk)}


def _insert(db: FSDB, table: str, pk: str, data: JSONValue) -> Response:
    db.insert(table, pk, data)
    return {}


def _update(db: FSDB, table: str, pk: str, data: JSONValue) -> Response:
    db.update(table, pk, data)
    return {}


def _upsert(db: FSDB, table: str, pk: str, data: JSONValue) -> Response:
    db.upsert(table, pk, data)
    return {}


def _delete(db: FSDB, table: str, pk: str) -> Response:
    return {"status": db.delete(table, pk)}


def _scan(db: FSDB, table: str, pattern: str = "*") -> Response:
    return {"data": [{"pk": pk, "data": d} for pk, d in db.scan(table, pattern)]}


def _index(db: FSDB, table: str, field: str) -> Response:
    db.create_index(table, field)
    return {}


def _find(db: FSDB, table: str, field: str, value: str) -> Response:
    return {"data": db.find(table, field, value)}


def _link(
    db: FSDB,
    src_table: str,
    src_pk: str,
    dest_table: str,
    dest_pk: str,
    data: JSONValue | None = None,
) -> Response:
    db.link(src_table, src_pk, dest_table, dest_pk, data)
    return {}


def _links(
    db: FSDB,
    table: str,
    src_pk: str = "*",
    dest_table: str = "*",
    dest_pk: str = "*",
) -> Response:
    return {
        "data": [
            {"src_pk": s, "dest_table": dt, "dest_pk": dp}
            for s, dt, dp in db.query_links(table, src_pk, dest_table, dest_pk)
        ]
    }


DISPATCH: dict[str, Callable[..., Response]] = {
    "list": _list,
    "show": _show,
    "create": _create,
    "drop": _drop,
    "get": _get,
    "insert": _insert,
    "update": _update,
    "upsert": _upsert,
    "delete": _delete,
    "scan": _scan,
    "index": _index,
    "find": _find,
    "link": _link,
    "links": _links,
}


def sidecar(db: FSDB) -> None:
    for l in sys.stdin:
        r = json.loads(l)
        try:
            resp = {"parameters": DISPATCH[r["method"]](db, **r["parameters"])}
        except KeyError:
            resp = {"error": "UnknownMethod", "parameters": {"method": r["method"]}}
        except Exception as e:
            resp = {"error": "InternalError", "parameters": {"field": repr(e)}}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="FSDB: Filesystem database CLI")
    p.add_argument("--root", type=str, help=f"DB root (default: {FSDB_ROOT})")
    p.add_argument("--workdir", type=str, help="FSDB temp workdir (default: /tmp)")
    p.add_argument(
        "-s", "--server", type=str, help="Server type, available: (stdin | http | sql)"
    )

    sub = p.add_subparsers(dest="method")

    sub.add_parser("list", help="List all tables")

    s = sub.add_parser("show", help="Show primary keys in a table")
    s.add_argument("--table", required=True)

    s = sub.add_parser("create", help="Create a table")
    s.add_argument("--table", required=True)

    s = sub.add_parser("drop", help="Drop a table")
    s.add_argument("--table", required=True)

    s = sub.add_parser("get", help="Get a record by primary key")
    s.add_argument("--table", required=True)
    s.add_argument("--pk", required=True)

    for cmd in ("insert", "update", "upsert"):
        s = sub.add_parser(cmd, help=f"{cmd.capitalize()} a record")
        s.add_argument("--table", required=True)
        s.add_argument("--pk", required=True)
        s.add_argument("--data", required=True, type=json.loads, help="JSON object")

    s = sub.add_parser("delete", help="Delete a record")
    s.add_argument("--table", required=True)
    s.add_argument("--pk", required=True)

    s = sub.add_parser("scan", help="Scan records by glob pattern")
    s.add_argument("--table", required=True)
    s.add_argument("--pattern", default="*")

    s = sub.add_parser("index", help="Create an index on a field")
    s.add_argument("--table", required=True)
    s.add_argument("--field", required=True)

    s = sub.add_parser("find", help="Find records by indexed field")
    s.add_argument("--table", required=True)
    s.add_argument("--field", required=True)
    s.add_argument("--value", required=True)

    s = sub.add_parser("link", help="Create a link between records")
    s.add_argument("--src-table", required=True)
    s.add_argument("--src-pk", required=True)
    s.add_argument("--dest-table", required=True)
    s.add_argument("--dest-pk", required=True)
    s.add_argument("--data", type=json.loads, default=None, help="JSON object")

    s = sub.add_parser("links", help="Query links")
    s.add_argument("--table", required=True)
    s.add_argument("--src-pk", default="*")
    s.add_argument("--dest-table", default="*")
    s.add_argument("--dest-pk", default="*")

    return p


def main():
    p = build_parser()
    args = p.parse_args()

    root = Path(args.root) if args.root else FSDB_ROOT
    workdir = (
        Path(args.workdir)
        if args.workdir
        else Path(tempfile.mkdtemp(dir=FSDB_WORKDIR, prefix="fsdb"))
    )

    db = FSDB(root, workdir)

    if args.server:
        match args.server:
            case "stdin":
                sidecar(db)
                return
            case "http" | "sql":
                raise NotImplementedError
            case _:
                raise NotImplementedError

    if args.method:
        try:
            print(
                json.dumps(
                    DISPATCH[args.method](
                        db,
                        **{
                            k: v
                            for k, v in vars(args).items()
                            if k not in ("root", "workdir", "server", "method")
                            and v is not None
                        },
                    )
                )
            )
        except Exception as e:
            logging.error(repr(e))
            sys.exit(1)
    else:
        p.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
