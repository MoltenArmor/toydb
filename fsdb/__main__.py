import json
import argparse
import logging
import tempfile
from pathlib import Path
from typing import cast
from fsdb.fsdb.fsdb import FSDB, FSDB_ROOT, FSDB_WORKDIR, JSONValue


def parse_cmdline(
    comp: list[str] | None = None,
) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        exit_on_error=False, description="FSDB: Filesystem database CLI"
    )

    parser.add_argument("--root", type=str, help=f"DB root (default: {FSDB_ROOT})")
    parser.add_argument(
        "--workdir",
        type=str,
        help=f"FSDB temp workdir (default: /tmp)",
    )
    parser.add_argument(
        "-s", "--server", type=str, help="Server type, available value: (http | sql)"
    )

    if comp:
        return parser.parse_known_args(comp)

    return parser.parse_known_args()


def main():
    root = FSDB_ROOT
    workdir = Path(tempfile.mkdtemp(dir=FSDB_WORKDIR, prefix="fsdb"))

    options, args = parse_cmdline()

    if cast(str | None, options.root):
        root = Path(cast(str, options.root))

    if cast(str | None, options.workdir):
        workdir = Path(cast(str, options.workdir))

    if cast(str | None, options.server):
        match cast(str, options.server):
            case "http":
                raise NotImplementedError

            case "sql":
                raise NotImplementedError

            case _:
                raise NotImplementedError

    else:
        db = FSDB(root, workdir)
        try:
            match args:
                case ["list"]:
                    print(db.lsdb())

                case ["show", table]:
                    print(db.lspk(table))

                case ["create", table]:
                    db.create(table)
                    logging.info(f"Successfully create table {table}")

                case ["drop", table]:
                    if db.drop(table):
                        logging.info(f"Successfully dropped table {table}")
                    else:
                        raise Exception(f"Failed to drop table.")

                case ["get", table, key]:
                    if v := db.get(table, key):
                        print(json.dumps(v, indent=2))

                case ["insert" | "update" | "upsert" as cmd, table, key, data]:
                    getattr(db, cmd)(table, key, json.loads(data))

                case ["delete", table, pk]:
                    if db.delete(table, pk):
                        logging.info(f"Successfully deleted {pk} in {table}")
                    else:
                        raise Exception(f"Failed to delete.")

                case ["scan", table, pattern]:
                    for pk, data in db.scan(table, pattern):
                        print(json.dumps({"pk": pk, "data": data}, indent=2))

                case ["index", table, field]:
                    db.create_index(table, field)
                    logging.info("Indexed")

                case ["find", table, field, value]:
                    if data := db.find(table, field, value):
                        print(json.dumps(data, indent=2))
                    else:
                        raise Exception(f"Not found!")

                case ["link", src_table, src_pk, dest_table, dest_pk]:
                    db.link(src_table, src_pk, dest_table, dest_pk)
                    logging.info(
                        f"Linked {src_table}:{src_pk} -> {dest_table}:{dest_pk}"
                    )

                case ["link", src_table, src_pk, dest_table, dest_pk, data]:
                    db.link(
                        src_table,
                        src_pk,
                        dest_table,
                        dest_pk,
                        cast(dict[str, JSONValue], json.loads(data)),
                    )
                    logging.info(
                        f"Linked {src_table}:{src_pk} -> {dest_table}:{dest_pk}"
                    )

                case ["links", table]:
                    for l, r in db.query_links(table):
                        print(f"{l} -> {r}")

                case ["links", table, left, right]:
                    for l, r in db.query_links(table, left, right):
                        print(f"{l} -> {r}")

                case _:
                    raise ValueError(f"UNKNOWN USAGE!")

        except IndexError:
            raise SyntaxError(f"Wrong usage!")


if __name__ == "__main__":
    main()
