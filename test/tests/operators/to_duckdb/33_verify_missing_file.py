# runner: python

import os


def main() -> None:
    path = os.path.join(os.environ["DUCKDB_ROOT"], "missing.duckdb")
    print(f"exists: {os.path.exists(path)}")


if __name__ == "__main__":
    main()
