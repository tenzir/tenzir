from pathlib import Path
import os


def main() -> None:
    print(Path(os.environ["PROMETHEUS_REMOTE_WRITE_CAPTURE"]).read_text(), end="")


if __name__ == "__main__":
    main()
