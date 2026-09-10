from pathlib import Path
import os


def main() -> None:
    print(Path(os.environ["HTTP_CAPTURE_FILE"]).read_text(), end="")


if __name__ == "__main__":
    main()
