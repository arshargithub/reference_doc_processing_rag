"""Index sample documents into Elasticsearch.

Usage:
    PYTHONPATH=. python scripts/index_sample.py <directory> <request_id> [--recreate]
    PYTHONPATH=. python scripts/index_sample.py sample_files/sample_1 sample_1 --recreate
    PYTHONPATH=. python scripts/index_sample.py sample_files/sample_2 sample_2
"""

import logging
import sys

from src.index.pipeline import index_directory

logging.basicConfig(level=logging.INFO, format="%(name)s  %(message)s")


def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/index_sample.py <directory> <request_id> [--recreate]")
        sys.exit(1)

    directory = sys.argv[1]
    request_id = sys.argv[2]
    recreate = "--recreate" in sys.argv

    stats = index_directory(
        directory=directory,
        request_id=request_id,
        recreate_index=recreate,
    )
    print(f"\nDone. {stats}")


if __name__ == "__main__":
    main()
