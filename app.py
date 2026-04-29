import argparse

from etl import staging, dwh, db


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--etl", action="store_true", help="Run staging and DWH pipelines before loading to DB")
    args = parser.parse_args()

    if args.etl:
        print("Running staging pipeline...")
        staging.main()
        print("Running DWH pipeline...")
        dwh.main()

    print("Loading data into PostgreSQL...")
    db.load_all()


if __name__ == "__main__":
    main()
