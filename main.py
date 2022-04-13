from sqlite_dissect.entrpoint import cli

if __name__ == "__main__":
    """
    Provide an entrypoint wrapper around the SQLite Dissect module to retain backwards compatibility with documented
    calls to `python main.py ...`
    """
    cli()
