try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from rro_bot.service import main

if __name__ == "__main__":
    if load_dotenv:
        load_dotenv(override=True)
    try:
        main()
    except KeyboardInterrupt:
        pass
