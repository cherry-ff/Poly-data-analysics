from __future__ import annotations

from pathlib import Path

from dotenv import find_dotenv, load_dotenv


def load_project_env(env_path: str | None = None) -> str | None:
    """Load a project `.env` file once per process.

    Resolution order:
    1. explicit ``env_path``
    2. nearest `.env` from current working directory
    3. `<cwd>/.env`
    """
    if env_path:
        path = Path(env_path).expanduser()
        if path.exists():
            load_dotenv(path, override=True)
            return str(path)
        return None

    found = find_dotenv(filename=".env", usecwd=True)
    if found:
        load_dotenv(found, override=False)
        return found

    fallback = Path.cwd() / ".env"
    if fallback.exists():
        load_dotenv(fallback, override=False)
        return str(fallback)
    return None
