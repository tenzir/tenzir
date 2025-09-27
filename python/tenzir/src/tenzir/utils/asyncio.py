"""This module provides asyncio utilities.

These asyncio utilities are based on Lynn Root's blog post series on asyncio at
https://www.roguelynn.com/words/asyncio-we-did-it-wrong/. In particular, they
help with gracefully shutting down and proper exception handling to terminate an
application, e.g., after catching SIGINT and SIGTERM.

__all__ = getattr(_asyncio, "__all__", [name for name in dir(_asyncio) if not name.startswith("_")])
