"""Middleware для бота"""

from .auth import AuthMiddleware

__all__ = ["AuthMiddleware"]