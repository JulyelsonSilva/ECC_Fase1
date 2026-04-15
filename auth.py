from functools import wraps

from flask import request

from config import ADMIN_TOKEN


def _admin_ok():
    token = request.args.get("token") or request.form.get("token")
    return bool(token) and token == ADMIN_TOKEN


def admin_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not _admin_ok():
            return "Unauthorized", 401
        return view_func(*args, **kwargs)
    return wrapper
