
def success(result):
    return {
        "code": 200,
        "msg": "success",
        "result": result
    }


def error(msg: str = None):
    return {
        "code": 500,
        "msg": msg,
        "result": None
    }
