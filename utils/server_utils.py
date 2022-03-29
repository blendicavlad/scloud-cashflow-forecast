from flask import request
import os


def shutdown_server():
    """
    We need this function to shutdown the service after it is finished executing
    !!! werkzeug.server.shutdown is depreciated and will be remove in Flask 2.0 !!!
    """
    fn = request.environ.get('werkzeug.server.shutdown')
    if fn is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    fn()


def is_dev_mode():
    if os.environ.get('FLASK_ENV') == 'development':
        return True
    else:
        return False
