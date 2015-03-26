import contextlib


@contextlib.contextmanager
def assertraises(error_class, message):
    try:
        yield
    except Exception as e:
        if not isinstance(e, error_class):
            raise Exception("Failed: wrong error thrown: expected %s but got "
                            "%s" % (error_class, e))
        elif message not in str(e):
            raise Exception("Failed: wrong error string: expected %s but got "
                            "%s" % (message, e.message))
    else:
        raise Exception("Failed: no error thrown")
