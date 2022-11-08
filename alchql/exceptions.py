class HTTPException(Exception):
    def __init__(self, status_code=400, msg="Unknown exception occurred"):
        self.status_code = status_code
        super().__init__(msg)
