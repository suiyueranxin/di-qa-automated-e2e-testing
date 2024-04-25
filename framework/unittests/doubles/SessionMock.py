import queue


class SessionMock:

    def __init__(self) -> None:
        self.lastcalledurl: str = None
        self.posteddata = None
        self.postedheaders = None
        self.responses = queue.SimpleQueue()

    def get(self, url):
        self.lastcalledurl = url
        return self.responses.get(block=False)

    def post(self, url, data, headers):
        self.lastcalledurl = url
        self.posteddata = data
        self.postedheaders = headers
        return self.responses.get(block=False)

    def delete(self, url, headers):
        self.lastcalledurl = url
        self.postedheaders = headers
        return self.responses.get(block=False)

    def put(self, url, data, headers):
        self.lastcalledurl = url
        self.putdata = data
        self.putheaders = headers
        return self.responses.get(block=False)

    # configuration of the mock
    def setresponse(self, statuscode: int):
        response = ResponseMock(statuscode, None)
        self.responses.put(response)

    def setresponsecontent(self, statuscode: int, content: str):
        response = ResponseMock(statuscode, content)
        self.responses.put(response)


class ResponseMock:

    def __init__(self, statuscode, content) -> None:
        self.status_code = statuscode
        self.text = content
