class RetryException(Exception):
    retry_in = None

    def __init__(self, *args, **kwargs):
        self.retry_in = kwargs.pop('retry_in', None)
        super(RetryException, self).__init__(*args, **kwargs)


class RetryMessageException(RetryException):
    pass


class DropMessageException(Exception):
    pass


class InvalidConfigException(Exception):
    pass


class QuotaExceededException(RetryException):
    def __init__(self, *args, **kwargs):
        self.quota_name = kwargs.pop('quota_name', None)
        self.quota_key = kwargs.pop('quota_key', None)
        super(QuotaExceededException, self).__init__(*args, **kwargs)


class RateLimitExceededException(RetryMessageException):
    pass


class ExtendedException(Exception):
    def __init__(self, *args, **kwargs):
        self.extra = kwargs.pop('extra', {})
        super(ExtendedException, self).__init__(*args, **kwargs)
