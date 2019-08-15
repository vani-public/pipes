from datetime import datetime, timedelta

from pypipes.config import ClientConfig
from pypipes.exceptions import QuotaExceededException

from pypipes.context.factory import ContextPoolFactory, LazyContext


class IQuota(object):
    name = None

    def consume(self, key):
        """
        Consume one from quota. Raises QuotaExceededException when quota is exceeded
        :param key: quota sub-key
        :raise: QuotaExceededException
        """
        pass


class UnlimitedQuota(IQuota):
    name = 'Unlimited'

    def __repr__(self):
        return self.__class__.__name__

    def consume(self, key):
        # no limitation on consume
        pass


class Quota(IQuota):
    _repr_period = None

    def __init__(self, quota_name, rate_counter, limit, threshold):
        """
        :param quota_name: quota name
        :type rate_counter: pypipes.service.rate_counter.IRateCounter
        :param limit: quota limit
        :param threshold: quota threshold in seconds or callable that returns a threshold value
        """
        self.name = quota_name
        self._rate_counter = rate_counter
        self._limit = limit
        self._threshold = threshold
        self._repr_period = '{} sec'.format(threshold)

    def __repr__(self):
        return '{}({}, {}/{})'.format(self.__class__.__name__, self.name,
                                      self._limit, self._repr_period)

    def consume(self, key=None):
        value, expires_in = self._rate_counter.increment(key or 'default',
                                                         threshold=self._threshold)
        if value > self._limit:
            raise QuotaExceededException(self.name,
                                         quota_name=self.name,
                                         quota_key=key,
                                         retry_in=expires_in)


class HourQuota(Quota):
    def __init__(self, quota_name, rate_counter, limit, hours=1):
        super(HourQuota, self).__init__(quota_name, rate_counter, limit, self.calc_threshold)
        self._delta = timedelta(hours=hours)
        self._repr_period = '{} h'.format(hours)

    def calc_threshold(self):
        # calc a threshold - time in seconds till end of a hour
        now = datetime.utcnow()
        return (now.replace(microsecond=0, second=0, minute=0) + self._delta - now).total_seconds()


class DayQuota(Quota):
    def __init__(self, quota_name, rate_counter, limit, days=1):
        super(DayQuota, self).__init__(quota_name, rate_counter, limit, self.calc_threshold)
        self._delta = timedelta(days=days)
        self._repr_period = '{} d'.format(days)

    def calc_threshold(self):
        # calc a threshold - time in seconds till end of a hour
        now = datetime.utcnow()
        return (now.replace(microsecond=0, second=0, minute=0, hour=0) + self._delta - now
                ).total_seconds()


class MonthQuota(Quota):
    def __init__(self, quota_name, rate_counter, limit, months=1):
        super(MonthQuota, self).__init__(quota_name, rate_counter, limit, self.calc_threshold)
        self._months = months
        self._repr_period = '{} mon'.format(months)

    def calc_threshold(self):
        # calc a threshold - time in seconds till first day of next month
        now = datetime.utcnow()
        month = int(now.month + self._months)
        year = int(now.year + month / 12)
        month = int(month % 12 + 1)

        return (now.replace(microsecond=0, second=0, minute=0, hour=0,
                            day=1, month=month, year=year) - now).total_seconds()


class QuotaPool(ContextPoolFactory):
    _rate_counter_pool = None
    _config = None
    _default_quota = None

    def __init__(self, config, rate_counter_pool):
        super(QuotaPool, self).__init__(self._get_quota)
        self._config = config
        self._rate_counter_pool = rate_counter_pool
        default_quota_config = ClientConfig(self._config.quota).default.get_level()
        self._default_quota = (self._create_quota_counter('default', **default_quota_config)
                               if default_quota_config else UnlimitedQuota())

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._config.quota)

    def _get_quota(self, quota_name):
        quota_config = self._config.quota.get_section(quota_name)
        if not quota_config:
            return self._default_quota
        return self._create_quota_counter(quota_name, **quota_config)

    def _create_quota_counter(self, quota_name, limit=0, months=0, days=0, hours=0, threshold=0):
        if limit <= 0:
            return UnlimitedQuota()
        if months > 0:
            quota_class, counter_name, value = MonthQuota, 'month_quota', months
        elif days > 0:
            quota_class, counter_name, value = DayQuota, 'day_quota', days
        elif hours > 0:
            quota_class, counter_name, value = HourQuota, 'hour_quota', hours
        else:
            quota_class, counter_name, value = Quota, 'quota', threshold or 60
        return quota_class(quota_name, self._rate_counter_pool[counter_name],
                           limit, value)


def quota_pool_factory(config, rate_counter):
    """
    Initialize a pool of IQuota contexts
    :type config: pypipes.config.Config
    :param rate_counter: IContextPool of IRateCounter services
        QuotaPool may use 'month_quota', 'day_quota', 'hour_quota'
        or 'quota' named service from rate_counter pool depending on quota configuration
    :type rate_counter: IContextPool[pypipes.service.rate_counter.IRateCounter]
    :return: quota context pool
    :rtype: QuotaPool
    """
    return QuotaPool(config=config,
                     rate_counter_pool=rate_counter)


quota_pool = LazyContext(quota_pool_factory)
