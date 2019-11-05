import time


class RateControl():
    def __init__(self, rate_limit=80000):
        self.rate_limit = float(rate_limit)
        self.requests = 0
        self.start_t = None
        self.delta_t = 0.
        self.reqs_per_day = 0.

    def pre_request(self, verbose=False):
        if self.reqs_per_day > self.rate_limit:
            time.sleep(1)

        self.requests += 1

        if verbose:
            print('request #{}'.format(self.requests))

        if self.start_t is None:
            self.start_t = time.time()
        else:
            self.delta_t = (time.time() - self.start_t) / (60. * 60. * 24.)
            self.reqs_per_day = self.requests / self.delta_t
