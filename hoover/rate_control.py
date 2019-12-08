import time


class RateControl():
    # rate limit in requests per 15 min.
    def __init__(self, rate_limit):
        self.rate_limit = float(rate_limit) * 4. * 24.
        self.requests = 0
        self.start_t = None
        self.delta_t = None
        self.reqs_per_day = 0.

    def pre_request(self, verbose=False):
        if self.reqs_per_day > self.rate_limit:
            if self.delta_t:
                delta_t = self.requests / self.rate_limit
                t = (delta_t - self.delta_t) * (60. * 60. * 24.)
                time.sleep(t)
            else:
                time.sleep(1)

        self.requests += 1

        if verbose:
            print('request #{}'.format(self.requests))

        if self.start_t is None:
            self.start_t = time.time()
        else:
            self.delta_t = (time.time() - self.start_t) / (60. * 60. * 24.)
            self.reqs_per_day = self.requests / self.delta_t
