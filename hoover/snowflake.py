
# Twitter Snowflake ID to timestamp (and back)
# https://github.com/client9/snowflake2time/
# Nick Galbreath @ngalbreath nickg@client9.com
# Public Domain -- No Copyright -- Cut-n-Paste!
# but be kind and give credit
#

# Python by default has unlimited integer precision, so this is mostly
# an exercise in date/time/UTC conversion

import time
import datetime
import calendar


def str2utc(s):
    # parse twitter time string into UTC seconds, unix-style
    return calendar.timegm(time.strptime(s, "%a %b %d %H:%M:%S +0000 %Y"))


def utc2snowflake(stamp):
    return (int(round(stamp * 1000)) - 1288834974657) << 22


def snowflake2utc(sf):
    return ((sf >> 22) + 1288834974657) / 1000.0


def str2utcms(s):
    return 1000 * calendar.timegm(
        time.strptime(s, '%a %b %d %H:%M:%S +0000 %Y'))


def snowflake2utcms(sf):
    return ((sf >> 22) + 1288834974657)


# really is the best way to get utc timestamp?
#   (minus changing your box to be UTC)
def utcnow():
    return calendar.timegm(datetime.datetime.utcnow().timetuple())


if __name__ == '__main__':
    print(snowflake2utc(1168286433804771328))
    print(str2utc('Sun Sep 01 22:17:07 +0000 2019'))
    print(utc2snowflake(1567376166))
