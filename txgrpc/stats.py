from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class NullStats(object):
    def log_success(self, ms):
        pass

    def log_error(self, ms):
        pass


class LocalStats(object):
    def __init__(self):
        self._stats = []

    def log_success(self, t):
        self._stats.append(t)

    def log_error(self, t):
        pass

    def reset(self):
        del self._stats[:]

    def get_stats(self):
        if not self._stats:
            return {
                'calls': 0,
                'min': 0,
                'max': 0,
                'avg': 0,
                'p75': 0,
                'p90': 0,
                'p95': 0,
            }
        stats = self._stats
        stats.sort()
        print(stats)
        return {
            'calls': len(stats),
            'min': stats[0],
            'max': stats[-1],
            'avg': sum(stats) / len(stats),
            'p75': stats[int(len(stats) * .75)],
            'p90': stats[int(len(stats) * .90)],
            'p95': stats[int(len(stats) * .95)],
        }

    def __repr__(self):
        stats = self.get_stats()
        result = []
        result.append('<LocalStats')
        for k, v in self.get_stats().items():
            result.aappend('    {}: {}'.format(k, v))
        result.append('>')
        return '\n'.join(result)
